from django.core.management.base import BaseCommand
from django.core.cache import cache
import time
import logging
from checker.services.postcode_helpers import get_location_to_postcodes_mapping

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Builds a complete location-to-postcode mapping and stores it in Redis with no expiration'

    def add_arguments(self, parser):
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Increase output verbosity',
        )
        parser.add_argument(
            '--cache-key',
            default='location_to_postcodes_mapping',
            help='Cache key to use for storing the mapping (default: location_to_postcodes_mapping)',
        )
        parser.add_argument(
            '--cache-version',
            default='1',
            help='Cache version to use (default: 1)',
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force rebuild even if mapping exists in Redis'
        )
        parser.add_argument(
            '--incremental',
            action='store_true',
            help='Only process locations not already in the mapping'
        )

    def handle(self, *args, **options):
        verbose = options['verbose']
        cache_key = options['cache_key']
        cache_version = options['cache_version']
        force = options['force']
        incremental = options['incremental']
        
        self.stdout.write(self.style.SUCCESS('Starting location mapping build...'))
        
        # First test Redis connectivity
        try:
            # Try to set and retrieve a test value
            test_key = 'redis_test_key'
            test_value = 'test_value'
            cache_set_result = cache.set(test_key, test_value, timeout=10)
            
            if cache_set_result is False:
                self.stdout.write(self.style.ERROR('Redis connectivity test failed: cache.set() returned False'))
                return
                
            retrieved_value = cache.get(test_key)
            if retrieved_value != test_value:
                self.stdout.write(self.style.ERROR(f'Redis connectivity test failed: Retrieved value "{retrieved_value}" does not match set value "{test_value}"'))
                return
                
            self.stdout.write(self.style.SUCCESS('Redis connectivity test successful'))
            
            # Check if we already have a mapping in Redis
            existing_mapping = cache.get(cache_key, version=cache_version)
            existing_count = len(existing_mapping) if existing_mapping else 0
            
            if existing_mapping and not force and not incremental:
                self.stdout.write(self.style.SUCCESS(f'Existing mapping found in Redis with {existing_count} locations'))
                if verbose:
                    self.stdout.write('Run with --force to rebuild, or --incremental to only process new locations')
                return
            elif existing_mapping and force and not incremental:
                self.stdout.write(f'Existing mapping found with {existing_count} locations, but --force specified. Rebuilding...')
                existing_mapping = {}  # Clear for full rebuild
            elif existing_mapping and incremental:
                self.stdout.write(f'Existing mapping found with {existing_count} locations. Running in incremental mode...')
            elif not existing_mapping:
                self.stdout.write('No existing mapping found in Redis. Building from scratch...')
                existing_mapping = {}
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error testing Redis connectivity: {e}'))
            self.stdout.write(self.style.ERROR('Make sure Redis is running and properly configured in settings.py'))
            return
        
        start_time = time.time()
        
        try:
            # Import and set up
            from checker.models import Component
            from django.db.models import Q, Count
            import re
            
            # Get a list of unique locations with any component count
            self.stdout.write('Fetching unique locations from database...')
            locations_query_start = time.time()
            
            locations = Component.objects.values('location').exclude(
                Q(location='') | Q(location__isnull=True)
            ).annotate(
                count=Count('id')  # Count actual components for each location
            ).distinct()
            
            # Convert to a list
            locations_list = list(locations)
            locations_count = len(locations_list)
            
            if verbose:
                self.stdout.write(f'Found {locations_count} unique locations (took {time.time() - locations_query_start:.3f}s)')
            
            # Start with the core hardcoded mappings for very common locations
            # These should always be included regardless of incremental mode
            base_mapping = {
                "nottingham": ["NG1", "NG2", "NG3", "NG4", "NG5", "NG6", "NG7", "NG8", "NG9",
                             "NG10", "NG11", "NG12", "NG14", "NG15", "NG16", "NG17", "NG18"],
                "london": ["SW", "SE", "W", "E", "N", "NW", "EC", "WC"],
                "manchester": ["M1", "M2", "M3", "M4", "M8", "M11", "M12", "M13", "M14", "M15", "M16"],
                "birmingham": ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "B10"],
                "clapham": ["SW4", "SW11"],
                "battersea": ["SW11", "SW8"],
                "peckham": ["SE15", "SE5"],
                "sheffield": ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10", "S11"],
                "liverpool": ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "L10"],
                "bristol": ["BS1", "BS2", "BS3", "BS4", "BS5", "BS6", "BS7", "BS8", "BS9"],
                "glasgow": ["G1", "G2", "G3", "G4", "G5", "G11", "G12", "G13", "G14"],
                "edinburgh": ["EH1", "EH2", "EH3", "EH4", "EH5", "EH6", "EH7", "EH8", "EH9"],
                "leeds": ["LS1", "LS2", "LS3", "LS4", "LS5", "LS6", "LS7", "LS8", "LS9"],
                "cardiff": ["CF1", "CF2", "CF3", "CF4", "CF5", "CF10", "CF11"],
                "belfast": ["BT1", "BT2", "BT3", "BT4", "BT5", "BT6", "BT7", "BT8", "BT9"],
            }
            
            # Start with the existing mapping if in incremental mode, or base mapping otherwise
            if incremental and existing_mapping:
                mapping = existing_mapping
                # Make sure core mappings are always included/updated
                for place, codes in base_mapping.items():
                    mapping[place] = codes
            else:
                mapping = base_mapping.copy()
            
            # For each location, extract the main place name and associate with outward_code
            processed_count = 0
            skipped_count = 0
            updated_count = 0
            
            for i, loc_dict in enumerate(locations_list):
                if i % 100 == 0 and i > 0 and verbose:
                    elapsed = time.time() - start_time
                    self.stdout.write(f'Processed {i}/{locations_count} locations ({i/locations_count*100:.1f}%) in {elapsed:.1f}s')
                
                processed_count += 1
                location = loc_dict['location']
                if not location:
                    continue
                    
                # Extract main place name (first part before comma/other delimiters)
                parts = re.split(r'[,\n]', location)
                place_name = parts[0].strip().lower() if parts else None
                
                if not place_name or len(place_name) < 3:
                    continue
                    
                # Skip if already in mapping and we're in incremental mode
                if place_name in mapping and incremental:
                    skipped_count += 1
                    continue
    
                # Find all components with this place name in their location
                # and get their outward codes
                components = Component.objects.filter(
                    location__istartswith=place_name
                ).exclude(
                    outward_code=''
                ).values_list('outward_code', flat=True).distinct()
                
                # Add to mapping if we found outward codes
                outward_codes = list(set([code.upper() for code in components if code]))
                if outward_codes:
                    mapping[place_name] = outward_codes
                    updated_count += 1
            
            # Store the mapping in Redis with no expiration
            cache_result = cache.set(cache_key, mapping, timeout=None, version=cache_version)
            
            if cache_result is False:
                self.stdout.write(self.style.ERROR('Failed to save mapping to Redis - cache.set() returned False'))
                return
                
            # Record the size of our mapping
            mapping_size = len(mapping)
            build_time = time.time() - start_time
            
            # Different success message based on mode
            if incremental:
                self.stdout.write(self.style.SUCCESS(
                    f'Successfully updated location mapping in Redis: {updated_count} locations added, {skipped_count} skipped, {mapping_size} total (in {build_time:.2f}s)'
                ))
            else:
                self.stdout.write(self.style.SUCCESS(
                    f'Successfully built and cached complete location mapping with {mapping_size} locations in {build_time:.2f}s'
                ))
            
            # Show the first 5 entries as a preview
            if verbose:
                self.stdout.write('First 5 entries in mapping:')
                for i, (location, outcodes) in enumerate(list(mapping.items())[:5]):
                    self.stdout.write(f'  {location}: {outcodes}')
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error building location mapping: {e}'))
            import traceback
            self.stdout.write(self.style.ERROR(traceback.format_exc()))
            return 