import os
import re
import json
import time
from django.core.management.base import BaseCommand
from django.db.models import Count, Q
from collections import defaultdict
from tqdm import tqdm

from checker.models import Component
from checker.services.postcode_helpers import (
    refresh_postcode_mapping,
    get_location_to_postcodes_mapping,
    add_location_to_mapping,
    _MAPPING_CACHE
)


class Command(BaseCommand):
    help = 'Adds missing common locations to the postcode mappings'

    def add_arguments(self, parser):
        parser.add_argument(
            '--min-components',
            type=int,
            default=10,
            help='Minimum number of components needed to include a location in the mapping',
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=30,
            help='Maximum number of locations to add',
        )
        parser.add_argument(
            '--print-mapping',
            action='store_true',
            help='Print added locations after processing',
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Print detailed debugging information',
        )

    def handle(self, *args, **options):
        min_components = options.get('min_components')
        limit = options.get('limit')
        print_mapping = options.get('print_mapping')
        debug = options.get('debug', False)
        
        self.stdout.write(self.style.SUCCESS('=== Adding Missing Locations ==='))
        
        # Get current mapping
        start_time = time.time()
        current_mapping = get_location_to_postcodes_mapping()
        initial_location_count = len(current_mapping)
        self.stdout.write(f'Current mapping has {initial_location_count} locations')
        
        if debug:
            self.stdout.write("Current mapping keys:")
            for key in sorted(current_mapping.keys())[:10]:
                self.stdout.write(f"  - {key}: {current_mapping[key]}")
            self.stdout.write(f"Internal cache has {len(_MAPPING_CACHE)} locations")
        
        # Find missing common locations
        locations_to_add = self.find_missing_locations(current_mapping, min_components, limit)
        
        # Process and add them to the mapping
        if locations_to_add:
            self.stdout.write(f'Processing {len(locations_to_add)} missing locations...')
            added_count = self.add_locations_to_mapping(locations_to_add, debug)
            
            if debug:
                self.stdout.write(f"After adding, internal cache has {len(_MAPPING_CACHE)} locations")
                self.stdout.write(f"Looking for added locations in the internal cache:")
                cache_has_locations = all(loc in _MAPPING_CACHE for loc in locations_to_add)
                self.stdout.write(f"All locations in cache: {cache_has_locations}")
            
            # Show what we've added to the in-memory cache before refresh
            if debug:
                self.stdout.write("Direct access to in-memory cache shows:")
                for loc in locations_to_add:
                    if loc in _MAPPING_CACHE:
                        self.stdout.write(f"  - {loc}: {_MAPPING_CACHE[loc]}")
                    else:
                        self.stdout.write(f"  - {loc}: Not found in cache!")
            
            # Refresh the mapping
            self.stdout.write('Refreshing mapping...')
            refresh_start = time.time()
            new_mapping_size = refresh_postcode_mapping()
            refresh_time = time.time() - refresh_start
            
            # Check what happened during the refresh
            if debug:
                new_mapping = get_location_to_postcodes_mapping()
                for loc in locations_to_add:
                    if loc in new_mapping:
                        self.stdout.write(f"After refresh: {loc} is present")
                    else:
                        self.stdout.write(f"After refresh: {loc} is MISSING")
            
            self.stdout.write(self.style.SUCCESS(
                f'Mapping refreshed, now contains {new_mapping_size} locations '
                f'({new_mapping_size - initial_location_count} added). Took {refresh_time:.2f}s.'
            ))
            
            # Print the updated mapping if requested
            if print_mapping:
                new_mapping = get_location_to_postcodes_mapping()
                self.stdout.write('Newly added locations:')
                for location in sorted(locations_to_add):
                    if location in new_mapping:
                        postcodes = new_mapping[location]
                        self.stdout.write(f'  - {location}: {", ".join(postcodes)}')
        else:
            self.stdout.write(self.style.SUCCESS('No locations to add.'))
        
        total_time = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(f'Total execution time: {total_time:.2f}s'))
    
    def find_missing_locations(self, current_mapping, min_components=10, limit=30):
        """Find locations that have components but aren't in the mapping"""
        self.stdout.write('Finding missing locations with significant component counts...')
        
        # Get locations that appear frequently in the database
        location_counts = (
            Component.objects.exclude(location='')
            .exclude(location__isnull=True)
            .values('location')
            .annotate(count=Count('location'))
            .filter(count__gte=min_components)
            .order_by('-count')
        )
        
        self.stdout.write(f'Found {location_counts.count()} locations with at least {min_components} components')
        
        # Extract place names
        missing_locations = []
        location_to_component_count = {}
        
        for loc_dict in tqdm(location_counts):
            location = loc_dict['location']
            count = loc_dict['count']
            
            # Extract main place name (first part before comma/other delimiters)
            parts = re.split(r'[,\n]', location)
            place_name = parts[0].strip().lower() if parts else None
            
            if not place_name or len(place_name) < 3 or any(c.isdigit() for c in place_name[:2]):
                # Skip locations that start with numbers or are too short
                continue
                
            # Skip placeholders and non-geographic terms
            skip_terms = [
                'component', 'provided', 'to be', 'yet to', 'after', 'power station',
                'works', 'confirmed', 'tbc', 'to be confirmed', 'awaiting', 'not known'
            ]
            
            if any(term in place_name.lower() for term in skip_terms):
                continue
                
            # Check if this place is already in the mapping
            if place_name not in current_mapping:
                # Make sure the location has components with outward_codes
                has_outward_codes = Component.objects.filter(
                    location__istartswith=place_name
                ).exclude(
                    outward_code=''
                ).exists()
                
                if has_outward_codes:
                    missing_locations.append(place_name)
                    location_to_component_count[place_name] = count
        
        # Sort by component count and take top ones
        top_missing = sorted(
            missing_locations, 
            key=lambda x: location_to_component_count.get(x, 0), 
            reverse=True
        )[:limit]
        
        # Report findings
        if top_missing:
            self.stdout.write(self.style.WARNING(
                f'Selected {len(top_missing)} locations to add out of {len(missing_locations)} missing'
            ))
            
            # Print top missing locations that will be added
            self.stdout.write('Locations to be added:')
            for place in top_missing:
                count = location_to_component_count.get(place, 0)
                self.stdout.write(f'  - {place} ({count} components)')
        else:
            self.stdout.write(self.style.SUCCESS('No suitable locations found to add'))
        
        return top_missing
    
    def add_locations_to_mapping(self, locations, debug=False):
        """Add locations to the internal mapping"""
        from django.db import models
        
        added_count = 0
        
        for location in tqdm(locations):
            # Find components with this location and get their outward codes
            components = Component.objects.filter(
                models.Q(location__istartswith=location) | 
                models.Q(location__icontains=f", {location}") |
                models.Q(location__iendswith=f" {location}")
            ).exclude(
                outward_code=''
            ).values_list('outward_code', flat=True).distinct()
            
            outward_codes = list(set([code.upper() for code in components if code]))
            
            if outward_codes:
                # Use the helper function to add to mapping
                success = add_location_to_mapping(location, outward_codes)
                if success:
                    added_count += 1
                    self.stdout.write(f'  Found {len(outward_codes)} outward codes for {location}')
                    if debug:
                        self.stdout.write(f"  Added to mapping: {location} -> {outward_codes}")
                        self.stdout.write(f"  Verify in cache: {location in _MAPPING_CACHE}")
                else:
                    self.stdout.write(self.style.ERROR(f'  Failed to add {location} to mapping'))
        
        return added_count 