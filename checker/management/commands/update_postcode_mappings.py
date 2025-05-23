import os
import re
import json
import time
from django.core.management.base import BaseCommand
from django.db.models import Count, Q
from collections import defaultdict
from tqdm import tqdm
from django.core.cache import cache

from checker.models import Component
from checker.services.postcode_helpers import (
    get_location_to_postcodes_mapping, 
    refresh_postcode_mapping,
    get_all_postcodes_for_area
)


class Command(BaseCommand):
    help = 'Updates the dynamic postcode mappings from component data in the database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--force-rebuild',
            action='store_true',
            help='Force rebuild of the location-to-postcode mappings',
        )
        parser.add_argument(
            '--test-location',
            type=str,
            help='Test a specific location after updating',
        )
        parser.add_argument(
            '--print-mapping',
            action='store_true',
            help='Print the current mapping after updating',
        )
        parser.add_argument(
            '--min-components',
            type=int,
            default=3,
            help='Minimum number of components needed to include a location in the mapping',
        )

    def handle(self, *args, **options):
        force_rebuild = options['force_rebuild']
        test_location = options.get('test_location')
        print_mapping = options.get('print_mapping')
        min_components = options.get('min_components')
        
        self.stdout.write(self.style.SUCCESS('=== Postcode Mapping Update ==='))
        self.stdout.write('Checking the current mapping...')
        
        # Clear cache to ensure we get fresh data
        if force_rebuild:
            self.stdout.write('Clearing cache to force rebuild...')
            cache.delete_pattern("area_postcodes_*") if hasattr(cache, 'delete_pattern') else None
            
        # First get the current mapping
        start_time = time.time()
        current_mapping = get_location_to_postcodes_mapping()
        initial_location_count = len(current_mapping)
        self.stdout.write(f'Current mapping has {initial_location_count} locations')
        
        # Analyze missing locations
        self.analyze_missing_locations(current_mapping, min_components)
        
        # Force a refresh of the mapping
        self.stdout.write('Refreshing mapping from database...')
        refresh_start = time.time()
        new_mapping_size = refresh_postcode_mapping()
        refresh_time = time.time() - refresh_start
        
        self.stdout.write(self.style.SUCCESS(
            f'Mapping refreshed, now contains {new_mapping_size} locations '
            f'({new_mapping_size - initial_location_count} added). Took {refresh_time:.2f}s.'
        ))
        
        # If requested, print the current mapping
        if print_mapping:
            self.print_current_mapping()
            
        # If a test location was provided, test it
        if test_location:
            self.test_specific_location(test_location)
            
        total_time = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(f'Total execution time: {total_time:.2f}s'))
    
    def analyze_missing_locations(self, current_mapping, min_components=3):
        """Find locations that have components but aren't in the mapping"""
        self.stdout.write('Analyzing potential missing locations...')
        
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
            
            if not place_name or len(place_name) < 3:
                continue
                
            # Check if this place is already in the mapping
            if place_name not in current_mapping:
                missing_locations.append(place_name)
                location_to_component_count[place_name] = count
        
        # Report findings
        if missing_locations:
            self.stdout.write(self.style.WARNING(
                f'Found {len(missing_locations)} locations missing from the current mapping'
            ))
            
            # Print top missing locations
            self.stdout.write('Top missing locations:')
            for place in sorted(missing_locations, key=lambda x: location_to_component_count.get(x, 0), reverse=True)[:10]:
                count = location_to_component_count.get(place, 0)
                self.stdout.write(f'  - {place} ({count} components)')
        else:
            self.stdout.write(self.style.SUCCESS('No missing locations found'))
    
    def print_current_mapping(self):
        """Print the current mapping, sorted by location"""
        self.stdout.write('Current location-to-postcode mapping:')
        
        mapping = get_location_to_postcodes_mapping()
        for location in sorted(mapping.keys()):
            postcodes = mapping[location]
            self.stdout.write(f'  - {location}: {", ".join(postcodes)}')
    
    def test_specific_location(self, location):
        """Test how a specific location is handled by the system"""
        self.stdout.write(f'Testing location: {location}')
        
        # Get postcodes for this location
        postcodes = get_all_postcodes_for_area(location)
        
        if postcodes:
            self.stdout.write(self.style.SUCCESS(
                f'Found {len(postcodes)} postcodes for {location}: {", ".join(postcodes)}'
            ))
            
            # Count components with these postcodes
            count = 0
            for postcode in postcodes:
                components = Component.objects.filter(
                    Q(location__icontains=postcode) | 
                    Q(outward_code__iexact=postcode)
                ).count()
                
                self.stdout.write(f'  - {postcode}: {components} components')
                count += components
                
            self.stdout.write(self.style.SUCCESS(f'Total components for location: {count}'))
        else:
            self.stdout.write(self.style.ERROR(f'No postcodes found for {location}')) 