import re
from django.core.management.base import BaseCommand
from checker.models import Component
from django.db.models import Q

class Command(BaseCommand):
    help = 'Check if a location exists in components and its outward codes'

    def add_arguments(self, parser):
        parser.add_argument('location', type=str, help='The location to search for')
        parser.add_argument('--limit', type=int, default=5, help='Limit the number of results to display')

    def handle(self, *args, **options):
        location = options['location']
        limit = options['limit']
        
        # Search for components with this location
        components = Component.objects.filter(location__icontains=location)
        count = components.count()
        
        self.stdout.write(f'Found {count} components with "{location}" in their location')
        
        # Extract outward codes
        outward_codes = set()
        for comp in components[:100]:  # Only check first 100 to avoid performance issues
            if comp.outward_code:
                outward_codes.add(comp.outward_code.upper())
        
        self.stdout.write(f'Unique outward codes: {", ".join(sorted(outward_codes)) if outward_codes else "None"}')
        
        # Show sample components
        self.stdout.write('\nSample components:')
        for comp in components[:limit]:
            self.stdout.write(f'- {comp.location} (Outward code: {comp.outward_code})')
        
        # Check if the location is in the county field
        county_components = Component.objects.filter(county__icontains=location)
        county_count = county_components.count()
        
        self.stdout.write(f'\nFound {county_count} components with "{location}" in their county field')
        
        # Show sample county components
        if county_count > 0:
            self.stdout.write('Sample county components:')
            for comp in county_components[:limit]:
                self.stdout.write(f'- County: {comp.county}, Location: {comp.location}') 