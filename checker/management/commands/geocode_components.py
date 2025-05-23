from django.core.management.base import BaseCommand
import requests
import time
from django.conf import settings
from checker.models import Component

class Command(BaseCommand):
    help = 'Geocode component locations using Google Maps API'

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=500, 
                          help='Maximum number of components to process (default: 500)')
        parser.add_argument('--force', action='store_true', 
                          help='Re-geocode already processed locations')
        parser.add_argument('--batch', type=int, default=50, 
                          help='Batch size for status updates (default: 50)')
        parser.add_argument(
            '--exclude-companies',
            nargs='+',  # Allows specifying multiple company names
            metavar='COMPANY_NAME',
            help='List of company names to exclude from geocoding (case-insensitive)'
        )

    def handle(self, *args, **options):
        limit = options['limit']
        force = options['force']
        batch_size = options['batch']
        excluded_companies = options['exclude_companies'] # Get the list of excluded companies
        
        # Get API key from settings
        api_key = getattr(settings, 'GOOGLE_MAPS_API_KEY', None)
        if not api_key:
            self.stderr.write('ERROR: GOOGLE_MAPS_API_KEY not found in settings')
            return
        
        # Build query for components to geocode
        query = Component.objects.all()
        if not force:
            query = query.filter(geocoded=False)
        
        # Apply company exclusion filter if provided
        if excluded_companies:
            # Use case-insensitive exclusion
            for company_name in excluded_companies:
                query = query.exclude(company_name__iexact=company_name)
            self.stdout.write(self.style.NOTICE(f"Excluding companies: {', '.join(excluded_companies)}"))
            
        if limit > 0:
            query = query[:limit]
            
        total = query.count()
        self.stdout.write(f'Found {total} components to geocode')
        
        processed = 0
        success = 0
        errors = 0
        
        for idx, component in enumerate(query, 1):
            if not component.location:
                self.stdout.write(f'Skipping component {component.id}: No location data')
                component.geocoded = True  # Mark as processed even though we can't geocode it
                component.save(update_fields=['geocoded'])
                processed += 1
                continue

            # Use the location field directly as the address string
            address_string = component.location

            try:
                # Log the address being used BEFORE the API call
                self.stdout.write(self.style.NOTICE(f"  -> Geocoding address: '{address_string}'"))

                # Call Google Geocoding API using the constructed address_string
                response = requests.get(
                    'https://maps.googleapis.com/maps/api/geocode/json',
                    params={
                        'address': address_string, # USE THE CONSTRUCTED STRING
                        'key': api_key,
                        'region': 'uk'  # Focus on UK
                    }
                )
                
                data = response.json()
                
                if data['status'] == 'OK' and data['results']:
                    location = data['results'][0]['geometry']['location']
                    component.latitude = location['lat']
                    component.longitude = location['lng']
                    component.geocoded = True
                    component.save()
                    success += 1
                    
                    if idx % batch_size == 0:
                        self.stdout.write(f'Progress: {idx}/{total} components processed')
                else:
                    self.stdout.write(f'Error geocoding {component.location}: {data.get("status", "Unknown error")}')
                    errors += 1
                
                # Sleep to avoid hitting API rate limits
                time.sleep(0.2)
                
            except Exception as e:
                self.stderr.write(f'Error processing component {component.id}: {str(e)}')
                errors += 1
                
            processed += 1
                
        self.stdout.write(self.style.SUCCESS(
            f'Geocoding completed: {processed} processed, {success} successful, {errors} errors'
        )) 