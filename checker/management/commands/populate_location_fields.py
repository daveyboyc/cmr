import re
import logging
from django.core.management.base import BaseCommand
from checker.models import Component

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Populates county and outward_code fields in the Component model from location data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit',
            type=int,
            help='Limit the number of components to process',
        )
        
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force update even if fields are already populated',
        )

    def handle(self, *args, **options):
        limit = options.get('limit')
        force = options.get('force')
        
        # UK postcode regex pattern for extracting postcodes
        # This pattern matches most UK postcodes
        postcode_pattern = r'[A-Za-z]{1,2}[0-9][A-Za-z0-9]?(\s*[0-9][A-Za-z]{2})?'
        
        # Common UK counties for lookup
        uk_counties = [
            'Nottinghamshire', 'Nottingham', 'Notts',
            'Derbyshire', 'Derby',
            'Leicestershire', 'Leicester',
            'Lincolnshire', 'Lincoln',
            'Yorkshire', 'York',
            'Lancashire', 'Lancaster',
            'London', 'Greater London',
            'Essex', 'Kent', 'Surrey', 'Sussex',
            'Hampshire', 'Dorset', 'Devon', 'Cornwall',
            'Somerset', 'Wiltshire', 'Gloucestershire',
            'Oxfordshire', 'Berkshire', 'Buckinghamshire',
            'Hertfordshire', 'Bedfordshire', 'Cambridgeshire',
            'Norfolk', 'Suffolk', 'Northamptonshire',
            'Warwickshire', 'Worcestershire', 'Herefordshire',
            'Shropshire', 'Staffordshire', 'Cheshire',
            'Cumbria', 'Durham', 'Northumberland',
            'Tyne and Wear', 'Merseyside', 'Greater Manchester',
            'West Midlands', 'South Yorkshire', 'West Yorkshire',
            'North Yorkshire', 'East Yorkshire',
        ]
        
        # NG postcodes are in Nottinghamshire
        outcode_to_county = {
            'NG': 'Nottinghamshire',
        }
        
        # Get components to process
        components_query = Component.objects.all()
        
        # Filter to only process records without data (unless force=True)
        if not force:
            components_query = components_query.filter(county__isnull=True, outward_code__isnull=True)
        
        # Apply limit if provided
        if limit:
            components_query = components_query[:limit]
        
        # Count total to process
        total = components_query.count()
        self.stdout.write(f"Processing {total} components...")
        
        # Track progress
        processed = 0
        updated = 0
        
        # Process each component
        for component in components_query.iterator():
            location = component.location or ""
            
            # Extract postcode using regex
            postcode_match = re.search(postcode_pattern, location, re.IGNORECASE)
            outward_code = None
            if postcode_match:
                postcode = postcode_match.group(0).strip().upper()
                # Extract outward code (first part of postcode)
                if ' ' in postcode:
                    outward_code = postcode.split(' ')[0]
                else:
                    # For postcodes without a space, try to split at position where numbers start
                    for i, char in enumerate(postcode):
                        if char.isdigit():
                            outward_code = postcode[:i+1]
                            break
                    
                    # If we couldn't find a digit, use the first 2-3 chars
                    if not outward_code and len(postcode) >= 2:
                        outward_code = postcode[:min(3, len(postcode))]
            
            # Extract county by looking for county names in the location
            county = None
            location_upper = location.upper()
            for potential_county in uk_counties:
                if potential_county.upper() in location_upper:
                    county = potential_county
                    break
            
            # If no county found but we have an outward code, try to infer county
            if not county and outward_code:
                # Check if outward code prefix matches known county
                for prefix, mapped_county in outcode_to_county.items():
                    if outward_code.startswith(prefix):
                        county = mapped_county
                        break
            
            # Special case for Nottingham: all NG postcodes are in Nottinghamshire
            if outward_code and outward_code.startswith('NG') and not county:
                county = 'Nottinghamshire'
            
            # Update the component if we found data
            if outward_code or county:
                if outward_code:
                    component.outward_code = outward_code
                
                if county:
                    component.county = county
                
                component.save(update_fields=['outward_code', 'county'])
                updated += 1
            
            processed += 1
            
            # Show progress
            if processed % 100 == 0:
                self.stdout.write(f"Processed {processed}/{total} components")
        
        self.stdout.write(self.style.SUCCESS(f"Completed! Processed {processed} components, updated {updated}")) 