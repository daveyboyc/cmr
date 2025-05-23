from django.core.management.base import BaseCommand
from django.core.cache import cache
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Simple test to create a minimal location mapping in Redis'

    def handle(self, *args, **options):
        self.stdout.write('Testing simple location mapping in Redis...')
        
        try:
            # Create a simple mapping with just a few locations
            mapping = {
                "london": ["SW", "SE", "W", "E", "N", "NW", "EC", "WC"],
                "manchester": ["M1", "M2", "M3", "M4"],
                "birmingham": ["B1", "B2", "B3", "B4"],
            }
            
            # Cache key - same as the main command uses
            cache_key = 'location_to_postcodes_mapping'
            cache_version = '1'
            
            # Try to set with timeout=None (permanent)
            self.stdout.write(f"Setting location mapping with {len(mapping)} locations")
            set_result = cache.set(cache_key, mapping, timeout=None, version=cache_version)
            self.stdout.write(f"cache.set() returned: {set_result}")
            
            # Verify by retrieving
            get_result = cache.get(cache_key, version=cache_version)
            
            if get_result:
                self.stdout.write(self.style.SUCCESS(f'✅ Successfully retrieved mapping with {len(get_result)} locations'))
                for location, codes in get_result.items():
                    self.stdout.write(f"  {location}: {codes}")
            else:
                self.stdout.write(self.style.ERROR(f'❌ Failed to retrieve mapping - got {get_result}'))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ ERROR: {str(e)}'))
            import traceback
            self.stdout.write(self.style.ERROR(traceback.format_exc())) 