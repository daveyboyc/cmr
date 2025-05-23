from django.core.management.base import BaseCommand
from django.core.cache import cache
import logging
import json

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Check the status of various Redis caches in the application'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Checking Redis cache status...'))
        
        try:
            # 1. Check location mapping
            location_key = 'location_to_postcodes_mapping'
            location_version = '1'
            location_mapping = cache.get(location_key, version=location_version)
            
            if location_mapping:
                location_count = len(location_mapping)
                self.stdout.write(self.style.SUCCESS(f'✅ LOCATION MAPPING: Found with {location_count} locations'))
                
                # Show some sample locations
                sample_locations = list(location_mapping.items())[:5]
                self.stdout.write('Sample mappings:')
                for location, codes in sample_locations:
                    self.stdout.write(f'  {location}: {codes}')
            else:
                self.stdout.write(self.style.ERROR(f'❌ LOCATION MAPPING: Not found in Redis'))
            
            # 2. Check CMU cache
            from checker.services.data_access import CMU_DATAFRAME_KEY
            cmu_df_pickle = cache.get(CMU_DATAFRAME_KEY)
            
            if cmu_df_pickle:
                self.stdout.write(self.style.SUCCESS(f'✅ CMU DATAFRAME: Found in Redis (binary data, size: {len(str(cmu_df_pickle))} bytes)'))
            else:
                self.stdout.write(self.style.ERROR(f'❌ CMU DATAFRAME: Not found in Redis'))
            
            # 3. Check company index
            company_index_key = 'company_index'
            company_index = cache.get(company_index_key)
            
            if company_index:
                company_count = len(company_index)
                self.stdout.write(self.style.SUCCESS(f'✅ COMPANY INDEX: Found with {company_count} companies'))
                
                # Show sample
                if isinstance(company_index, dict):
                    sample_companies = list(company_index.items())[:3]
                    self.stdout.write('Sample companies:')
                    for company, data in sample_companies:
                        self.stdout.write(f'  {company}: {str(data)[:100]}...')
            else:
                self.stdout.write(self.style.ERROR(f'❌ COMPANY INDEX: Not found in Redis'))
            
            # 4. Check map cache
            map_cache_key = 'map_data:ee5555cff4ec5228b0061260f9e17c3d'  # Known from logs
            map_cache = cache.get(map_cache_key)
            
            if map_cache:
                self.stdout.write(self.style.SUCCESS(f'✅ MAP CACHE: Found (size: {len(str(map_cache))} bytes)'))
            else:
                self.stdout.write(self.style.ERROR(f'❌ MAP CACHE: Not found in Redis'))
            
            # 5. List all keys in Redis (sample)
            self.stdout.write('\nTrying to list Redis keys (if available)...')
            import redis
            from django.conf import settings
            
            try:
                # Try to extract Redis URL from Django settings
                redis_url = settings.CACHES['default']['LOCATION']
                r = redis.from_url(redis_url)
                all_keys = r.keys('*')
                
                self.stdout.write(f'Found {len(all_keys)} total keys in Redis:')
                for i, key in enumerate(all_keys[:10]):  # Show first 10
                    if i >= 10:
                        self.stdout.write(f'... and {len(all_keys) - 10} more')
                        break
                    self.stdout.write(f'  {key}')
            except Exception as e:
                self.stdout.write(f'Could not list Redis keys: {e}')
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ ERROR DURING CHECK: {str(e)}'))
            import traceback
            self.stdout.write(self.style.ERROR(traceback.format_exc())) 