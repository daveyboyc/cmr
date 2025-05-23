from django.core.management.base import BaseCommand
from django.core.cache import cache
import logging
import json

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Test Redis connectivity with a simple cache operation'

    def handle(self, *args, **options):
        self.stdout.write('Testing Redis connectivity...')
        
        try:
            # 1. Basic string test
            test_key = 'test_redis_string'
            test_value = 'test_value'
            self.stdout.write(f"Setting '{test_key}' to '{test_value}'")
            
            set_result = cache.set(test_key, test_value, timeout=60)
            self.stdout.write(f"cache.set() returned: {set_result}")
            
            get_result = cache.get(test_key)
            self.stdout.write(f"cache.get() returned: {get_result}")
            
            if get_result == test_value:
                self.stdout.write(self.style.SUCCESS('✅ BASIC STRING TEST PASSED'))
            else:
                self.stdout.write(self.style.ERROR(f'❌ BASIC STRING TEST FAILED - Got {get_result} instead of {test_value}'))
            
            # 2. Dictionary test
            test_dict_key = 'test_redis_dict'
            test_dict = {
                'name': 'test',
                'value': 123,
                'list': [1, 2, 3],
                'nested': {'a': 1, 'b': 2}
            }
            self.stdout.write(f"Setting '{test_dict_key}' to dictionary")
            
            dict_set_result = cache.set(test_dict_key, test_dict, timeout=60)
            self.stdout.write(f"cache.set() for dict returned: {dict_set_result}")
            
            dict_get_result = cache.get(test_dict_key)
            self.stdout.write(f"cache.get() for dict returned: {json.dumps(dict_get_result, default=str)}")
            
            if dict_get_result == test_dict:
                self.stdout.write(self.style.SUCCESS('✅ DICTIONARY TEST PASSED'))
            else:
                self.stdout.write(self.style.ERROR('❌ DICTIONARY TEST FAILED'))
            
            # 3. Timeout=None test (permanent storage)
            test_perm_key = 'test_redis_permanent'
            test_perm_value = 'permanent_value'
            self.stdout.write(f"Testing permanent storage with timeout=None")
            
            perm_set_result = cache.set(test_perm_key, test_perm_value, timeout=None)
            self.stdout.write(f"cache.set() with timeout=None returned: {perm_set_result}")
            
            perm_get_result = cache.get(test_perm_key)
            self.stdout.write(f"cache.get() for permanent key returned: {perm_get_result}")
            
            if perm_get_result == test_perm_value:
                self.stdout.write(self.style.SUCCESS('✅ PERMANENT STORAGE TEST PASSED'))
            else:
                self.stdout.write(self.style.ERROR(f'❌ PERMANENT STORAGE TEST FAILED - Got {perm_get_result} instead of {test_perm_value}'))
            
            # 4. Version test
            test_version_key = 'test_redis_version'
            test_version_v1 = 'version1'
            test_version_v2 = 'version2'
            
            version1_set = cache.set(test_version_key, test_version_v1, timeout=60, version=1)
            version2_set = cache.set(test_version_key, test_version_v2, timeout=60, version=2)
            
            version1_get = cache.get(test_version_key, version=1)
            version2_get = cache.get(test_version_key, version=2)
            
            self.stdout.write(f"Version 1 value: {version1_get}")
            self.stdout.write(f"Version 2 value: {version2_get}")
            
            if version1_get == test_version_v1 and version2_get == test_version_v2:
                self.stdout.write(self.style.SUCCESS('✅ VERSION TEST PASSED'))
            else:
                self.stdout.write(self.style.ERROR('❌ VERSION TEST FAILED'))
                
            self.stdout.write(self.style.SUCCESS("ALL TESTS COMPLETED"))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ ERROR DURING TESTING: {str(e)}'))
            import traceback
            self.stdout.write(self.style.ERROR(traceback.format_exc())) 