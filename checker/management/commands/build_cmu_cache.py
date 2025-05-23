from django.core.management.base import BaseCommand
from django.core.cache import cache
from checker.services.data_access import get_cmu_dataframe
import time
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Builds and caches the CMU dataframe in Redis with no expiration'

    def add_arguments(self, parser):
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Increase output verbosity',
        )

    def handle(self, *args, **options):
        """Build and cache the CMU dataframe."""
        verbose = options.get('verbose', False)
        
        if verbose:
            self.stdout.write('Starting CMU dataframe build...')
            
        # Test Redis connectivity
        try:
            cache.set('redis_test', 'ok')
            test_result = cache.get('redis_test')
            if test_result == 'ok':
                self.stdout.write('Redis connectivity test successful')
            else:
                self.stdout.write(self.style.ERROR('Redis connectivity test failed - unexpected value returned'))
                return
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Redis connectivity test failed: {str(e)}'))
            return
            
        # Start timing
        start_time = time.time()
        
        # Force rebuild the dataframe
        try:
            df, build_time = get_cmu_dataframe(force_rebuild=True)
            
            if df is None:
                self.stdout.write(self.style.ERROR('Failed to build CMU dataframe'))
                return
                
            # Report success
            record_count = len(df)
            total_time = time.time() - start_time
            
            self.stdout.write(self.style.SUCCESS(
                f'Successfully built and cached CMU dataframe with {record_count} records in {total_time:.2f}s'
            ))
            
            # Show some statistics in verbose mode
            if verbose:
                company_count = df['Name of Applicant'].nunique()
                years = sorted(df['Delivery Year'].unique())
                
                self.stdout.write(f'Unique companies: {company_count}')
                self.stdout.write(f'Delivery years: {", ".join(str(y) for y in years)}')
                
                # Show a sample
                self.stdout.write('\nSample records (first 5):')
                sample = df.head(5)[['CMU ID', 'Name of Applicant', 'Delivery Year', 'Auction Name']]
                for i, row in sample.iterrows():
                    self.stdout.write(f"  {row['CMU ID']}: {row['Name of Applicant']} - {row['Delivery Year']} ({row['Auction Name']})")
                    
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error building CMU dataframe: {str(e)}')) 