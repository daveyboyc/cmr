from django.core.management.base import BaseCommand
from django.core.cache import cache
from checker.services.company_search import build_cached_company_links
from checker.services.data_access import get_cmu_dataframe
import time
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Builds and caches company links for common search terms'

    def add_arguments(self, parser):
        parser.add_argument(
            'search_terms',
            nargs='*',
            help='Space-separated list of search terms to cache (e.g., "energy storage" "solar" "gas")',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Increase output verbosity',
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force rebuild even if cache already exists',
        )
        parser.add_argument(
            '--popular',
            action='store_true',
            help='Build cache for predefined list of popular search terms',
        )

    def handle(self, *args, **options):
        """Build and cache company links for specified search terms."""
        verbose = options.get('verbose', False)
        force_rebuild = options.get('force', False)
        use_popular = options.get('popular', False)
        search_terms = options.get('search_terms', [])
        
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
        
        # If popular flag is set, use predefined list of popular search terms
        if use_popular:
            search_terms = [
                'energy storage',
                'solar',
                'gas',
                'nuclear',
                'wind',
                'battery',
                'hydro',
                'ess',
                'chp',
                'drax',
                'scottishpower',
                'edf',
                'eon',
                'npower',
                'centrica',
                'london',
                'manchester',
                'birmingham',
                'glasgow',
                'edinburgh',
            ]
            self.stdout.write(f'Using {len(search_terms)} popular search terms')
        
        if not search_terms:
            self.stdout.write(self.style.WARNING('No search terms provided. Use --popular for predefined terms or provide your own.'))
            return
            
        # Get CMU dataframe
        self.stdout.write('Loading CMU dataframe...')
        try:
            df, load_time = get_cmu_dataframe()
            if df is None:
                self.stdout.write(self.style.ERROR('Failed to load CMU dataframe'))
                return
            self.stdout.write(f'Loaded CMU dataframe with {len(df)} records in {load_time:.2f}s')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error loading CMU dataframe: {str(e)}'))
            return
        
        # Process each search term
        total_start = time.time()
        successful = 0
        failed = 0
        
        for term in search_terms:
            term = term.strip()
            if not term:
                continue
                
            try:
                self.stdout.write(f'Building cache for "{term}"...')
                start_time = time.time()
                
                # Build cache
                links = build_cached_company_links(term, df, force_rebuild=force_rebuild)
                
                # Report results
                end_time = time.time()
                if links:
                    self.stdout.write(self.style.SUCCESS(
                        f'Successfully cached {len(links)} company links for "{term}" in {end_time - start_time:.2f}s'
                    ))
                    successful += 1
                else:
                    self.stdout.write(self.style.WARNING(
                        f'No company links found for "{term}" (cached empty result)'
                    ))
                    successful += 1  # Still successful, just empty result
                    
                if verbose:
                    self.stdout.write(f'  First few matches:')
                    for i, link in enumerate(links[:3]):
                        self.stdout.write(f'  {i+1}. {link.get("company_name", "Unknown")}')
                    
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Error caching "{term}": {str(e)}'))
                failed += 1
        
        # Final summary
        total_time = time.time() - total_start
        self.stdout.write(self.style.SUCCESS(
            f'Cache building completed in {total_time:.2f}s. '
            f'Successful: {successful}, Failed: {failed}'
        )) 