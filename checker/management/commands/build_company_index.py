from django.core.management.base import BaseCommand
from django.core.cache import cache
from checker.services.data_access import get_cmu_dataframe
import time
import logging
import pickle
import base64
from checker.utils import normalize

logger = logging.getLogger(__name__)

# Redis key for the company index
COMPANY_INDEX_KEY = "company_index_v1"
# Redis key for the last update timestamp
COMPANY_INDEX_UPDATE_KEY = "company_index_last_updated"
# Redis expiration time - 30 days (should be rebuilt after each crawl)
COMPANY_INDEX_TTL = 3600 * 24 * 30

class Command(BaseCommand):
    help = 'Builds a complete company index in Redis for all components'

    def add_arguments(self, parser):
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Increase output verbosity',
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force rebuild even if index already exists',
        )

    def handle(self, *args, **options):
        """Build a complete index of all companies in Redis."""
        verbose = options.get('verbose', False)
        force_rebuild = options.get('force', False)
        
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
        
        # Check if index already exists (unless force rebuild is specified)
        if not force_rebuild:
            index_exists = cache.get(COMPANY_INDEX_KEY)
            if index_exists:
                last_updated = cache.get(COMPANY_INDEX_UPDATE_KEY)
                if last_updated:
                    self.stdout.write(self.style.WARNING(
                        f'Company index already exists in Redis (last updated: {time.ctime(float(last_updated))}).\n'
                        f'Use --force to rebuild.'
                    ))
                    return
        
        # Get CMU dataframe
        self.stdout.write('Loading CMU dataframe...')
        start_time = time.time()
        try:
            df, load_time = get_cmu_dataframe()
            if df is None:
                self.stdout.write(self.style.ERROR('Failed to load CMU dataframe'))
                return
            self.stdout.write(f'Loaded CMU dataframe with {len(df)} records in {load_time:.2f}s')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error loading CMU dataframe: {str(e)}'))
            return
        
        # Process all unique companies
        self.stdout.write('Building complete company index...')
        company_build_start = time.time()
        
        # Get all unique company names
        unique_companies = df["Full Name"].unique()
        company_count = len(unique_companies)
        self.stdout.write(f'Found {company_count} unique companies')
        
        # Index to store all company data
        company_index = {}
        processed_count = 0
        skipped_count = 0
        
        for company_name in unique_companies:
            processed_count += 1
            
            if not company_name or company_name == "nan":
                skipped_count += 1
                continue
                
            try:
                # Get normalized company name for the key
                normalized_name = normalize(company_name)
                
                # Get company records
                company_df = df[df["Full Name"] == company_name]
                
                # Get counts
                num_records = len(company_df)
                cmu_ids = company_df["CMU ID"].unique()
                num_cmu_ids = len(cmu_ids)
                
                # Extract year data
                year_auctions = {}
                for idx, row in company_df.iterrows():
                    year = row.get("Delivery Year", "").strip()
                    auction = row.get("Auction Name", "").strip()
                    
                    if year and auction:
                        if year not in year_auctions:
                            year_auctions[year] = {}
                        if auction not in year_auctions[year]:
                            year_auctions[year][auction] = True
                
                # Store company metadata
                company_url_name = normalized_name.replace(" ", "").lower()
                
                # Build years html
                years_html = ""
                sorted_years = sorted(year_auctions.keys(), reverse=True)
                for year in sorted_years:
                    years_html += f'<span class="badge rounded-pill bg-primary me-1">{year}</span>'
                
                # Build HTML for this company
                company_html = f"""
                <div>
                    <strong><div><strong><a href="/company/{company_url_name}/">{company_name}</a></strong></div></strong>
                    <div class="small">
                        <span class="text-muted">{num_cmu_ids} CMU ID{'' if num_cmu_ids == 1 else 's'}, {num_records} component{'' if num_records == 1 else 's'}</span>
                    </div>
                    <div class="mt-2">
                        {years_html}
                    </div>
                </div>
                """
                
                # Store the company data in the index
                company_index[normalized_name] = {
                    "html": company_html,
                    "company_name": company_name,
                    "normalized_name": normalized_name,
                    "num_cmu_ids": num_cmu_ids,
                    "num_components": num_records,
                    "years": sorted_years,
                    "url": f"/company/{company_url_name}/",
                    "cmu_ids": list(cmu_ids)
                }
                
                if verbose and processed_count % 100 == 0:
                    self.stdout.write(f'Processed {processed_count}/{company_count} companies...')
                
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Error processing company "{company_name}": {str(e)}'))
                skipped_count += 1
        
        # Store the index in Redis
        try:
            serialized_index = base64.b64encode(pickle.dumps(company_index)).decode('utf-8')
            cache.set(COMPANY_INDEX_KEY, serialized_index, COMPANY_INDEX_TTL)
            cache.set(COMPANY_INDEX_UPDATE_KEY, str(time.time()), COMPANY_INDEX_TTL)
            
            # Calculate stats
            total_time = time.time() - company_build_start
            per_company_time = total_time / (processed_count - skipped_count) if processed_count > skipped_count else 0
            index_size = len(serialized_index) / 1024  # KB
            
            self.stdout.write(self.style.SUCCESS(
                f'Successfully built company index in {total_time:.2f}s:\n'
                f'- Processed {processed_count} companies\n'
                f'- Included {len(company_index)} companies in index\n'
                f'- Skipped {skipped_count} companies\n'
                f'- Average time per company: {per_company_time:.4f}s\n'
                f'- Index size: {index_size:.1f} KB\n'
                f'- Cache TTL: {COMPANY_INDEX_TTL/3600/24:.1f} days'
            ))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error storing company index in Redis: {str(e)}'))
            return 