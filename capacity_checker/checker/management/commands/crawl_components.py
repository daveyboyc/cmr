# Save this as checker/management/commands/crawl_components.py

from django.core.management.base import BaseCommand
from checker.views import fetch_all_cmu_records, fetch_components_for_cmu_id, get_component_data_from_json
import pandas as pd
import time

class Command(BaseCommand):
    help = 'Crawl all components for all CMU IDs'

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=5000, help='Limit of CMU records to process')
        parser.add_argument('--timeout', type=int, default=120, help='Max seconds to run')
        parser.add_argument('--skip-existing', action='store_true', help='Skip CMU IDs that already have JSON data')
        parser.add_argument('--company', type=str, default=None, help='Only process CMU IDs for a specific company')

    def handle(self, *args, **options):
        limit = options['limit']
        max_time = options['timeout']
        skip_existing = options['skip_existing']
        company_filter = options['company']
        
        start_time = time.time()
        self.stdout.write(f"Starting crawler with {limit} limit and {max_time}s timeout")
        
        # Get all CMU records
        self.stdout.write("Fetching all CMU records...")
        all_records, _ = fetch_all_cmu_records(limit=limit)
        cmu_df = pd.DataFrame(all_records)
        
        # Get all CMU IDs
        possible_cmu_id_fields = ["CMU ID", "cmu_id", "CMU_ID", "cmuId", "id", "identifier", "ID"]
        cmu_id_field = next((field for field in possible_cmu_id_fields if field in cmu_df.columns), None)
        if cmu_id_field:
            cmu_df["CMU ID"] = cmu_df[cmu_id_field].fillna("N/A").astype(str)
        else:
            cmu_df["CMU ID"] = "N/A"
        
        # Process company name fields
        cmu_df["Name of Applicant"] = cmu_df.get("Name of Applicant", pd.Series()).fillna("").astype(str)
        cmu_df["Parent Company"] = cmu_df.get("Parent Company", pd.Series()).fillna("").astype(str)
        
        cmu_df["Full Name"] = cmu_df["Name of Applicant"].str.strip()
        cmu_df["Full Name"] = cmu_df.apply(
            lambda row: row["Full Name"] if row["Full Name"] else row["Parent Company"],
            axis=1
        )
        
        # Filter by company if specified
        if company_filter:
            self.stdout.write(f"Filtering for company: {company_filter}")
            cmu_df = cmu_df[
                cmu_df["Full Name"].str.contains(company_filter, case=False, na=False) |
                cmu_df["Parent Company"].str.contains(company_filter, case=False, na=False)
            ]
            if len(cmu_df) == 0:
                self.stdout.write(self.style.WARNING(f"No CMU IDs found for company: {company_filter}"))
                return
        
        all_cmu_ids = cmu_df["CMU ID"].unique().tolist()
        self.stdout.write(f"Found {len(all_cmu_ids)} unique CMU IDs")
        
        # Process CMU IDs
        processed = 0
        skipped = 0
        error_count = 0
        
        for i, cmu_id in enumerate(all_cmu_ids):
            # Check if we've exceeded our time limit
            current_time = time.time()
            if current_time - start_time > max_time:
                self.stdout.write(self.style.WARNING(f"Reached timeout limit of {max_time}s"))
                break
            
            # Skip if already processed and skip_existing is True
            if skip_existing and get_component_data_from_json(cmu_id) is not None:
                skipped += 1
                if skipped % 10 == 0:
                    self.stdout.write(f"Skipped {skipped} CMU IDs that already have data")
                continue
            
            # Fetch components
            try:
                self.stdout.write(f"Processing CMU ID: {cmu_id} ({i+1}/{len(all_cmu_ids)})")
                components, _ = fetch_components_for_cmu_id(cmu_id)
                processed += 1
                
                if len(components) > 0:
                    self.stdout.write(self.style.SUCCESS(f"  Found {len(components)} components"))
                else:
                    self.stdout.write(self.style.WARNING(f"  No components found"))
                
                # Sleep a bit to avoid overwhelming the API
                time.sleep(0.5)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error processing {cmu_id}: {e}"))
                error_count += 1
                
                # Sleep a bit longer after an error
                time.sleep(1)
        
        elapsed = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(
            f"Crawler completed in {elapsed:.2f}s. Processed {processed} CMU IDs, "
            f"skipped {skipped} existing, encountered {error_count} errors."
        ))