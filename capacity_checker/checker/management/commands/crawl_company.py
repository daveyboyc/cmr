from django.core.management.base import BaseCommand
from checker.views import fetch_all_cmu_records
import pandas as pd
from django.core.cache import cache
import time

class Command(BaseCommand):
    help = 'Crawl CMU IDs for a specific company'

    def add_arguments(self, parser):
        parser.add_argument('company', type=str, help='Company name to search for')

    def handle(self, *args, **options):
        company_name = options['company']
        self.stdout.write(f"Crawling for '{company_name}'")
        
        # Crawl function
        try:
            # Fetch all CMU records
            self.stdout.write("Fetching all CMU records...")
            all_records, api_time = fetch_all_cmu_records(limit=5000)
            self.stdout.write(f"Fetched {len(all_records)} records in {api_time:.2f} seconds")
            
            cmu_df = pd.DataFrame(all_records)
            
            # Process the dataframe
            cmu_df["Name of Applicant"] = cmu_df.get("Name of Applicant", pd.Series()).fillna("").astype(str)
            cmu_df["Parent Company"] = cmu_df.get("Parent Company", pd.Series()).fillna("").astype(str)
            
            possible_cmu_id_fields = ["CMU ID", "cmu_id", "CMU_ID", "cmuId", "id", "identifier", "ID"]
            cmu_id_field = next((field for field in possible_cmu_id_fields if field in cmu_df.columns), None)
            if cmu_id_field:
                cmu_df["CMU ID"] = cmu_df[cmu_id_field].fillna("N/A").astype(str)
            else:
                cmu_df["CMU ID"] = "N/A"
            
            cmu_df["Full Name"] = cmu_df["Name of Applicant"].str.strip()
            cmu_df["Full Name"] = cmu_df.apply(
                lambda row: row["Full Name"] if row["Full Name"] else row["Parent Company"],
                axis=1
            )
            
            # Filter for the specific company
            self.stdout.write(f"Filtering for '{company_name}'...")
            company_records = cmu_df[
                (cmu_df["Full Name"].str.contains(company_name, case=False, na=False)) |
                (cmu_df["Parent Company"].str.contains(company_name, case=False, na=False))
            ]
            
            # Build and cache the mapping
            cmu_to_company_mapping = {}
            for _, row in company_records.iterrows():
                cmu_id = row.get("CMU ID", "").strip()
                if cmu_id and cmu_id != "N/A":
                    cmu_to_company_mapping[cmu_id] = row.get("Full Name", "")
            
            # Update the existing cache or create a new one
            existing_mapping = cache.get("cmu_to_company_mapping", {})
            existing_mapping.update(cmu_to_company_mapping)
            cache.set("cmu_to_company_mapping", existing_mapping, 86400)  # Cache for 24 hours
            
            self.stdout.write(self.style.SUCCESS(f"Found {len(cmu_to_company_mapping)} CMU IDs for '{company_name}'"))
            
            # Print out the found CMU IDs for reference
            if cmu_to_company_mapping:
                self.stdout.write("CMU IDs found:")
                for cmu_id, name in cmu_to_company_mapping.items():
                    self.stdout.write(f"  - {cmu_id}: {name}")
            else:
                self.stdout.write(self.style.WARNING(f"No CMU IDs found for '{company_name}'"))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error during crawl: {e}"))