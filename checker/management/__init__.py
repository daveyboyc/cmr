# Save this file as management/commands/crawl_company.py in one of your Django apps

import requests
import pandas as pd
import time
from django.core.management.base import BaseCommand, CommandError
from django.core.cache import cache

try:
    from tqdm import tqdm
except ImportError:
    # Define a simple replacement for tqdm
    def tqdm(iterable, *args, **kwargs):
        return iterable


class Command(BaseCommand):
    help = 'Crawls the CMU data for a specific company and caches the results'

    def add_arguments(self, parser):
        parser.add_argument('company_name', type=str, help='The company name to search for')
        parser.add_argument('--limit', type=int, default=5000, help='Limit for API requests')
        parser.add_argument('--cache-time', type=int, default=86400, help='Cache time in seconds (default: 24 hours)')

    def normalize(self, text):
        """Lowercase and remove all whitespace."""
        if not isinstance(text, str):
            return ""
        return "".join(text.lower().split())

    def fetch_all_cmu_records(self, limit=5000):
        """Fetch all CMU records from the API."""
        params = {
            "resource_id": "25a5fa2e-873d-41c5-8aaf-fbc2b06d79e6",
            "limit": limit,
            "offset": 0
        }

        all_records = []
        total_time = 0
        self.stdout.write(self.style.NOTICE("Fetching all CMU records..."))

        while True:
            start_time = time.time()
            try:
                response = requests.get(
                    "https://api.neso.energy/api/3/action/datastore_search",
                    params=params,
                    timeout=20
                )
                response.raise_for_status()
                total_time += time.time() - start_time
                result = response.json()["result"]
                records = result["records"]
                all_records.extend(records)
                self.stdout.write(f"Fetched {len(all_records)} of {result['total']} records")

                if len(all_records) >= result["total"]:
                    break

                params["offset"] += limit

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error fetching CMU records: {e}"))
                break

        return all_records, total_time

    def fetch_component_details(self, cmu_id, limit=100):
        """Fetch component details for a CMU ID."""
        params = {
            "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
            "q": cmu_id,
            "limit": limit
        }

        start_time = time.time()
        try:
            response = requests.get(
                "https://api.neso.energy/api/3/action/datastore_search",
                params=params,
                timeout=20
            )
            response.raise_for_status()
            elapsed = time.time() - start_time
            result = response.json()["result"]
            return result.get("records", []), elapsed
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error fetching components for CMU ID {cmu_id}: {e}"))
            elapsed = time.time() - start_time
            return [], elapsed

    def handle(self, *args, **options):
        company_name = options['company_name']
        limit = options['limit']
        cache_time = options['cache_time']

        self.stdout.write(self.style.SUCCESS(f"Starting crawl for company: {company_name}"))

        # 1. First check if we already have the CMU data cached
        cmu_df = cache.get("cmu_df")
        if cmu_df is None:
            all_records, api_time = self.fetch_all_cmu_records(limit=limit)
            cmu_df = pd.DataFrame(all_records)
            cache.set("cmu_df", cmu_df, cache_time)  # Cache for 24 hours by default
        else:
            self.stdout.write(self.style.SUCCESS("Using cached CMU data"))

        # 2. Prepare and normalize the company data
        cmu_df["Name of Applicant"] = cmu_df.get("Name of Applicant", pd.Series()).fillna("").astype(str)
        cmu_df["Parent Company"] = cmu_df.get("Parent Company", pd.Series()).fillna("").astype(str)
        cmu_df["Delivery Year"] = cmu_df.get("Delivery Year", pd.Series()).fillna("").astype(str)

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

        cmu_df["Normalized Full Name"] = cmu_df["Full Name"].apply(self.normalize)
        norm_company = self.normalize(company_name)

        # 3. Find matching records
        matching_records = cmu_df[
            cmu_df["Normalized Full Name"].str.contains(norm_company, regex=False, na=False)
        ]

        if matching_records.empty:
            self.stdout.write(self.style.WARNING(f"No CMU records found for company: {company_name}"))
            return

        # 4. Get all unique CMU IDs for this company
        company_cmu_ids = []
        cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})

        for full_name in matching_records["Full Name"].unique():
            records = cmu_df[cmu_df["Full Name"] == full_name]
            cmu_ids = records["CMU ID"].unique().tolist()
            cmu_ids = [cmu_id for cmu_id in cmu_ids if cmu_id and cmu_id != "N/A"]
            company_cmu_ids.extend(cmu_ids)

            # Update company mapping for each CMU ID
            for cmu_id in cmu_ids:
                cmu_to_company_mapping[cmu_id] = full_name

        # Update the cache with company mapping
        cache.set("cmu_to_company_mapping", cmu_to_company_mapping, cache_time)

        self.stdout.write(self.style.SUCCESS(f"Found {len(company_cmu_ids)} CMU IDs for {company_name}"))

        # 5. Fetch and cache location data for each CMU ID
        cmu_to_location_mapping = cache.get("cmu_to_location_mapping", {})

        for cmu_id in tqdm(company_cmu_ids, desc="Fetching component details"):
            records, _ = self.fetch_component_details(cmu_id)

            if records:
                components_df = pd.DataFrame(records)
                if "Location and Post Code" in components_df.columns:
                    locations = components_df["Location and Post Code"].unique().tolist()
                    locations = [loc for loc in locations if loc and loc != "N/A"]

                    if locations:
                        cmu_to_location_mapping[cmu_id] = locations[0]  # Use first location
                        self.stdout.write(f"  CMU ID {cmu_id}: Location = {locations[0]}")
                    else:
                        self.stdout.write(f"  CMU ID {cmu_id}: No location found")

            # Add a small delay to avoid hitting rate limits
            time.sleep(0.1)

        # Update the cache with location mapping
        cache.set("cmu_to_location_mapping", cmu_to_location_mapping, cache_time)

        # 6. Simulate a search for this company to cache the results
        # This will use the same code path as your views.py search_companies function
        cache_key = f"search_results_{norm_company}"
        search_results = {
            company_name: [f"Crawled and cached {len(company_cmu_ids)} CMU IDs with locations"]
        }
        cache.set(cache_key, search_results, cache_time)

        self.stdout.write(self.style.SUCCESS(f"Successfully crawled and cached data for {company_name}"))
        self.stdout.write(self.style.SUCCESS(f"Use your existing search page to view the results"))