from django.core.management.base import BaseCommand
import requests
import json
from django.core.cache import cache
from checker.views import cache_company_data

class Command(BaseCommand):
    help = "Cache data for a company"

    def add_arguments(self, parser):
        parser.add_argument("company", type=str, help="Company name to cache data for")

    def handle(self, *args, **options):
        company = options["company"]
        self.stdout.write(f"Caching data for company: {company}")
        
        # Step 1: Fetch CMU records
        self.stdout.write("Fetching CMU records...")
        cmu_records = self.fetch_cmu_records(company)
        
        if not cmu_records:
            self.stdout.write(self.style.WARNING(f"No CMU records found for company: {company}"))
            return
        
        self.stdout.write(self.style.SUCCESS(f"Found {len(cmu_records)} CMU records"))
        
        # Step 2: Extract CMU IDs
        cmu_ids = []
        for record in cmu_records:
            cmu_id = record.get("CMU ID")
            if cmu_id and cmu_id not in cmu_ids:
                cmu_ids.append(cmu_id)
        
        if not cmu_ids:
            self.stdout.write(self.style.WARNING("No CMU IDs found in the records"))
            return
        
        self.stdout.write(f"Found CMU IDs: {', '.join(cmu_ids)}")
        
        # Step 3: Fetch components for each CMU ID
        components_by_cmu = {}
        for cmu_id in cmu_ids:
            self.stdout.write(f"Fetching components for CMU ID: {cmu_id}")
            components = self.fetch_components(cmu_id)
            components_by_cmu[cmu_id] = components
            self.stdout.write(f"Found {len(components)} components")
        
        # Step 4: Cache the data
        cache_result = cache_company_data(company, cmu_records, components_by_cmu)
        if cache_result:
            self.stdout.write(self.style.SUCCESS("Data cached successfully"))
        else:
            self.stdout.write(self.style.ERROR("Failed to cache data"))
    
    def fetch_cmu_records(self, company):
        """Fetch CMU records for a company"""
        try:
            params = {
                "resource_id": "25a5fa2e-873d-41c5-8aaf-fbc2b06d79e6",
                "q": company,
                "limit": 1000
            }
            
            response = requests.get(
                "https://data.nationalgrideso.com/api/3/action/datastore_search",
                params=params,
                timeout=20
            )
            response.raise_for_status()
            result = response.json().get("result", {})
            return result.get("records", [])
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error fetching CMU records: {str(e)}"))
            return []
    
    def fetch_components(self, cmu_id):
        """Fetch components for a CMU ID"""
        try:
            params = {
                "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
                "q": cmu_id,
                "limit": 1000
            }
            
            response = requests.get(
                "https://data.nationalgrideso.com/api/3/action/datastore_search",
                params=params,
                timeout=20
            )
            response.raise_for_status()
            result = response.json().get("result", {})
            return result.get("records", [])
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error fetching components: {str(e)}"))
            return [] 