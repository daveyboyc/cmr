from django.core.management.base import BaseCommand
import requests
from django.core.cache import cache

class Command(BaseCommand):
    help = "Cache components for a CMU ID"

    def add_arguments(self, parser):
        parser.add_argument("cmu_id", type=str, help="CMU ID to cache components for")

    def handle(self, *args, **options):
        cmu_id = options["cmu_id"]
        self.stdout.write(f"Caching components for CMU ID: {cmu_id}")
        
        try:
            # Fetch components
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
            components = result.get("records", [])
            
            if not components:
                self.stdout.write(self.style.WARNING(f"No components found for CMU ID: {cmu_id}"))
                return
            
            self.stdout.write(self.style.SUCCESS(f"Found {len(components)} components"))
            
            # Cache the components
            cache_key = f"components_{cmu_id}"
            cache.set(cache_key, components, 86400)  # Cache for 24 hours
            
            self.stdout.write(self.style.SUCCESS(f"Cached components with key: {cache_key}"))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error caching components: {str(e)}")) 