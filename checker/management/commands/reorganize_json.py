# Put this in a file called checker/management/commands/reorganize_json.py

from django.core.management.base import BaseCommand
import os
import json
from django.conf import settings
from django.core.cache import cache


class Command(BaseCommand):
    help = 'Reorganize component data from single JSON to split files'

    def handle(self, *args, **options):
        old_json_path = os.path.join(settings.BASE_DIR, 'component_data.json')
        if not os.path.exists(old_json_path):
            self.stdout.write(self.style.ERROR(f'Original JSON file not found: {old_json_path}'))
            return

        self.stdout.write('Loading original component data...')
        with open(old_json_path, 'r') as f:
            all_components = json.load(f)

        self.stdout.write(f'Found data for {len(all_components)} CMU IDs')

        # Create directory for split files
        json_dir = os.path.join(settings.BASE_DIR, 'json_data')
        os.makedirs(json_dir, exist_ok=True)

        # Get company mapping from cache if available
        cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
        self.stdout.write(f'Found {len(cmu_to_company_mapping)} CMU IDs in company mapping cache')

        # Group CMU IDs by prefix
        prefixes = {}
        for cmu_id, components in all_components.items():
            prefix = cmu_id[0].upper() if cmu_id else "0"
            if prefix not in prefixes:
                prefixes[prefix] = {}

            # Add company name to components if available
            company_name = cmu_to_company_mapping.get(cmu_id, "")
            if company_name:
                for component in components:
                    if "Company Name" not in component:
                        component["Company Name"] = company_name

            prefixes[prefix][cmu_id] = components

        # Save each prefix group to its own file
        for prefix, data in prefixes.items():
            json_path = os.path.join(json_dir, f'components_{prefix}.json')
            with open(json_path, 'w') as f:
                json.dump(data, f, indent=2)
            self.stdout.write(self.style.SUCCESS(f'Saved {len(data)} CMU IDs to {json_path}'))

        self.stdout.write(self.style.SUCCESS('Reorganization complete'))