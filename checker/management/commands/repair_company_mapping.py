# Create this file at: checker/management/commands/repair_company_mapping.py

from django.core.management.base import BaseCommand
import os
import json
from django.conf import settings
from django.core.cache import cache
import glob


class Command(BaseCommand):
    help = 'Repairs and rebuilds the CMU ID to company name mapping'

    def add_arguments(self, parser):
        parser.add_argument('--clear', action='store_true', help='Clear existing mapping before rebuilding')

    def handle(self, *args, **options):
        # Get existing mapping from cache
        cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
        self.stdout.write(f'Found {len(cmu_to_company_mapping)} CMU IDs in existing company mapping cache')

        if options['clear']:
            self.stdout.write('Clearing existing mapping')
            cmu_to_company_mapping = {}

        # Get company names from CMU data JSON
        cmu_json_path = os.path.join(settings.BASE_DIR, 'cmu_data.json')
        if os.path.exists(cmu_json_path):
            self.stdout.write('Processing CMU data JSON...')
            try:
                with open(cmu_json_path, 'r') as f:
                    cmu_records = json.load(f)

                for record in cmu_records:
                    cmu_id = None
                    company_name = None

                    # Try to find CMU ID
                    for field in ['CMU ID', 'cmu_id', 'CMU_ID', 'cmuId']:
                        if field in record and record[field]:
                            cmu_id = record[field]
                            break

                    # Try to find company name
                    for field in ['Name of Applicant', 'Parent Company', 'Company', 'Full Name']:
                        if field in record and record[field] and str(record[field]).strip():
                            company_name = str(record[field]).strip()
                            break

                    if cmu_id and company_name:
                        cmu_to_company_mapping[cmu_id] = company_name

                self.stdout.write(f'Added company names from CMU data, now have {len(cmu_to_company_mapping)} mappings')

            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Error processing CMU data: {e}'))

        # Process all component JSON files to get company names
        json_dir = os.path.join(settings.BASE_DIR, 'json_data')
        if os.path.exists(json_dir):
            self.stdout.write('Processing component JSON files...')
            json_files = glob.glob(os.path.join(json_dir, 'components_*.json'))

            for json_file in json_files:
                try:
                    with open(json_file, 'r') as f:
                        all_components = json.load(f)

                    self.stdout.write(f'Processing {json_file} with {len(all_components)} CMU entries')

                    for cmu_id, components in all_components.items():
                        if not isinstance(components, list) or not components:
                            continue

                        # Check if any component has Company Name field
                        company_name = None
                        for comp in components:
                            if isinstance(comp, dict) and "Company Name" in comp and comp["Company Name"]:
                                company_name = comp["Company Name"]
                                break

                        if company_name and cmu_id:
                            self.stdout.write(f'  Found company {company_name} for CMU ID {cmu_id}')
                            cmu_to_company_mapping[cmu_id] = company_name
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'Error processing {json_file}: {e}'))

        # Store the updated mapping back to cache
        cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 24 * 3600)  # Cache for 24 hours
        self.stdout.write(self.style.SUCCESS(f'Saved {len(cmu_to_company_mapping)} CMU to company mappings to cache'))

        # Display sample entries
        sample_entries = list(cmu_to_company_mapping.items())[:10]
        for cmu_id, company in sample_entries:
            self.stdout.write(f'  {cmu_id} -> {company}')