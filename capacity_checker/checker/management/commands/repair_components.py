import os
import json
import glob
import logging
from django.core.management.base import BaseCommand
from django.core.cache import cache
from django.conf import settings
from ...services.data_access import get_component_data_from_json, save_component_data_to_json, get_json_path


class Command(BaseCommand):
    help = 'Finds and fixes missing components by scanning all JSON files'

    def add_arguments(self, parser):
        parser.add_argument(
            '--company',
            type=str,
            help='Specific company name to repair components for (optional)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            default=False,
            help='Show what would be done without making changes',
        )

    def handle(self, *args, **options):
        company_name = options.get('company')
        dry_run = options.get('dry_run')
        
        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No changes will be made'))

        # Get existing mapping from cache
        cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
        self.stdout.write(f'Loaded {len(cmu_to_company_mapping)} CMU to company mappings from cache')

        # Get all CMU IDs for the specified company name (if provided)
        company_cmu_ids = []
        if company_name:
            # Get CMU dataframe
            from ...services.company_search import get_cmu_dataframe
            cmu_df, _ = get_cmu_dataframe()
            
            if cmu_df is None:
                self.stdout.write(self.style.ERROR(f'Error loading CMU data'))
                return
                
            # Find all records for this company
            company_records = cmu_df[cmu_df["Full Name"] == company_name]
            
            if company_records.empty:
                self.stdout.write(self.style.ERROR(f'No CMU records found for company: {company_name}'))
                return
                
            # Get all CMU IDs for this company
            company_cmu_ids = company_records["CMU ID"].unique().tolist()
            self.stdout.write(f'Found {len(company_cmu_ids)} CMU IDs for company: {company_name}')
        
        # Scan all JSON files for components
        json_dir = os.path.join(settings.BASE_DIR, 'json_data')
        if not os.path.exists(json_dir):
            self.stdout.write(self.style.ERROR(f'JSON directory not found: {json_dir}'))
            return
            
        json_files = glob.glob(os.path.join(json_dir, 'components_*.json'))
        self.stdout.write(f'Found {len(json_files)} JSON files to scan')

        # Counters for reporting
        total_cmu_ids = 0
        total_components = 0
        fixed_cmu_ids = 0
        fixed_components = 0
        
        # Track all CMU IDs that have been processed
        processed_cmu_ids = set()
        
        # Process each JSON file
        for json_file in sorted(json_files):
            try:
                self.stdout.write(f'Processing {os.path.basename(json_file)}...')
                
                with open(json_file, 'r') as f:
                    all_components = json.load(f)
                
                file_cmu_ids = len(all_components)
                file_components = sum(len(components) for components in all_components.values())
                self.stdout.write(f'  Contains {file_cmu_ids} CMU IDs with {file_components} total components')
                
                total_cmu_ids += file_cmu_ids
                total_components += file_components
                
                # Track if we need to update this file
                file_updated = False
                
                # Process each CMU ID in the file
                for cmu_id, components in list(all_components.items()):
                    # Add to processed set
                    processed_cmu_ids.add(cmu_id)
                    
                    # Skip if not for our target company (if specified)
                    if company_name:
                        if cmu_id not in company_cmu_ids:
                            continue
                
                    # Skip if no components
                    if not components:
                        self.stdout.write(f'  {cmu_id}: Empty component list')
                        continue
                        
                    # Check if components have Company Name field
                    missing_company_name = False
                    company_name_value = None
                    
                    # First check if any component has Company Name
                    for component in components:
                        if isinstance(component, dict) and component.get("Company Name"):
                            company_name_value = component["Company Name"]
                            break
                            
                    # If we found a company name, make sure all components have it
                    if company_name_value:
                        updates_needed = 0
                        for component in components:
                            if isinstance(component, dict) and not component.get("Company Name"):
                                updates_needed += 1
                                if not dry_run:
                                    component["Company Name"] = company_name_value
                        
                        if updates_needed > 0:
                            self.stdout.write(f'  {cmu_id}: Adding Company Name "{company_name_value}" to {updates_needed} components')
                            fixed_components += updates_needed
                            file_updated = True
                    else:
                        # Try to get company name from mapping
                        company_name_value = cmu_to_company_mapping.get(cmu_id)
                        
                        # Try case-insensitive match if needed
                        if not company_name_value:
                            for mapping_cmu_id, mapping_company in cmu_to_company_mapping.items():
                                if mapping_cmu_id.lower() == cmu_id.lower():
                                    company_name_value = mapping_company
                                    break
                                    
                        if company_name_value:
                            self.stdout.write(f'  {cmu_id}: Adding Company Name "{company_name_value}" to all {len(components)} components from mapping')
                            if not dry_run:
                                for component in components:
                                    if isinstance(component, dict):
                                        component["Company Name"] = company_name_value
                            fixed_components += len(components)
                            file_updated = True
                        else:
                            self.stdout.write(self.style.WARNING(f'  {cmu_id}: No Company Name found in components or mapping'))
                
                # Save the file if it was updated
                if file_updated and not dry_run:
                    self.stdout.write(f'  Saving updated components to {json_file}')
                    with open(json_file, 'w') as f:
                        json.dump(all_components, f, indent=2)
                    fixed_cmu_ids += 1
                
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Error processing {json_file}: {e}'))
                
        # Now check if any CMU IDs for the target company are missing
        if company_name and company_cmu_ids:
            missing_cmu_ids = set(company_cmu_ids) - processed_cmu_ids
            if missing_cmu_ids:
                self.stdout.write(self.style.WARNING(f'Found {len(missing_cmu_ids)} missing CMU IDs for {company_name}'))
                
                for cmu_id in missing_cmu_ids:
                    # Create empty component array
                    company_name_value = cmu_to_company_mapping.get(cmu_id)
                    if not company_name_value:
                        for mapping_cmu_id, mapping_company in cmu_to_company_mapping.items():
                            if mapping_cmu_id.lower() == cmu_id.lower():
                                company_name_value = mapping_company
                                break
                    
                    if company_name_value:
                        self.stdout.write(f'  Creating empty component for {cmu_id} with Company Name "{company_name_value}"')
                        if not dry_run:
                            # Create a placeholder component
                            placeholder_component = {
                                "CMU ID": cmu_id,
                                "Company Name": company_name_value,
                                "Description of CMU Components": "Placeholder component",
                                "Location and Post Code": "Unknown",
                                "_id": -1  # Placeholder ID
                            }
                            save_component_data_to_json(cmu_id, [placeholder_component])
                            fixed_cmu_ids += 1
                    else:
                        self.stdout.write(self.style.ERROR(f'  Cannot create component for {cmu_id} - no company name in mapping'))
        
        # Summary
        self.stdout.write(self.style.SUCCESS(
            f'Scan completed: Found {total_cmu_ids} CMU IDs with {total_components} components'
        ))
        
        if dry_run:
            self.stdout.write(self.style.WARNING(
                f'Would fix {fixed_components} components across {fixed_cmu_ids} CMU IDs'
            ))
        else:
            self.stdout.write(self.style.SUCCESS(
                f'Fixed {fixed_components} components across {fixed_cmu_ids} CMU IDs'
            )) 