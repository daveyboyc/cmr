import os
import json
import glob
import time
from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import transaction
from ...models import Component

class Command(BaseCommand):
    help = 'Migrate all component data from JSON files to the database'

    def add_arguments(self, parser):
        parser.add_argument('--batch-size', type=int, default=500, help='Number of components to commit in each transaction')
        parser.add_argument('--file', type=str, help='Specific JSON file to migrate (defaults to all)')
        parser.add_argument('--skip-existing', action='store_true', help='Skip components already in the database')
        parser.add_argument('--letter', type=str, help='Migrate only files starting with this letter (e.g., A)')
        parser.add_argument('--dry-run', action='store_true', help='Show what would be migrated without making changes')

    def handle(self, *args, **options):
        self.batch_size = options['batch_size']
        self.specific_file = options['file']
        self.skip_existing = options['skip_existing']
        self.letter_filter = options['letter'].upper() if options['letter'] else None
        self.dry_run = options['dry_run']
        
        self.stdout.write(self.style.SUCCESS(f"Starting JSON to database migration"))
        if self.dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN MODE - No changes will be made"))
        
        start_time = time.time()
        
        # Get list of JSON files to process
        json_dir = os.path.join(settings.BASE_DIR, 'json_data')
        if not os.path.exists(json_dir):
            self.stdout.write(self.style.ERROR(f"JSON directory not found: {json_dir}"))
            return
        
        if self.specific_file:
            if os.path.exists(os.path.join(json_dir, self.specific_file)):
                json_files = [os.path.join(json_dir, self.specific_file)]
            else:
                self.stdout.write(self.style.ERROR(f"Specified file not found: {self.specific_file}"))
                return
        else:
            if self.letter_filter:
                json_pattern = os.path.join(json_dir, f'components_{self.letter_filter}.json')
                self.stdout.write(f"Processing only files for letter: {self.letter_filter}")
            else:
                json_pattern = os.path.join(json_dir, 'components_*.json')
            json_files = glob.glob(json_pattern)
        
        self.stdout.write(f"Found {len(json_files)} JSON files to process")
        
        # Statistics
        stats = {
            'files_processed': 0,
            'cmu_ids_processed': 0,
            'components_found': 0,
            'components_added': 0,
            'components_skipped': 0,
            'errors': 0
        }
        
        # Process each file
        for json_file in json_files:
            file_stats = self.process_json_file(json_file)
            
            # Update overall stats
            stats['files_processed'] += 1
            stats['cmu_ids_processed'] += file_stats['cmu_ids_processed']
            stats['components_found'] += file_stats['components_found']
            stats['components_added'] += file_stats['components_added']
            stats['components_skipped'] += file_stats['components_skipped']
            stats['errors'] += file_stats['errors']
            
            # Show progress
            elapsed = time.time() - start_time
            progress = stats['files_processed'] / len(json_files) * 100
            self.stdout.write(f"Progress: {progress:.1f}% - {stats['files_processed']}/{len(json_files)} files")
            self.stdout.write(f"Components added so far: {stats['components_added']}")
            
            rate = stats['components_added'] / elapsed if elapsed > 0 else 0
            eta = (len(json_files) - stats['files_processed']) * (elapsed / stats['files_processed']) if stats['files_processed'] > 0 else 0
            
            # Format as minutes:seconds
            eta_minutes = int(eta // 60)
            eta_seconds = int(eta % 60)
            eta_str = f"{eta_minutes:02d}:{eta_seconds:02d}"
            
            self.stdout.write(f"Processing rate: {rate:.1f} components/second | ETA: {eta_str}")
        
        # Print summary
        total_time = time.time() - start_time
        total_minutes = total_time / 60
        
        summary = f"""
╔══════════════════════════════════════════════════╗
║              MIGRATION SUMMARY                   ║
╠══════════════════════════════════════════════════╣
║ Time elapsed:           {total_minutes:.1f} minutes             ║
║ Files processed:        {stats['files_processed']}                       ║
║ CMU IDs processed:      {stats['cmu_ids_processed']}                     ║
║ Components found:       {stats['components_found']}                   ║
║ Components added:       {stats['components_added']}                   ║
║ Components skipped:     {stats['components_skipped']}                   ║
║ Errors encountered:     {stats['errors']}                       ║
╚══════════════════════════════════════════════════╝
"""
        
        self.stdout.write(self.style.SUCCESS(summary))
        
        if self.dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN COMPLETE - No changes were made to the database"))
            self.stdout.write(self.style.WARNING("Run again without --dry-run to perform the actual migration"))
    
    def process_json_file(self, json_file):
        """Process a single JSON file."""
        filename = os.path.basename(json_file)
        self.stdout.write(f"Processing {filename}...")
        
        # Stats for this file
        file_stats = {
            'cmu_ids_processed': 0,
            'components_found': 0,
            'components_added': 0,
            'components_skipped': 0,
            'errors': 0
        }
        
        try:
            # Load JSON data
            with open(json_file, 'r') as f:
                all_components = json.load(f)
            
            self.stdout.write(f"  File contains {len(all_components)} CMU IDs")
            
            # Process in batches
            current_batch = []
            
            for cmu_id, components in all_components.items():
                file_stats['cmu_ids_processed'] += 1
                
                if not components:
                    continue
                
                file_stats['components_found'] += len(components)
                
                # Process each component
                for component in components:
                    # Get component ID
                    component_id = component.get("_id", "")
                    
                    # Skip if we would skip in the actual migration
                    if self.skip_existing and component_id and Component.objects.filter(component_id=component_id).exists():
                        file_stats['components_skipped'] += 1
                        continue
                    
                    # Extract standard fields
                    location = component.get("Location and Post Code", "")
                    description = component.get("Description of CMU Components", "")
                    technology = component.get("Generating Technology Class", "")
                    company_name = component.get("Company Name", "")
                    auction_name = component.get("Auction Name", "")
                    delivery_year = component.get("Delivery Year", "")
                    status = component.get("Status", "")
                    type_value = component.get("Type", "")
                    
                    # Create the model instance but don't save it yet
                    if not self.dry_run:
                        component_obj = Component(
                            component_id=component_id,
                            cmu_id=cmu_id,
                            location=location,
                            description=description,
                            technology=technology,
                            company_name=company_name,
                            auction_name=auction_name,
                            delivery_year=delivery_year,
                            status=status,
                            type=type_value,
                            additional_data=component
                        )
                        current_batch.append(component_obj)
                    
                    file_stats['components_added'] += 1
                    
                    # If batch is full, save to database
                    if len(current_batch) >= self.batch_size:
                        if not self.dry_run:
                            with transaction.atomic():
                                Component.objects.bulk_create(
                                    current_batch, 
                                    ignore_conflicts=self.skip_existing
                                )
                            self.stdout.write(f"  Saved batch of {len(current_batch)} components")
                        current_batch = []
            
            # Save any remaining items in the batch
            if current_batch and not self.dry_run:
                with transaction.atomic():
                    Component.objects.bulk_create(
                        current_batch, 
                        ignore_conflicts=self.skip_existing
                    )
                self.stdout.write(f"  Saved final batch of {len(current_batch)} components")
            
            self.stdout.write(self.style.SUCCESS(
                f"  Processed {file_stats['cmu_ids_processed']} CMU IDs with "
                f"{file_stats['components_found']} components "
                f"(added: {file_stats['components_added']}, skipped: {file_stats['components_skipped']})"
            ))
            
        except Exception as e:
            import traceback
            self.stdout.write(self.style.ERROR(f"Error processing {filename}: {str(e)}"))
            self.stdout.write(traceback.format_exc())
            file_stats['errors'] += 1
        
        return file_stats