import os
import glob
import shutil
import time
from django.core.management.base import BaseCommand
from django.conf import settings
from ...models import Component

class Command(BaseCommand):
    help = 'Clean up JSON files after migration to database'

    def add_arguments(self, parser):
        parser.add_argument('--archive', action='store_true', help='Archive JSON files instead of deleting them')
        parser.add_argument('--verify', action='store_true', 
                            help='Verify components are in database before removing (slower)')
        parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
        parser.add_argument('--letters', type=str, help='Specific letters to clean (e.g., "ABC")')

    def handle(self, *args, **options):
        archive_mode = options['archive']
        verify_mode = options['verify']
        dry_run = options['dry_run']
        specific_letters = options['letters'].upper() if options['letters'] else None
        
        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN MODE - No files will be modified"))
        
        # Safety check: Make sure there are components in the database
        db_count = Component.objects.count()
        if db_count == 0:
            self.stdout.write(self.style.ERROR(
                "ABORT: The database appears to be empty! No components found."
            ))
            self.stdout.write(self.style.ERROR(
                "You must migrate data to the database before cleaning up JSON files."
            ))
            self.stdout.write(self.style.ERROR(
                "Run 'python manage.py migrate_json_to_db' first."
            ))
            return
        
        self.stdout.write(f"Database contains {db_count:,} components")
        
        # Get JSON directory
        json_dir = os.path.join(settings.BASE_DIR, 'json_data')
        if not os.path.exists(json_dir):
            self.stdout.write(self.style.ERROR(f"JSON directory not found: {json_dir}"))
            return
        
        # Create archive directory if needed
        archive_dir = None
        if archive_mode:
            archive_dir = os.path.join(settings.BASE_DIR, 'json_archive')
            archive_dir = f"{archive_dir}_{time.strftime('%Y%m%d_%H%M%S')}"
            if not dry_run:
                os.makedirs(archive_dir, exist_ok=True)
            self.stdout.write(f"Archive directory: {archive_dir}")
        
        # Get all JSON files
        if specific_letters:
            json_files = []
            for letter in specific_letters:
                pattern = os.path.join(json_dir, f'components_{letter}.json')
                json_files.extend(glob.glob(pattern))
        else:
            json_pattern = os.path.join(json_dir, 'components_*.json')
            json_files = glob.glob(json_pattern)
        
        self.stdout.write(f"Found {len(json_files)} JSON files to process")
        
        # Process files
        stats = {
            'files_processed': 0,
            'files_archived': 0,
            'files_deleted': 0,
            'total_size_cleaned': 0,
            'cmu_ids_checked': 0,
            'missing_cmu_ids': []
        }
        
        for json_file in json_files:
            file_size = os.path.getsize(json_file)
            filename = os.path.basename(json_file)
            
            # Safety verification (if requested)
            missing_cmu_ids = []
            if verify_mode:
                import json
                try:
                    with open(json_file, 'r') as f:
                        file_data = json.load(f)
                    
                    self.stdout.write(f"Verifying {len(file_data)} CMU IDs in {filename}...")
                    
                    # Check each CMU ID to make sure its components are in the database
                    for cmu_id in file_data.keys():
                        stats['cmu_ids_checked'] += 1
                        
                        # Check if this CMU ID has components in the database
                        if not Component.objects.filter(cmu_id=cmu_id).exists():
                            missing_cmu_ids.append(cmu_id)
                    
                    if missing_cmu_ids:
                        self.stdout.write(self.style.WARNING(
                            f"SKIPPING {filename} - Found {len(missing_cmu_ids)} CMU IDs "
                            f"that are not in the database"
                        ))
                        stats['missing_cmu_ids'].extend(missing_cmu_ids)
                        continue
                        
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error verifying {filename}: {e}"))
                    continue
            
            stats['files_processed'] += 1
            
            # Archive or delete
            if archive_mode:
                if not dry_run:
                    shutil.copy2(json_file, os.path.join(archive_dir, filename))
                    os.remove(json_file)
                stats['files_archived'] += 1
                self.stdout.write(f"Archived: {filename} ({file_size/1024/1024:.2f} MB)")
            else:
                if not dry_run:
                    os.remove(json_file)
                stats['files_deleted'] += 1
                self.stdout.write(f"Deleted: {filename} ({file_size/1024/1024:.2f} MB)")
            
            stats['total_size_cleaned'] += file_size
        
        # Format total size in appropriate units
        if stats['total_size_cleaned'] > 1024 * 1024 * 1024:
            size_str = f"{stats['total_size_cleaned'] / (1024*1024*1024):.2f} GB"
        else:
            size_str = f"{stats['total_size_cleaned'] / (1024*1024):.2f} MB"
        
        # Print summary
        summary = f"""
╔══════════════════════════════════════════════════╗
║                  CLEANUP SUMMARY                 ║
╠══════════════════════════════════════════════════╣
║ Files processed:       {stats['files_processed']:<7}                   ║
║ Files {"archived" if archive_mode else "deleted"}:       {stats['files_archived' if archive_mode else 'files_deleted']:<7}                   ║
║ Total size cleaned:    {size_str:<10}              ║
"""

        if verify_mode:
            summary += f"║ CMU IDs checked:       {stats['cmu_ids_checked']:<7}                   ║\n"
            summary += f"║ Missing CMU IDs:       {len(stats['missing_cmu_ids']):<7}                   ║\n"
        
        summary += "╚══════════════════════════════════════════════════╝"
        
        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN COMPLETE - No files were modified"))
            self.stdout.write(self.style.WARNING("Run again without --dry-run to perform actual cleanup"))
        
        self.stdout.write(self.style.SUCCESS(summary))
        
        # If there were missing CMU IDs, show a sample
        if verify_mode and stats['missing_cmu_ids']:
            self.stdout.write(self.style.WARNING(
                f"Warning: {len(stats['missing_cmu_ids'])} CMU IDs in JSON files are not in the database"
            ))
            self.stdout.write("Sample of missing CMU IDs:")
            for cmu_id in stats['missing_cmu_ids'][:10]:
                self.stdout.write(f"  - {cmu_id}")
            if len(stats['missing_cmu_ids']) > 10:
                self.stdout.write(f"  ... and {len(stats['missing_cmu_ids']) - 10} more")