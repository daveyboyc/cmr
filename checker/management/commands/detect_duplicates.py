import os
import json
import glob
import hashlib
from collections import defaultdict
from django.core.management.base import BaseCommand
from django.conf import settings

class Command(BaseCommand):
    help = 'Detect and optionally clean duplicate component records'

    def add_arguments(self, parser):
        parser.add_argument('--clean', action='store_true', help='Remove duplicates from JSON files')
        parser.add_argument('--match-level', type=str, default='standard', 
                            choices=['exact', 'standard', 'relaxed'], 
                            help='How strict to be when matching duplicates')
        parser.add_argument('--dry-run', action='store_true', help='Show what would be removed without making changes')
        parser.add_argument('--file', type=str, help='Specific JSON file to check')
        parser.add_argument('--cmu', type=str, help='Specific CMU ID to check')

    def handle(self, *args, **options):
        # Path to the JSON data directory
        json_dir = os.path.join(settings.BASE_DIR, 'json_data')
        
        # Get JSON files to examine
        if options['file']:
            json_files = [os.path.join(json_dir, options['file'])]
            if not os.path.exists(json_files[0]):
                self.stdout.write(self.style.ERROR(f"File not found: {json_files[0]}"))
                return
        else:
            json_pattern = os.path.join(json_dir, 'components_*.json')
            json_files = glob.glob(json_pattern)
        
        self.stdout.write(f"Examining {len(json_files)} JSON files for duplicates")
        
        # Define key fields for different match levels
        if options['match_level'] == 'exact':
            # For exact matching - use all fields
            key_fields = None  # None means use all fields
        elif options['match_level'] == 'standard':
            # Standard matching - use important identifying fields
            key_fields = ['CMU ID', 'Location and Post Code', 'Description of CMU Components', 
                         'Generating Technology Class', 'Delivery Year', 'Status']
        else:  # relaxed
            # Relaxed matching - only use essential fields
            key_fields = ['CMU ID', 'Location and Post Code']
        
        # Track statistics
        total_components = 0
        unique_components = 0
        duplicate_sets = []
        duplicates_found = 0
        
        # Dict to track components by their hash
        component_hashes = defaultdict(list)
        
        # Process specific CMU if requested
        target_cmu = options['cmu']
        
        # First pass: identify duplicates across all files
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    file_data = json.load(f)
                
                # Process each CMU ID
                for cmu_id, components in file_data.items():
                    # Skip if not the target CMU (if specified)
                    if target_cmu and cmu_id != target_cmu:
                        continue
                        
                    for component in components:
                        total_components += 1
                        
                        # Generate a hash based on key fields
                        component_hash = self._hash_component(component, key_fields)
                        
                        # Track component by hash
                        component_entry = {
                            'cmu_id': cmu_id,
                            'file': os.path.basename(json_file),
                            'component': component,
                            'hash': component_hash
                        }
                        component_hashes[component_hash].append(component_entry)
            
            except Exception as e:
                self.stderr.write(f"Error processing {json_file}: {str(e)}")
        
        # Identify duplicate sets
        for component_hash, entries in component_hashes.items():
            if len(entries) > 1:
                duplicate_sets.append(entries)
                duplicates_found += len(entries) - 1
            else:
                unique_components += 1
        
        # Output results
        self.stdout.write(self.style.SUCCESS(f"Found {total_components} total components"))
        self.stdout.write(self.style.SUCCESS(f"Found {unique_components} unique components"))
        self.stdout.write(self.style.SUCCESS(f"Found {duplicates_found} duplicate components in {len(duplicate_sets)} sets"))
        
        # Display some example duplicates
        if duplicate_sets:
            self.stdout.write("\nExample duplicate sets:")
            for i, dup_set in enumerate(duplicate_sets[:5]):  # Show first 5 sets
                self.stdout.write(f"\nDuplicate Set #{i+1} ({len(dup_set)} identical components):")
                
                # Show where each duplicate is found
                for j, entry in enumerate(dup_set):
                    self.stdout.write(f"  #{j+1}: CMU ID: {entry['cmu_id']} in {entry['file']}")
                
                # Show the fields of the first component
                self.stdout.write("\n  Fields:")
                component = dup_set[0]['component']
                for field in sorted(component.keys()):
                    value = component[field]
                    # Truncate long values
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:50] + "..."
                    self.stdout.write(f"    {field}: {value}")
        
        # Clean duplicates if requested
        if options['clean'] or options['dry_run']:
            self._clean_duplicates(duplicate_sets, json_dir, json_files, 
                                  dry_run=options['dry_run'])
    
    def _hash_component(self, component, key_fields):
        """Generate a hash of component based on key fields."""
        if key_fields is None:
            # Use all fields for exact matching
            data_to_hash = json.dumps(component, sort_keys=True)
        else:
            # Use only specified fields
            data_to_hash = {}
            for field in key_fields:
                if field in component:
                    value = component[field]
                    # Handle non-string values
                    if not isinstance(value, str):
                        value = json.dumps(value)
                    data_to_hash[field] = value
            data_to_hash = json.dumps(data_to_hash, sort_keys=True)
        
        return hashlib.md5(data_to_hash.encode()).hexdigest()
    
    def _clean_duplicates(self, duplicate_sets, json_dir, json_files, dry_run=True):
        """Remove duplicate entries from JSON files."""
        if dry_run:
            self.stdout.write(self.style.WARNING("\nDRY RUN - No changes will be made"))
        else:
            self.stdout.write(self.style.WARNING("\nCleaning duplicates from files..."))
        
        # Track changes for each file
        file_changes = defaultdict(int)
        
        # For each duplicate set, keep the first one and remove others
        for dup_set in duplicate_sets:
            keep = dup_set[0]  # Keep the first occurrence
            remove = dup_set[1:]  # Remove the rest
            
            self.stdout.write(f"Keeping component in {keep['file']} for CMU ID {keep['cmu_id']}")
            for entry in remove:
                self.stdout.write(f"  Removing duplicate from {entry['file']} for CMU ID {entry['cmu_id']}")
                file_changes[entry['file']] += 1
        
        if dry_run:
            self.stdout.write(self.style.SUCCESS("\nDry run complete. Run with --clean to apply changes."))
            return
            
        # Now update the files
        for file_name in set([entry['file'] for dup_set in duplicate_sets for entry in dup_set]):
            # Get full path
            file_path = os.path.join(json_dir, file_name)
            
            try:
                # Read the file
                with open(file_path, 'r') as f:
                    file_data = json.load(f)
                
                # Create a set of hashes to keep for each CMU ID
                keep_hashes = {}
                for dup_set in duplicate_sets:
                    # Find the hash to keep
                    keep_hash = dup_set[0]['hash']
                    # Record which CMU IDs have this duplicate hash
                    for entry in dup_set:
                        if entry['file'] == file_name:
                            if entry['cmu_id'] not in keep_hashes:
                                keep_hashes[entry['cmu_id']] = set()
                            # If this is the one to keep, add its hash
                            if entry == dup_set[0]:
                                keep_hashes[entry['cmu_id']].add(keep_hash)
                
                # Filter components
                components_removed = 0
                for cmu_id in file_data.keys():
                    if cmu_id in keep_hashes:
                        # Get list of component hashes to keep
                        keep_set = keep_hashes[cmu_id]
                        original_count = len(file_data[cmu_id])
                        
                        # Filter out duplicates - keep only the first occurrence of each hash
                        seen_hashes = set()
                        filtered_components = []
                        
                        for component in file_data[cmu_id]:
                            component_hash = self._hash_component(component, None)  # Use exact match
                            if component_hash not in seen_hashes:
                                seen_hashes.add(component_hash)
                                filtered_components.append(component)
                        
                        # Update the list
                        file_data[cmu_id] = filtered_components
                        components_removed += original_count - len(filtered_components)
                
                # Write the file back
                with open(file_path, 'w') as f:
                    json.dump(file_data, f, indent=2)
                
                self.stdout.write(f"Updated {file_name}: removed {components_removed} duplicates")
            
            except Exception as e:
                self.stderr.write(f"Error updating {file_name}: {str(e)}")
        
        self.stdout.write(self.style.SUCCESS("Duplicate cleanup complete!")) 