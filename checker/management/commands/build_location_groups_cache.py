import re
import json
import hashlib
import logging
import time

from django.core.management.base import BaseCommand
from django.core.cache import cache
from django.db.models import F

from checker.models import Component
# from checker.utils import normalize_location # Ideal: Move normalize_location here
# For now, we'll define normalize_location and is_auction_year_active locally

logger = logging.getLogger(__name__)

# --- Helper functions (mirrored from checker_tags.py or to be moved to utils) ---
def normalize_location_local(loc):
    """Normalize location for consistent grouping."""
    if not loc:
        return ""
        
    # Convert to lowercase and strip whitespace
    norm = str(loc).lower().strip()
    
    # Simple replacements instead of regex
    norm = norm.replace(',', ' ')
    norm = norm.replace('.', ' ')
    norm = norm.replace('-', ' ')
    norm = norm.replace('_', ' ')
    norm = norm.replace('/', ' ')
    norm = norm.replace('\\\\', ' ') # Ensure backslashes are handled
    
    # Replace multiple spaces with single space
    while '  ' in norm:
        norm = norm.replace('  ', ' ')
        
    # REMOVED SPECIAL CASE:
    # if "energy centre" in norm and "mosley" in norm:
    #     return "energy centre lower mosley street"
        
    return norm

def is_auction_year_active_local(auction_name):
    """Check if an auction year is 2024-25 or later."""
    if not auction_name:
        return False
        
    # Simple extract the first 4-digit number as the year
    parts = str(auction_name).split()
    for part in parts:
        if len(part) >= 4 and part[:4].isdigit():
            year_str = part[:4]
            try:
                year_int = int(year_str)
                # Active if 2024 or later
                return year_int >= 2024
            except ValueError:
                continue
    
    return False

def extract_year_local(auction_name):
    """Extracts year from auction name for sorting."""
    # Simple extract the first 4-digit number as the year
    parts = str(auction_name).split()
    for part in parts:
        if len(part) >= 4 and part[:4].isdigit():
            year_str = part[:4]
            try:
                return int(year_str)
            except ValueError:
                continue
    
    return 0
# --- End Helper functions ---

class Command(BaseCommand):
    help = 'Builds and caches component location groups in Redis.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--force-rebuild',
            action='store_true',
            help='Force a rebuild by clearing existing cached groups first.',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Number of components to process in each batch.',
        )

    def handle(self, *args, **options):
        force_rebuild = options['force_rebuild']
        batch_size = options['batch_size']
        
        start_time = time.time()
        logger.info("Starting to build location groups cache...")

        if force_rebuild:
            logger.info("Force rebuild requested. Clearing existing loc_group:* keys from Redis...")
            self.stdout.write(self.style.WARNING("Note: True cache clearing by pattern depends on Redis backend and isn't universally supported via Django's cache API. New entries will overwrite old ones."))

        # Fetch all components with necessary fields.
        # Using .values() to get dictionaries directly and select specific fields
        # to optimize memory usage and speed.
        components_qs = Component.objects.all().values(
            'id', 'location', 'description', 'cmu_id', 'auction_name', 'delivery_year',
            'company_name', 'technology', 'status', 'type', 'component_id', 'additional_data', 'derated_capacity_mw'
        ).order_by('id')  # Consistent order for processing

        total_components = components_qs.count()
        logger.info(f"Found {total_components} components to process.")

        grouped_data_for_cache = {}  # Temp dict to hold groups before caching

        for i in range(0, total_components, batch_size):
            batch_end = min(i + batch_size, total_components)
            batch_size_actual = batch_end - i
            batch_num = i // batch_size + 1
            total_batches = (total_components + batch_size - 1) // batch_size
            
            self.stdout.write(f"Processing batch {batch_num}/{total_batches} ({batch_size_actual} components)...")
            
            # Get components for this batch
            batch_components = list(components_qs[i:batch_end])

            for comp_dict in batch_components:
                # Create component dict with defaults for missing values
                current_component = {
                    'id': comp_dict['id'],
                    'location': comp_dict['location'] or '',
                    'description': comp_dict['description'] or '',
                    'cmu_id': comp_dict['cmu_id'] or '',
                    'auction_name': comp_dict['auction_name'] or '',
                    'delivery_year': comp_dict['delivery_year'] or '',
                    'company_name': comp_dict['company_name'] or '',
                    'technology': comp_dict['technology'] or '',
                    'status': comp_dict['status'] or '',
                    'type': comp_dict['type'] or '',
                    'component_id_str': comp_dict['component_id'] or '',
                    'additional_data': comp_dict['additional_data'] or {},
                    'derated_capacity_mw': comp_dict['derated_capacity_mw']
                }

                # Create normalized key for grouping
                norm_location = normalize_location_local(current_component['location'])
                description = current_component['description']
                group_key_tuple = (norm_location, description)

                # Initialize group if not seen before
                if group_key_tuple not in grouped_data_for_cache:
                    grouped_data_for_cache[group_key_tuple] = {
                        'location': current_component['location'],
                        'description': description,
                        'cmu_ids': set(),
                        'auction_names': set(),
                        'auction_to_components': {},
                        'active_status': False,
                        'components': [],
                        'first_component': current_component,
                        'count': 0
                    }
                
                # Add component to group
                group = grouped_data_for_cache[group_key_tuple]
                group['components'].append(current_component)
                group['count'] += 1
                
                # Add CMU ID if present
                if current_component['cmu_id']:
                    group['cmu_ids'].add(current_component['cmu_id'])
                
                # Process auction name
                if current_component['auction_name']:
                    group['auction_names'].add(current_component['auction_name'])
                    
                    # Add component ID to auction mapping
                    if current_component['id'] and current_component['auction_name']:
                        if current_component['auction_name'] not in group['auction_to_components']:
                            group['auction_to_components'][current_component['auction_name']] = []
                        group['auction_to_components'][current_component['auction_name']].append(current_component['id'])
                
                # Check if this auction makes the group active
                if not group['active_status'] and is_auction_year_active_local(current_component['auction_name']):
                    group['active_status'] = True
            
            self.stdout.write(f"Finished processing batch. Current groups: {len(grouped_data_for_cache)}")

        logger.info(f"Aggregated {len(grouped_data_for_cache)} unique location groups.")
        logger.info("Storing groups in Redis...")

        # Prepare groups for caching in Redis
        cached_count = 0
        logged_keys_count = 0 # Counter for logging sample keys
        MAX_LOGGED_KEYS = 5 # How many sample keys to log

        for group_key_tuple, group_content in grouped_data_for_cache.items():
            # Convert set to list and sort for cmu_ids
            group_content['cmu_ids'] = sorted(list(group_content['cmu_ids']))
            
            # Sort auction names by year
            auction_names_list = list(group_content['auction_names'])
            auction_names_list.sort(key=extract_year_local, reverse=True)
            group_content['auction_names'] = auction_names_list

            # Create Redis key from group key tuple
            norm_loc_str, desc_str = group_key_tuple
            redis_key_seed = f"{norm_loc_str}_{desc_str}"
            redis_key = f"loc_group:{hashlib.sha256(redis_key_seed.encode('utf-8')).hexdigest()}"
            
            if logged_keys_count < MAX_LOGGED_KEYS:
                logger.info(f"Sample Redis Key Generation: norm_loc='{norm_loc_str}', desc_len={len(desc_str)} -> key='{redis_key}'")
                logged_keys_count += 1

            try:
                # Store in Redis indefinitely
                cache.set(redis_key, json.dumps(group_content), timeout=None)
                cached_count += 1
            except Exception as e:
                logger.error(f"Error caching group with key {redis_key}: {str(e)}")
                logger.error(f"Key details: norm_loc='{norm_loc_str}', desc_len={len(desc_str)}")

        end_time = time.time()
        elapsed = end_time - start_time
        
        logger.info(f"Successfully built and cached {cached_count} location groups.")
        logger.info(f"Total time taken: {elapsed:.2f} seconds.")
        self.stdout.write(self.style.SUCCESS(f"Successfully built and cached {cached_count} location groups in {elapsed:.2f} seconds.")) 