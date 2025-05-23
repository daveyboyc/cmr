import time
import requests
import traceback
import json
import os
from django.core.management.base import BaseCommand
from django.db import transaction
from django.conf import settings
from checker.models import Component
from checker.models import CMURegistry

class Command(BaseCommand):
    help = 'Crawl component data directly into the database with resume capabilities'

    def add_arguments(self, parser):
        parser.add_argument('--batch-size', type=int, default=100, help='How many CMUs to process per batch')
        parser.add_argument('--limit', type=int, default=0, help='Max CMUs to process (0 = unlimited)')
        parser.add_argument('--offset', type=int, default=0, help='Starting offset for CMU IDs')
        parser.add_argument('--cmu', type=str, help='Process specific CMU ID')
        parser.add_argument('--resume', action='store_true', help='Resume from last saved checkpoint')
        parser.add_argument('--force', action='store_true', help='Process all CMUs even if they already have components')
        parser.add_argument('--company', type=str, help='Process only CMUs for this company')
        parser.add_argument('--sleep', type=float, default=1.0, help='Sleep time between batches to avoid rate limiting')

    def handle(self, *args, **options):
        # Start the crawl
        self.stdout.write(self.style.SUCCESS("Starting component crawl to database"))
        start_time = time.time()
        
        # Extract options
        self.batch_size = options['batch_size']
        self.limit = options['limit']
        self.offset = options['offset']
        self.specific_cmu = options['cmu']
        self.resume = options['resume']
        self.force_update = options['force']
        self.company_filter = options['company']
        self.sleep_time = options['sleep']
        
        # Setup checkpoint directory
        self.checkpoint_dir = os.path.join(settings.BASE_DIR, 'checkpoints')
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        self.checkpoint_file = os.path.join(self.checkpoint_dir, 'crawler_checkpoint.json')
        
        # Get total number of CMUs first
        total_cmus = self.get_total_cmus()
        
        # Track statistics
        self.stats = {
            'cmu_ids_processed': 0,
            'cmu_ids_with_components': 0,
            'components_found': 0,
            'components_added': 0,
            'components_skipped': 0,
            'errors': 0,
            'total_cmus': total_cmus,
            'start_time': start_time,
            'last_offset': self.offset,
            'last_cmu_id': None,
            'batches_processed': 0
        }
        
        # Load checkpoint if resuming
        if self.resume:
            self.load_checkpoint()
            
        # Process specific CMU if requested
        if self.specific_cmu:
            self.crawl_single_cmu(self.specific_cmu, self.stats)
        else:
            # Process all CMUs in batches
            self.crawl_all_cmus()
        
        # Print final statistics
        elapsed_time = time.time() - start_time
        self.stdout.write(self.style.SUCCESS("\nCrawl completed in {:.2f} seconds".format(elapsed_time)))
        self.stdout.write(f"  CMU IDs processed: {self.stats['cmu_ids_processed']} of {self.stats['total_cmus']}")
        self.stdout.write(f"  CMU IDs with components: {self.stats['cmu_ids_with_components']}")
        self.stdout.write(f"  Components found: {self.stats['components_found']}")
        self.stdout.write(f"  Components added to database: {self.stats['components_added']}")
        self.stdout.write(f"  Components skipped: {self.stats.get('components_skipped', 0)}")
        self.stdout.write(f"  Errors encountered: {self.stats['errors']}")
    
    def load_checkpoint(self):
        """Load the most recent checkpoint if available."""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                
                # Restore state from checkpoint
                self.stats = checkpoint.get('stats', self.stats)
                self.offset = checkpoint.get('offset', self.offset)
                
                # Mark as resumed for ETA calculation
                self.stats['resumed_at'] = time.time()
                
                self.stdout.write(self.style.SUCCESS(
                    f"Resuming crawl from offset {self.offset}, "
                    f"already processed {self.stats['cmu_ids_processed']} CMUs"
                ))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error loading checkpoint: {e}"))
                self.stdout.write("Starting fresh crawl")
        else:
            self.stdout.write("No checkpoint found. Starting fresh crawl.")
    
    def save_checkpoint(self, forced=False):
        """Save current crawl progress to checkpoint file."""
        try:
            # Prepare checkpoint data
            checkpoint = {
                'stats': self.stats,
                'offset': self.stats['last_offset'],
                'timestamp': time.time()
            }
            
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f, indent=2)
                
            self.stdout.write(f"Saved checkpoint at offset {self.stats['last_offset']}")
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error saving checkpoint: {e}"))
    
    def get_total_cmus(self):
        """Get total number of CMUs available."""
        cmu_api_url = "https://api.neso.energy/api/3/action/datastore_search"
        cmu_resource_id = "25a5fa2e-873d-41c5-8aaf-fbc2b06d79e6"
        
        try:
            # Make a request with limit=0 to get total
            params = {
                "resource_id": cmu_resource_id,
                "limit": 0
            }
            
            # Add company filter if specified
            if self.company_filter:
                params["q"] = self.company_filter
                
            response = requests.get(cmu_api_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get("success"):
                return data.get("result", {}).get("total", 0)
            return 0
            
        except Exception as e:
            self.stderr.write(f"Error getting total CMUs: {str(e)}")
            return 0

    def crawl_all_cmus(self):
        """Crawl all CMU IDs from the API with a simple spinner animation."""
        # CMU API endpoint
        cmu_api_url = "https://api.neso.energy/api/3/action/datastore_search"
        cmu_resource_id = "25a5fa2e-873d-41c5-8aaf-fbc2b06d79e6"
        
        # Process CMUs in batches
        continue_crawl = True
        current_offset = self.offset
        start_time = self.stats.get('start_time', time.time())
        
        # Spinner characters for animation
        spinner_chars = ['-', '\\', '|', '/']
        spinner_idx = 0
        
        # Initial progress message
        self.stdout.write("\nStarting crawl from offset {}".format(current_offset))
        
        while continue_crawl:
            # Save checkpoint before processing batch
            self.stats['last_offset'] = current_offset
            self.save_checkpoint()
            
            # Calculate progress
            progress = (current_offset / self.stats['total_cmus'] * 100) if self.stats['total_cmus'] > 0 else 0
            elapsed_time = time.time() - start_time
            
            if elapsed_time > 0 and self.stats['cmu_ids_processed'] > 0:
                rate = self.stats['cmu_ids_processed'] / elapsed_time
                eta_seconds = (self.stats['total_cmus'] - current_offset) / rate if rate > 0 else 0
                eta_str = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
            else:
                rate = 0
                eta_str = "calculating..."
            
            # Update spinner character
            spinner = spinner_chars[spinner_idx]
            spinner_idx = (spinner_idx + 1) % len(spinner_chars)
            
            # Simple progress line with spinner
            self.stdout.write(
                f"\r{spinner} Progress: {progress:.1f}% | Offset: {current_offset}/{self.stats['total_cmus']} | "
                f"Components: {self.stats['components_found']} found, {self.stats['components_added']} added, "
                f"{self.stats.get('components_skipped', 0)} skipped | ETA: {eta_str}", 
                ending=''
            )
            self.stdout.flush()  # Make sure it updates immediately
            
            # Fetch batch of CMU IDs
            cmu_params = {
                "resource_id": cmu_resource_id,
                "limit": self.batch_size,
                "offset": current_offset
            }
            
            try:
                cmu_response = requests.get(cmu_api_url, params=cmu_params, timeout=30)
                cmu_response.raise_for_status()
                cmu_data = cmu_response.json()
                
                if cmu_data.get("success"):
                    cmu_records = cmu_data.get("result", {}).get("records", [])
                    
                    if not cmu_records:
                        self.stdout.write("\nNo more CMU records found. Crawl complete.")
                        break
                    
                    # Process each CMU ID in this batch
                    cmus_to_update_or_create = []
                    for record in cmu_records:
                        cmu_id = record.get("CMU ID")
                        if cmu_id:
                            # Prepare data for update_or_create for CMURegistry
                            cmus_to_update_or_create.append(
                                CMURegistry(cmu_id=cmu_id, raw_data=record)
                            )
                            
                            # --- Original component processing logic --- 
                            self.stats['last_cmu_id'] = cmu_id
                            self.crawl_single_cmu(cmu_id, record)
                        
                        # Check if we've reached the limit
                        if self.limit > 0 and self.stats['cmu_ids_processed'] >= self.limit:
                            self.stdout.write(f"\nReached limit of {self.limit} CMU IDs. Stopping crawl.")
                            continue_crawl = False
                            break # Exit inner loop (over records)
                    
                    # Bulk update or create CMURegistry entries for the batch
                    if cmus_to_update_or_create:
                        try:
                            # Note: Django's bulk_update_or_create requires Django 4.1+
                            # Using a loop with update_or_create for broader compatibility,
                            # though less efficient than bulk operations.
                            # If using Django 4.1+, consider:
                            # CMURegistry.objects.bulk_update_or_create(cmus_to_update_or_create, ['raw_data'], match_field='cmu_id')
                            with transaction.atomic():
                                for cmu_obj in cmus_to_update_or_create:
                                    CMURegistry.objects.update_or_create(
                                        cmu_id=cmu_obj.cmu_id,
                                        defaults={'raw_data': cmu_obj.raw_data}
                                    )
                            self.stdout.write(f"\nUpdated/Created {len(cmus_to_update_or_create)} CMU registry entries for batch.", ending='')
                        except Exception as bulk_err:
                            self.stderr.write(f"\nError updating/creating CMURegistry entries: {bulk_err}")
                            # Optionally log this error without stopping the crawl

                    if not continue_crawl:
                        break # Exit outer loop (while continue_crawl)
                    
                    # Update offset for next batch
                    current_offset += len(cmu_records)
                    self.stats['batches_processed'] = self.stats.get('batches_processed', 0) + 1
                else:
                    self.stderr.write(f"\nCMU API request unsuccessful: {cmu_data.get('error', 'Unknown error')}")
                    self.stats['errors'] = self.stats.get('errors', 0) + 1
                    break
                    
            except Exception as e:
                self.stderr.write(f"\nError fetching CMU IDs: {str(e)}")
                traceback.print_exc()
                self.stats['errors'] = self.stats.get('errors', 0) + 1
                break
                
            # Prevent rate limiting
            time.sleep(self.sleep_time)
        
        # Final progress update with newline
        self.stdout.write("\nCrawl completed!")
        self.stdout.write(
            f"Final stats: {self.stats['components_found']} components found, "
            f"{self.stats['components_added']} added, "
            f"{self.stats.get('components_skipped', 0)} skipped, "
            f"{self.stats.get('errors', 0)} errors"
        )
        
        # Save final checkpoint
        self.stats['last_offset'] = current_offset
        self.save_checkpoint(forced=True)
    
    def crawl_single_cmu(self, cmu_id, cmu_record=None):
        """Crawl components for a single CMU ID (silent version)."""
        # Components API endpoint
        component_api_url = "https://api.neso.energy/api/3/action/datastore_search"
        component_resource_id = "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8"
        
        self.stats['cmu_ids_processed'] = self.stats.get('cmu_ids_processed', 0) + 1
        
        # Update skipped components safely
        if not self.force_update and not self.specific_cmu:
            db_count = Component.objects.filter(cmu_id=cmu_id).count()
            if db_count > 0:
                self.stats['components_skipped'] = self.stats.get('components_skipped', 0) + db_count
                return
        
        # Fetch components for this CMU ID
        component_params = {
            "resource_id": component_resource_id,
            "q": cmu_id,
            "limit": 1000  # Get up to 1000 components per CMU
        }
        
        try:
            component_response = requests.get(component_api_url, params=component_params, timeout=30)
            component_response.raise_for_status()
            component_data = component_response.json()
            
            if component_data.get("success"):
                component_records = component_data.get("result", {}).get("records", [])
                self.stats['components_found'] = self.stats.get('components_found', 0) + len(component_records)
                
                if component_records:
                    self.stats['cmu_ids_with_components'] = self.stats.get('cmu_ids_with_components', 0) + 1
                    
                    # Get company name from CMU record if available
                    company_name = None
                    if cmu_record:
                        company_name = cmu_record.get("Name of Applicant") or cmu_record.get("Parent Company")
                    
                    # Save components to database
                    self.save_components_to_db(cmu_id, component_records, company_name)
            else:
                self.stats['errors'] = self.stats.get('errors', 0) + 1
                
        except Exception as e:
            self.stats['errors'] = self.stats.get('errors', 0) + 1
            
    def save_components_to_db(self, cmu_id, component_records, company_name):
        """Save component records to the database (silent version)."""
        # Use a transaction for better performance and atomicity
        with transaction.atomic():
            components_added = 0
            components_skipped = 0
            
            for component in component_records:
                # Get component ID if available
                component_id = component.get("_id", "")
                
                # Skip if we already have this component in the database
                if component_id and Component.objects.filter(component_id=component_id).exists():
                    components_skipped += 1
                    continue
                
                try:
                    # Extract standard fields
                    location = component.get("Location and Post Code", "")
                    description = component.get("Description of CMU Components", "")
                    technology = component.get("Generating Technology Class", "")
                    auction_name = component.get("Auction Name", "")
                    delivery_year = component.get("Delivery Year", "")
                    status = component.get("Status", "")
                    type_value = component.get("Type", "")
                    
                    # Use company name from component if available, otherwise use CMU record
                    comp_company = component.get("Company Name", "")
                    if comp_company:
                        company_name = comp_company
                    
                    # --- Calculate derated_capacity_mw --- 
                    derated_capacity_mw = None
                    capacity_str = component.get("De-Rated Capacity")
                    if capacity_str is not None:
                        try:
                            derated_capacity_mw = float(capacity_str)
                        except (ValueError, TypeError):
                            pass # Keep as None if conversion fails
                    # --- End calculation ---
                    
                    # Create new component in database
                    Component.objects.create(
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
                        additional_data=component,  # Store all data as JSON
                        derated_capacity_mw=derated_capacity_mw # Set the new field
                    )
                    components_added += 1
                    
                except Exception as e:
                    self.stats['errors'] = self.stats.get('errors', 0) + 1
            
            # Update global statistics safely
            self.stats['components_added'] = self.stats.get('components_added', 0) + components_added
            self.stats['components_skipped'] = self.stats.get('components_skipped', 0) + components_skipped