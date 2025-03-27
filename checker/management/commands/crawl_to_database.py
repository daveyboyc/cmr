import time
import requests
import traceback
import json
import os
from django.core.management.base import BaseCommand
from django.db import transaction
from django.conf import settings
from ...models import Component

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
        """Crawl all CMU IDs from the API with static progress display."""
        # CMU API endpoint
        cmu_api_url = "https://api.neso.energy/api/3/action/datastore_search"
        cmu_resource_id = "25a5fa2e-873d-41c5-8aaf-fbc2b06d79e6"
        
        # Get total CMUs first if not already set
        if not self.stats['total_cmus']:
            self.stats['total_cmus'] = self.get_total_cmus()
        
        # Process CMUs in batches
        continue_crawl = True
        current_offset = self.offset
        start_time = self.stats.get('start_time', time.time())
        last_update_time = 0
        
        # Initial progress display
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("\033[s")  # Save cursor position
        self.stdout.write("Progress: 0.0% (0/{})".format(self.stats['total_cmus']))
        self.stdout.write("\nComponents found: 0 | Added: 0 | Skipped: 0")
        self.stdout.write("\nProcessing rate: 0.0 CMUs/second | ETA: calculating...")
        self.stdout.write("\nCurrent CMU ID: ---")
        self.stdout.write("\nBatch offset: 0")
        self.stdout.write("\n" + "=" * 50)
        
        while continue_crawl:
            # Save checkpoint before processing batch
            self.stats['last_offset'] = current_offset
            self.save_checkpoint()
            
            # Calculate progress
            progress = (current_offset / self.stats['total_cmus'] * 100) if self.stats['total_cmus'] > 0 else 0
            
            # Calculate elapsed time accounting for resume
            if 'resumed_at' in self.stats:
                prev_duration = self.stats.get('resumed_at', time.time()) - start_time
                current_duration = time.time() - self.stats.get('resumed_at', time.time())
                elapsed_time = prev_duration + current_duration
            else:
                elapsed_time = time.time() - start_time
            
            # Calculate processing rates
            if elapsed_time > 0 and self.stats['cmu_ids_processed'] > 0:
                rate = self.stats['cmu_ids_processed'] / elapsed_time
                remaining = self.stats['total_cmus'] - current_offset
                eta_seconds = remaining / rate if rate > 0 else 0
                
                # Format as hours:minutes:seconds
                eta_hours = int(eta_seconds // 3600)
                eta_minutes = int((eta_seconds % 3600) // 60)
                eta_seconds = int(eta_seconds % 60)
                
                eta_str = f"{eta_hours:02d}:{eta_minutes:02d}:{eta_seconds:02d}"
            else:
                rate = 0
                eta_str = "calculating..."
            
            # Update progress display (only every 0.1 seconds to reduce flicker)
            current_time = time.time()
            if current_time - last_update_time >= 0.1:
                self.stdout.write("\033[u")  # Restore cursor position
                self.stdout.write("\033[K")  # Clear line
                self.stdout.write(f"Progress: {progress:.1f}% ({current_offset}/{self.stats['total_cmus']})")
                self.stdout.write("\033[K")  # Clear line
                self.stdout.write(f"\nComponents found: {self.stats['components_found']} | Added: {self.stats['components_added']} | Skipped: {self.stats.get('components_skipped', 0)}")
                self.stdout.write("\033[K")  # Clear line
                self.stdout.write(f"\nProcessing rate: {rate:.1f} CMUs/second | ETA: {eta_str}")
                self.stdout.write("\033[K")  # Clear line
                self.stdout.write(f"\nCurrent CMU ID: {self.stats.get('last_cmu_id', '---')}")
                self.stdout.write("\033[K")  # Clear line
                self.stdout.write(f"\nBatch offset: {current_offset}")
                
                last_update_time = current_time
            
            # Fetch batch of CMU IDs
            cmu_params = {
                "resource_id": cmu_resource_id,
                "limit": self.batch_size,
                "offset": current_offset
            }
            
            # Add company filter if specified (fallback to None if not defined)
            company_filter = getattr(self, 'company_filter', None)
            if company_filter:
                cmu_params["q"] = company_filter
            
            try:
                cmu_response = requests.get(cmu_api_url, params=cmu_params, timeout=30)
                cmu_response.raise_for_status()
                cmu_data = cmu_response.json()
                
                if cmu_data.get("success"):
                    cmu_records = cmu_data.get("result", {}).get("records", [])
                    
                    if not cmu_records:
                        self.stdout.write("\n\nNo more CMU records found. Crawl complete.")
                        break
                    
                    # Process each CMU ID in this batch
                    for record in cmu_records:
                        cmu_id = record.get("CMU ID")
                        if cmu_id:
                            self.stats['last_cmu_id'] = cmu_id
                            
                            # Update current CMU in progress display
                            if current_time - last_update_time >= 0.1:
                                self.stdout.write("\033[u")  # Restore cursor position
                                self.stdout.write("\033[3B")  # Move down 3 lines
                                self.stdout.write("\033[K")  # Clear line
                                self.stdout.write(f"\nCurrent CMU ID: {cmu_id}")
                                self.stdout.write("\033[2A")  # Move back up 2 lines
                            
                            self.crawl_single_cmu(cmu_id, record)
                        
                        # Check if we've reached the limit
                        if self.limit > 0 and self.stats['cmu_ids_processed'] >= self.limit:
                            self.stdout.write("\n\nReached limit of {self.limit} CMU IDs. Stopping crawl.")
                            continue_crawl = False
                            break
                    
                    # Update offset for next batch
                    current_offset += len(cmu_records)
                    self.stats['batches_processed'] = self.stats.get('batches_processed', 0) + 1
                else:
                    self.stderr.write(f"\n\nCMU API request unsuccessful: {cmu_data.get('error', 'Unknown error')}")
                    self.stats['errors'] = self.stats.get('errors', 0) + 1
                    break
                    
            except Exception as e:
                self.stderr.write(f"\n\nError fetching CMU IDs: {str(e)}")
                traceback.print_exc()
                self.stats['errors'] = self.stats.get('errors', 0) + 1
                break
                
            # Prevent rate limiting
            time.sleep(self.sleep_time)
        
        # Final progress update
        self.stdout.write("\033[u")  # Restore cursor position
        self.stdout.write("\033[K")  # Clear line
        self.stdout.write(f"Progress: {progress:.1f}% ({current_offset}/{self.stats['total_cmus']}) - COMPLETED")
        self.stdout.write("\033[K")  # Clear line
        self.stdout.write(f"\nComponents found: {self.stats['components_found']} | Added: {self.stats['components_added']} | Skipped: {self.stats.get('components_skipped', 0)}")
        self.stdout.write("\033[K")  # Clear line
        self.stdout.write(f"\nProcessing rate: {rate:.1f} CMUs/second | Total time: {elapsed_time:.1f} seconds")
        self.stdout.write("\033[K")  # Clear line
        self.stdout.write(f"\nLast CMU ID: {self.stats.get('last_cmu_id', '---')}")
        self.stdout.write("\033[K")  # Clear line
        self.stdout.write(f"\nFinal offset: {current_offset}")
        self.stdout.write("\n" + "=" * 50)
        
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
                        additional_data=component  # Store all data as JSON
                    )
                    components_added += 1
                    
                except Exception as e:
                    self.stats['errors'] = self.stats.get('errors', 0) + 1
            
            # Update global statistics safely
            self.stats['components_added'] = self.stats.get('components_added', 0) + components_added
            self.stats['components_skipped'] = self.stats.get('components_skipped', 0) + components_skipped