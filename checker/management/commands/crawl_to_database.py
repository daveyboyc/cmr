import time
import requests
import traceback
from django.core.management.base import BaseCommand
from django.db import transaction
from ...models import Component

class Command(BaseCommand):
    help = 'Crawl component data directly into the database'

    def add_arguments(self, parser):
        parser.add_argument('--batch-size', type=int, default=100, help='How many CMUs to process per batch')
        parser.add_argument('--limit', type=int, default=0, help='Max CMUs to process (0 = unlimited)')
        parser.add_argument('--offset', type=int, default=0, help='Starting offset for CMU IDs')
        parser.add_argument('--cmu', type=str, help='Process specific CMU ID')

    def handle(self, *args, **options):
        # Start the crawl
        self.stdout.write(self.style.SUCCESS("Starting component crawl to database"))
        start_time = time.time()
        
        # Get total number of CMUs first
        total_cmus = self.get_total_cmus()
        self.stdout.write(f"Total CMUs to process: {total_cmus}")
        
        # Track statistics
        stats = {
            'cmu_ids_processed': 0,
            'cmu_ids_with_components': 0,
            'components_found': 0,
            'components_added': 0,
            'errors': 0,
            'total_cmus': total_cmus
        }
        
        # Process specific CMU if requested
        if options['cmu']:
            self.crawl_single_cmu(options['cmu'], stats)
        else:
            # Process all CMUs in batches
            self.crawl_all_cmus(options['batch_size'], options['limit'], options['offset'], stats)
        
        # Print final statistics
        elapsed_time = time.time() - start_time
        self.stdout.write(self.style.SUCCESS("\nCrawl completed in {:.2f} seconds".format(elapsed_time)))
        self.stdout.write(f"  CMU IDs processed: {stats['cmu_ids_processed']} of {stats['total_cmus']}")
        self.stdout.write(f"  CMU IDs with components: {stats['cmu_ids_with_components']}")
        self.stdout.write(f"  Components found: {stats['components_found']}")
        self.stdout.write(f"  Components added to database: {stats['components_added']}")
        self.stdout.write(f"  Errors encountered: {stats['errors']}")
    
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
            response = requests.get(cmu_api_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get("success"):
                return data.get("result", {}).get("total", 0)
            return 0
            
        except Exception as e:
            self.stderr.write(f"Error getting total CMUs: {str(e)}")
            return 0

    def crawl_all_cmus(self, batch_size, limit, offset, stats):
        """Crawl all CMU IDs from the API."""
        # CMU API endpoint
        cmu_api_url = "https://api.neso.energy/api/3/action/datastore_search"
        cmu_resource_id = "25a5fa2e-873d-41c5-8aaf-fbc2b06d79e6"
        
        # Process CMUs in batches
        continue_crawl = True
        current_offset = offset
        
        while continue_crawl:
            progress_percent = (stats['cmu_ids_processed'] / stats['total_cmus']) * 100 if stats['total_cmus'] > 0 else 0
            remaining = stats['total_cmus'] - stats['cmu_ids_processed']
            
            self.stdout.write(f"Progress: {progress_percent:.1f}% ({stats['cmu_ids_processed']}/{stats['total_cmus']}, {remaining} remaining)")
            self.stdout.write(f"Fetching CMU batch at offset {current_offset}")
            
            # Fetch batch of CMU IDs
            cmu_params = {
                "resource_id": cmu_resource_id,
                "limit": batch_size,
                "offset": current_offset
            }
            
            try:
                cmu_response = requests.get(cmu_api_url, params=cmu_params, timeout=30)
                cmu_response.raise_for_status()
                cmu_data = cmu_response.json()
                
                if cmu_data.get("success"):
                    cmu_records = cmu_data.get("result", {}).get("records", [])
                    
                    if not cmu_records:
                        self.stdout.write("No more CMU records found. Crawl complete.")
                        break
                    
                    # Process each CMU ID in this batch
                    for record in cmu_records:
                        cmu_id = record.get("CMU ID")
                        if cmu_id:
                            self.crawl_single_cmu(cmu_id, stats, record)
                        
                        # Check if we've reached the limit
                        if limit > 0 and stats['cmu_ids_processed'] >= limit:
                            self.stdout.write(f"Reached limit of {limit} CMU IDs. Stopping crawl.")
                            continue_crawl = False
                            break
                    
                    # Update offset for next batch
                    current_offset += len(cmu_records)
                else:
                    self.stderr.write(f"CMU API request unsuccessful: {cmu_data.get('error', 'Unknown error')}")
                    stats['errors'] += 1
                    break
                    
            except Exception as e:
                self.stderr.write(f"Error fetching CMU IDs: {str(e)}")
                traceback.print_exc()
                stats['errors'] += 1
                break
                
            # Prevent rate limiting
            time.sleep(1)
    
    def crawl_single_cmu(self, cmu_id, stats, cmu_record=None):
        """Crawl components for a single CMU ID."""
        # Components API endpoint
        component_api_url = "https://api.neso.energy/api/3/action/datastore_search"
        component_resource_id = "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8"
        
        self.stdout.write(f"Processing CMU ID: {cmu_id}")
        stats['cmu_ids_processed'] += 1
        
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
                stats['components_found'] += len(component_records)
                
                if component_records:
                    stats['cmu_ids_with_components'] += 1
                    self.stdout.write(f"  Found {len(component_records)} components")
                    
                    # Get company name from CMU record if available
                    company_name = None
                    if cmu_record:
                        company_name = cmu_record.get("Name of Applicant") or cmu_record.get("Parent Company")
                    
                    # Save components to database
                    self.save_components_to_db(cmu_id, component_records, company_name, stats)
                else:
                    self.stdout.write("  No components found")
            else:
                self.stderr.write(f"  Component API request unsuccessful: {component_data.get('error', 'Unknown error')}")
                stats['errors'] += 1
                
        except Exception as e:
            self.stderr.write(f"  Error fetching components for {cmu_id}: {str(e)}")
            traceback.print_exc()
            stats['errors'] += 1
            
    def save_components_to_db(self, cmu_id, component_records, company_name, stats):
        """Save component records to the database."""
        # Use a transaction for better performance and atomicity
        with transaction.atomic():
            for component in component_records:
                # Get component ID if available
                component_id = component.get("_id", "")
                
                # Skip if we already have this component in the database
                if component_id and Component.objects.filter(component_id=component_id).exists():
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
                    stats['components_added'] += 1
                    
                except Exception as e:
                    self.stderr.write(f"  Error saving component {component_id}: {str(e)}")
                    stats['errors'] += 1
