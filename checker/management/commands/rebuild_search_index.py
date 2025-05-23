import time
import logging
from django.core.management.base import BaseCommand
from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank
from django.db import connection

from checker.models import Component

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Rebuilds the full-text search index for all component records'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size',
            type=int,
            default=10000,
            help='Number of records to process in each batch'
        )

    def handle(self, *args, **options):
        batch_size = options['batch_size']
        
        start_time = time.time()
        self.stdout.write(self.style.SUCCESS(f'Starting search index rebuild...'))
        
        # Check if we have the necessary PostgreSQL extensions installed
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM pg_extension WHERE extname = 'pg_trgm'")
                trgm_installed = cursor.fetchone()[0] > 0
                
                if not trgm_installed:
                    self.stdout.write(self.style.WARNING('pg_trgm extension not found. Installing...'))
                    cursor.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error checking/installing PostgreSQL extensions: {e}'))
        
        # Get total record count
        total_records = Component.objects.count()
        self.stdout.write(self.style.SUCCESS(f'Found {total_records} total records to index'))
        
        # Process in batches
        processed = 0
        batch_count = 0
        
        try:
            # Check if the search_vector field exists
            search_vector_exists = True
            try:
                # Just check if one record has the field by trying to access it
                Component.objects.filter(search_vector__isnull=True).exists()
            except Exception:
                search_vector_exists = False
                self.stdout.write(self.style.ERROR('search_vector field does not exist. Run migrations first!'))
                return
            
            # Update the search vector directly using SQL for performance
            self.stdout.write(self.style.SUCCESS('Using SQL UPDATE for search_vector field'))
            with connection.cursor() as cursor:
                cursor.execute("""
                UPDATE checker_component
                SET search_vector = 
                    setweight(to_tsvector('english', COALESCE(company_name, '')), 'A') ||
                    setweight(to_tsvector('english', COALESCE(location, '')), 'A') ||
                    setweight(to_tsvector('english', COALESCE(county, '')), 'B') ||
                    setweight(to_tsvector('english', COALESCE(outward_code, '')), 'B') ||
                    setweight(to_tsvector('english', COALESCE(description, '')), 'C') ||
                    setweight(to_tsvector('english', COALESCE(technology, '')), 'C') ||
                    setweight(to_tsvector('english', COALESCE(cmu_id, '')), 'D') ||
                    setweight(to_tsvector('english', COALESCE(auction_name, '')), 'D') ||
                    setweight(to_tsvector('english', COALESCE(delivery_year, '')), 'D')
                """)
                processed = cursor.rowcount
                
            self.stdout.write(self.style.SUCCESS(f'Updated search vector for {processed} records'))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error rebuilding search index: {e}'))
            import traceback
            self.stdout.write(traceback.format_exc())
        
        # Create trigger if it doesn't already exist
        try:
            with connection.cursor() as cursor:
                # Check if trigger function exists
                cursor.execute("SELECT COUNT(*) FROM pg_proc WHERE proname = 'component_search_vector_update'")
                function_exists = cursor.fetchone()[0] > 0
                
                if not function_exists:
                    self.stdout.write(self.style.SUCCESS('Creating search vector update trigger function'))
                    cursor.execute("""
                    CREATE OR REPLACE FUNCTION component_search_vector_update() RETURNS trigger AS $$
                    BEGIN
                        NEW.search_vector = 
                            setweight(to_tsvector('english', COALESCE(NEW.company_name, '')), 'A') ||
                            setweight(to_tsvector('english', COALESCE(NEW.location, '')), 'A') ||
                            setweight(to_tsvector('english', COALESCE(NEW.county, '')), 'B') ||
                            setweight(to_tsvector('english', COALESCE(NEW.outward_code, '')), 'B') ||
                            setweight(to_tsvector('english', COALESCE(NEW.description, '')), 'C') ||
                            setweight(to_tsvector('english', COALESCE(NEW.technology, '')), 'C') ||
                            setweight(to_tsvector('english', COALESCE(NEW.cmu_id, '')), 'D') ||
                            setweight(to_tsvector('english', COALESCE(NEW.auction_name, '')), 'D') ||
                            setweight(to_tsvector('english', COALESCE(NEW.delivery_year, '')), 'D');
                        RETURN NEW;
                    END
                    $$ LANGUAGE plpgsql;
                    """)
                
                # Check if trigger exists
                cursor.execute("SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'component_search_trigger'")
                trigger_exists = cursor.fetchone()[0] > 0
                
                if not trigger_exists:
                    self.stdout.write(self.style.SUCCESS('Creating search vector update trigger'))
                    cursor.execute("""
                    CREATE TRIGGER component_search_trigger
                    BEFORE INSERT OR UPDATE
                    ON checker_component
                    FOR EACH ROW
                    EXECUTE FUNCTION component_search_vector_update();
                    """)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error creating trigger: {e}'))
            import traceback
            self.stdout.write(traceback.format_exc())
        
        # Final report
        elapsed_time = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(
            f'Finished rebuilding search index in {elapsed_time:.2f} seconds. '
            f'Processed {processed} out of {total_records} records.'
        ))