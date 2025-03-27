import os
import json
import time
import glob
from django.core.management.base import BaseCommand
from django.db import connection
from django.db.models import Count
from ...models import Component

class Command(BaseCommand):
    help = 'Show database status and statistics'

    def add_arguments(self, parser):
        parser.add_argument('--json-stats', action='store_true', help='Show JSON file statistics for comparison')
        parser.add_argument('--top', type=int, default=10, help='Number of top companies to show')
        parser.add_argument('--benchmark', action='store_true', help='Run query performance benchmarks')

    def handle(self, *args, **options):
        show_json_stats = options['json_stats']
        top_n = options['top']
        run_benchmark = options['benchmark']
        
        self.stdout.write(self.style.SUCCESS("Database Status Report"))
        self.stdout.write("=" * 50)
        
        # Basic database statistics
        try:
            # Get component count
            total_components = Component.objects.count()
            self.stdout.write(f"Total components in database: {total_components:,}")
            
            # Get unique CMU ID count
            cmu_count = Component.objects.values('cmu_id').distinct().count()
            self.stdout.write(f"Unique CMU IDs: {cmu_count:,}")
            
            # Get unique company count 
            company_count = Component.objects.values('company_name').distinct().count()
            self.stdout.write(f"Unique companies: {company_count:,}")
            
            # Get database size
            with connection.cursor() as cursor:
                try:
                    cursor.execute("SELECT pg_database_size(current_database());")
                    db_size = cursor.fetchone()[0]
                    # Convert to MB or GB
                    if db_size > 1024 * 1024 * 1024:
                        self.stdout.write(f"Database size: {db_size / (1024 * 1024 * 1024):.2f} GB")
                    else:
                        self.stdout.write(f"Database size: {db_size / (1024 * 1024):.2f} MB")
                except Exception as db_error:
                    self.stdout.write(self.style.WARNING(f"Could not determine database size: {db_error}"))
            
            # Show table statistics
            with connection.cursor() as cursor:
                try:
                    cursor.execute("""
                        SELECT relname, n_live_tup, pg_size_pretty(pg_total_relation_size(C.oid))
                        FROM pg_class C
                        LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
                        WHERE nspname NOT IN ('pg_catalog', 'information_schema')
                        AND relkind='r'
                        ORDER BY n_live_tup DESC;
                    """)
                    tables = cursor.fetchall()
                    
                    self.stdout.write("\nTable Statistics:")
                    self.stdout.write(f"{'Table Name':<30} {'Rows':<10} {'Size':<10}")
                    self.stdout.write("-" * 50)
                    
                    for table in tables:
                        self.stdout.write(f"{table[0]:<30} {table[1]:<10,} {table[2]:<10}")
                except Exception as table_error:
                    self.stdout.write(self.style.WARNING(f"Could not retrieve table statistics: {table_error}"))
                    
            # Top companies by component count
            top_companies = Component.objects.values('company_name') \
                                   .annotate(count=Count('id')) \
                                   .order_by('-count')[:top_n]
            
            self.stdout.write(f"\nTop {top_n} Companies by Component Count:")
            self.stdout.write(f"{'Company Name':<40} {'Component Count':<15}")
            self.stdout.write("-" * 55)
            
            for company in top_companies:
                if company['company_name']:
                    self.stdout.write(f"{company['company_name'][:40]:<40} {company['count']:<15,}")
                    
            # Show technology distribution
            tech_distribution = Component.objects.values('technology') \
                                        .annotate(count=Count('id')) \
                                        .order_by('-count')[:10]
            
            self.stdout.write(f"\nTechnology Distribution (Top 10):")
            self.stdout.write(f"{'Technology':<40} {'Component Count':<15}")
            self.stdout.write("-" * 55)
            
            for tech in tech_distribution:
                if tech['technology']:
                    self.stdout.write(f"{tech['technology'][:40]:<40} {tech['count']:<15,}")
                    
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error retrieving database statistics: {e}"))
            
        # Show JSON statistics if requested
        if show_json_stats:
            self.stdout.write("\n" + "=" * 50)
            self.stdout.write(self.style.SUCCESS("JSON File Statistics (for comparison)"))
            self.stdout.write("=" * 50)
            
            try:
                json_dir = os.path.join(settings.BASE_DIR, 'json_data')
                if not os.path.exists(json_dir):
                    self.stdout.write(self.style.WARNING(f"JSON directory not found: {json_dir}"))
                    return
                
                # Get list of JSON files
                json_pattern = os.path.join(json_dir, 'components_*.json')
                json_files = glob.glob(json_pattern)
                
                total_size = 0
                total_cmu_ids = 0
                total_components = 0
                companies = {}
                technologies = {}
                
                self.stdout.write(f"Found {len(json_files)} JSON files")
                
                for json_file in json_files:
                    try:
                        # Get file size
                        file_size = os.path.getsize(json_file)
                        total_size += file_size
                        
                        # Load and parse JSON
                        with open(json_file, 'r') as f:
                            json_data = json.load(f)
                            
                        # Count CMU IDs and components
                        cmu_count = len(json_data)
                        total_cmu_ids += cmu_count
                        
                        comp_count = 0
                        for cmu_id, components in json_data.items():
                            comp_count += len(components)
                            
                            # Count companies
                            company_name = None
                            for comp in components:
                                if isinstance(comp, dict) and "Company Name" in comp:
                                    company_name = comp["Company Name"]
                                    break
                                    
                            if company_name:
                                companies[company_name] = companies.get(company_name, 0) + len(components)
                                
                            # Count technologies
                            for comp in components:
                                if isinstance(comp, dict) and "Generating Technology Class" in comp:
                                    tech = comp["Generating Technology Class"]
                                    if tech:
                                        technologies[tech] = technologies.get(tech, 0) + 1
                            
                        total_components += comp_count
                        
                    except Exception as e:
                        self.stdout.write(self.style.WARNING(f"Error processing {json_file}: {e}"))
                
                # Display statistics
                if total_size > 1024 * 1024 * 1024:
                    self.stdout.write(f"Total JSON size: {total_size / (1024 * 1024 * 1024):.2f} GB")
                else:
                    self.stdout.write(f"Total JSON size: {total_size / (1024 * 1024):.2f} MB")
                
                self.stdout.write(f"Total CMU IDs in JSON: {total_cmu_ids:,}")
                self.stdout.write(f"Total components in JSON: {total_components:,}")
                self.stdout.write(f"Unique companies in JSON: {len(companies):,}")
                
                # Display top companies
                top_json_companies = sorted(companies.items(), key=lambda x: x[1], reverse=True)[:top_n]
                
                self.stdout.write(f"\nTop {top_n} Companies in JSON Files:")
                self.stdout.write(f"{'Company Name':<40} {'Component Count':<15}")
                self.stdout.write("-" * 55)
                
                for company, count in top_json_companies:
                    if company:
                        self.stdout.write(f"{company[:40]:<40} {count:<15,}")
                
                # Display technology distribution
                top_json_tech = sorted(technologies.items(), key=lambda x: x[1], reverse=True)[:10]
                
                self.stdout.write(f"\nTechnology Distribution in JSON Files (Top 10):")
                self.stdout.write(f"{'Technology':<40} {'Component Count':<15}")
                self.stdout.write("-" * 55)
                
                for tech, count in top_json_tech:
                    if tech:
                        self.stdout.write(f"{tech[:40]:<40} {count:<15,}")
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error retrieving JSON statistics: {e}"))