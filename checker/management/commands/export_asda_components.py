import csv
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db.models import Q
from checker.models import Component


class Command(BaseCommand):
    help = 'Export Asda components with load curtailment to CSV'

    def add_arguments(self, parser):
        parser.add_argument('--output', type=str, help='Output CSV filename')
        parser.add_argument('--all', action='store_true', help='Export all Asda components')

    def handle(self, *args, **options):
        output_file = options.get('output')
        export_all = options.get('all', False)

        if export_all:
            self.stdout.write('Exporting ALL Asda components...')
            components = self.extract_all_asda_components()
        else:
            self.stdout.write('Searching for Asda components with load curtailment...')
            components = self.extract_asda_load_components()
            
            # If no specific matches found, try getting all components
            if not components:
                self.stdout.write('No specific components found. Falling back to ALL Asda components...')
                components = self.extract_all_asda_components()

        if components:
            self.save_to_csv(components, output_file)
        else:
            self.stdout.write(self.style.ERROR('No components found to export'))

    def extract_asda_load_components(self):
        """
        Extract all components related to Asda stores with load curtailment/reduction.
        """
        # Build filter for Asda-related components
        asda_filter = (
            Q(location__icontains='Asda') | 
            Q(description__icontains='Asda') |
            Q(description__icontains='ASDA')
        )
        
        # Build filter for load curtailment-related descriptions
        description_filter = (
            Q(description__icontains='load drop') |
            Q(description__icontains='load curtailment') |
            Q(description__icontains='load reduction') |
            Q(description__icontains='demand reduction') |
            Q(description__icontains='demand response') |
            Q(description__icontains='DSR')
        )
        
        # Query the database for Asda components with load curtailment
        components = Component.objects.filter(asda_filter).filter(description_filter).order_by('location')
        
        count = components.count()
        self.stdout.write(f"Found {count} Asda components with load curtailment")
        
        if count == 0:
            self.stdout.write("No matching components found. Checking with broader criteria...")
            
            # Try just Asda components
            asda_only_count = Component.objects.filter(asda_filter).count()
            self.stdout.write(f"Total Asda components: {asda_only_count}")
            
            # Try specific company search
            for company in ['FLEXITRICITY LIMITED', 'OCTOPUS ENERGY LIMITED']:
                asda_count = Component.objects.filter(asda_filter).filter(company_name__icontains=company).count()
                self.stdout.write(f"Asda components for {company}: {asda_count}")
            
            # Try to find all components with load curtailment terms
            curtailment_count = Component.objects.filter(description_filter).count()
            self.stdout.write(f"Total components with load curtailment terms: {curtailment_count}")
            
            return []
        
        result = []
        for comp in components:
            result.append({
                'location': comp.location,
                'description': comp.description,
                'cmu_id': comp.cmu_id,
                'company_name': comp.company_name,
                'delivery_year': comp.delivery_year,
                'auction_name': comp.auction_name,
                'derated_capacity': comp.derated_capacity_mw if hasattr(comp, 'derated_capacity_mw') else None,
                'type': comp.type,
                'technology': comp.technology,
                'component_id': comp.id,
                'status': comp.status if hasattr(comp, 'status') else None,
            })
        
        return result

    def extract_all_asda_components(self):
        """
        Extract all Asda-related components regardless of description.
        """
        asda_filter = (
            Q(location__icontains='Asda') | 
            Q(description__icontains='Asda') |
            Q(description__icontains='ASDA')
        )
        
        components = Component.objects.filter(asda_filter).order_by('location')
        
        count = components.count()
        self.stdout.write(f"Found {count} total Asda components")
        
        if count == 0:
            return []
        
        result = []
        for comp in components:
            result.append({
                'location': comp.location,
                'description': comp.description,
                'cmu_id': comp.cmu_id,
                'company_name': comp.company_name,
                'delivery_year': comp.delivery_year,
                'auction_name': comp.auction_name,
                'derated_capacity': comp.derated_capacity_mw if hasattr(comp, 'derated_capacity_mw') else None,
                'type': comp.type,
                'technology': comp.technology,
                'component_id': comp.id,
                'status': comp.status if hasattr(comp, 'status') else None,
            })
        
        return result

    def save_to_csv(self, components, filename=None):
        """Save the components to a CSV file."""
        if not components:
            self.stdout.write("No components to export.")
            return

        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"asda_load_components_{timestamp}.csv"

        # Define the field names based on the first component
        fieldnames = components[0].keys()

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for component in components:
                writer.writerow(component)
        
        self.stdout.write(self.style.SUCCESS(f"Exported {len(components)} components to {filename}"))
        self.stdout.write(f"Full path: {filename}") 