import csv
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db.models import Q
from checker.models import Component


class Command(BaseCommand):
    help = 'Export Flexitricity components with load curtailment to CSV'

    def add_arguments(self, parser):
        parser.add_argument('--output', type=str, help='Output CSV filename')
        parser.add_argument('--all', action='store_true', help='Export all Flexitricity components')

    def handle(self, *args, **options):
        output_file = options.get('output')
        export_all = options.get('all', False)

        if export_all:
            self.stdout.write('Exporting ALL Flexitricity components...')
            components = self.extract_all_flexitricity_components()
        else:
            self.stdout.write('Searching for Flexitricity components with load curtailment...')
            components = self.extract_flexitricity_load_components()
            
            # If no specific matches found, try getting all components
            if not components:
                self.stdout.write('No specific components found. Falling back to ALL Flexitricity components...')
                components = self.extract_all_flexitricity_components()

        if components:
            self.save_to_csv(components, output_file)
        else:
            self.stdout.write(self.style.ERROR('No components found to export'))

    def extract_flexitricity_load_components(self):
        """
        Extract all components for Flexitricity with load curtailment descriptions.
        """
        # Define possible company name variations
        company_variations = [
            'FLEXITRICITY LIMITED', 
            'Flexitricity Limited',
            'Flexitricity',
            'FLEXITRICITY'
        ]
        
        # Define possible description terms
        description_terms = [
            'load drop',
            'load curtailment',
            'load reduction',
            'demand reduction',
            'demand response',
            'load shifting',
            'DSR'
        ]
        
        # Build company filter
        company_filter = Q()
        for company in company_variations:
            company_filter |= Q(company_name__icontains=company)
        
        # Build description filter
        description_filter = Q()
        for term in description_terms:
            description_filter |= Q(description__icontains=term)
        
        # Query the database
        components = Component.objects.filter(company_filter).filter(description_filter).order_by('location')
        
        count = components.count()
        self.stdout.write(f"Found {count} matching components")
        
        if count == 0:
            self.stdout.write("No matching components found. Checking with broader criteria...")
            
            # Check how many components exist for each company variation
            for company in company_variations:
                count = Component.objects.filter(company_name__icontains=company).count()
                self.stdout.write(f"Components for '{company}': {count}")
            
            # Check descriptions across all companies
            for term in description_terms:
                count = Component.objects.filter(description__icontains=term).count()
                self.stdout.write(f"Components with '{term}' in description: {count}")
            
            # Search for any Asda components as a fallback
            asda_components = Component.objects.filter(
                Q(location__icontains='Asda') | 
                Q(description__icontains='Asda')
            ).filter(description_filter).count()
            self.stdout.write(f"Found {asda_components} Asda components with load curtailment terms")
            
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

    def extract_all_flexitricity_components(self):
        """
        Extract all Flexitricity components regardless of description.
        """
        components = Component.objects.filter(
            Q(company_name__icontains='FLEXITRICITY') |
            Q(company_name__icontains='Flexitricity')
        ).order_by('location')
        
        count = components.count()
        self.stdout.write(f"Found {count} total Flexitricity components")
        
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
            filename = f"flexitricity_load_components_{timestamp}.csv"

        # Define the field names based on the first component
        fieldnames = components[0].keys()

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for component in components:
                writer.writerow(component)
        
        self.stdout.write(self.style.SUCCESS(f"Exported {len(components)} components to {filename}"))
        self.stdout.write(f"Full path: {filename}") 