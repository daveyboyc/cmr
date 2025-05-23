"""
Management command to build map cache for common viewports.

This command pre-generates and caches map data for commonly used viewports
and zoom levels, reducing load on the database and improving map performance.
"""
from django.core.management.base import BaseCommand
import json
import time
import logging
from django.db.models import Q, Count, Max
from checker.models import Component
from checker.views import get_simplified_technology
from checker.services.map_cache import (
    cache_map_data, 
    cache_clusters, 
    cache_component_detail, 
    MAP_DATA_EXPIRATION
)

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Pre-generates and caches map data for common viewports'

    def add_arguments(self, parser):
        parser.add_argument(
            '--full',
            action='store_true',
            help='Build the complete map cache (slower but more comprehensive)',
        )

    def handle(self, *args, **options):
        start_time = time.time()
        full_build = options.get('full', False)
        
        self.stdout.write(self.style.SUCCESS(
            f'Starting map cache generation ({"full" if full_build else "quick"} build)'
        ))

        # Cache common UK viewports
        uk_viewport = {
            'north': 58.7,  # Northern Scotland
            'south': 50.0,  # Southern England
            'east': 1.8,    # Eastern England
            'west': -8.2    # Western Ireland
        }
        
        # Narrower England-focused viewport
        england_viewport = {
            'north': 55.0,  # Northern England
            'south': 50.0,  # Southern England
            'east': 1.8,    # Eastern England
            'west': -5.7    # Western England
        }

        # Cache common technology types
        technology_types = [
            'Gas', 
            'Wind', 
            'Solar', 
            'Battery', 
            'Nuclear', 
            'DSR', 
            'Biomass', 
            'Interconnector'
        ]
        
        # Cache common zoom levels
        zoom_levels = [5, 6, 7, 8, 9, 10] if full_build else [6, 8]

        # Get common companies
        top_companies = Component.objects.exclude(company_name__isnull=True)\
                               .exclude(company_name='')\
                               .values('company_name')\
                               .annotate(count=Count('id'))\
                               .order_by('-count')[:5]
        
        # Extract just the company names
        top_company_names = [company['company_name'] for company in top_companies]
        
        self.stdout.write(f'Caching map data for {len(technology_types)} technologies, {len(zoom_levels)} zoom levels')
        
        # Step 1: Cache map data for each technology
        for tech in technology_types:
            self.cache_technology_data(tech, uk_viewport)
        
        # Step 2: Cache clustered data for different zoom levels
        for zoom in zoom_levels:
            for tech in technology_types:
                self.cache_zoom_clusters(tech, zoom, uk_viewport)
            
            # Also cache clusters for England viewport at higher zoom levels
            if zoom >= 7:
                for tech in technology_types:
                    self.cache_zoom_clusters(tech, zoom, england_viewport)
        
        # Step 3: Cache component details for popular components
        if full_build:
            self.cache_common_component_details()
        
        # Step 4: Cache data for top companies
        if full_build:
            for company in top_company_names:
                self.cache_company_data(company, uk_viewport)
        
        elapsed_time = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(
            f'Map cache generation completed in {elapsed_time:.2f}s'
        ))
    
    def cache_technology_data(self, technology, viewport):
        """Cache map data for a specific technology."""
        # Prepare parameters for the cache
        params = {
            'technology': technology,
            'north': viewport['north'],
            'south': viewport['south'], 
            'east': viewport['east'],
            'west': viewport['west'],
            'detail_level': 'minimal'
        }
        
        # Get components for this technology
        start_time = time.time()
        components = self.get_components_for_technology(technology, viewport)
        
        if not components:
            self.stdout.write(f'No components found for technology: {technology}')
            return
        
        # Convert to GeoJSON
        features = self.components_to_features(components)
        
        # Create the GeoJSON response
        geojson = {
            'type': 'FeatureCollection',
            'features': features,
            'metadata': {
                'count': len(features),
                'total': Component.objects.filter(geocoded=True).count(),
                'filtered': True,
                'technology': technology,
                'cached_at': time.time(),
                'expires_at': time.time() + MAP_DATA_EXPIRATION
            }
        }
        
        # Cache the data
        json_str = json.dumps(geojson)
        cache_map_data(params, json_str)
        
        elapsed = time.time() - start_time
        self.stdout.write(
            f'Cached {len(features)} components for {technology} in {elapsed:.2f}s'
        )
    
    def cache_zoom_clusters(self, technology, zoom_level, viewport):
        """Cache clustered data for a specific zoom level."""
        start_time = time.time()
        
        # Get components for this technology
        components = self.get_components_for_technology(technology, viewport)
        
        if not components:
            self.stdout.write(f'No components to cluster for technology: {technology}')
            return
        
        # Create clusters based on zoom level
        clusters = self.cluster_components(components, zoom_level)
        
        # Cache the clusters
        cache_clusters(zoom_level, viewport, clusters, technology)
        
        elapsed = time.time() - start_time
        self.stdout.write(
            f'Cached {len(clusters)} clusters for {technology} at zoom {zoom_level} in {elapsed:.2f}s'
        )
    
    def cache_company_data(self, company, viewport):
        """Cache map data for a specific company."""
        # Prepare parameters for the cache
        params = {
            'company': company,
            'north': viewport['north'],
            'south': viewport['south'], 
            'east': viewport['east'],
            'west': viewport['west'],
            'detail_level': 'minimal'
        }
        
        # Get components for this company
        start_time = time.time()
        components = Component.objects.filter(
            company_name=company,
            geocoded=True,
            latitude__isnull=False, 
            longitude__isnull=False,
            latitude__lte=viewport['north'],
            latitude__gte=viewport['south'],
            longitude__lte=viewport['east'],
            longitude__gte=viewport['west']
        )
        
        if not components:
            self.stdout.write(f'No components found for company: {company}')
            return
        
        # Convert to GeoJSON
        features = self.components_to_features(components)
        
        # Create the GeoJSON response
        geojson = {
            'type': 'FeatureCollection',
            'features': features,
            'metadata': {
                'count': len(features),
                'total': Component.objects.filter(geocoded=True).count(),
                'filtered': True,
                'company': company,
                'cached_at': time.time(),
                'expires_at': time.time() + MAP_DATA_EXPIRATION
            }
        }
        
        # Cache the data
        json_str = json.dumps(geojson)
        cache_map_data(params, json_str)
        
        elapsed = time.time() - start_time
        self.stdout.write(
            f'Cached {len(features)} components for company {company} in {elapsed:.2f}s'
        )
    
    def cache_common_component_details(self):
        """Cache details for commonly viewed components."""
        # Get components in high-interest areas
        components = Component.objects.filter(
            geocoded=True,
            latitude__isnull=False,
            longitude__isnull=False,
        ).order_by('-delivery_year')[:1000]  # Cache 1000 most recent components
        
        start_time = time.time()
        count = 0
        
        for component in components:
            # Prepare component detail data
            detail_data = {
                'success': True,
                'data': {
                    'id': component.id,
                    'title': component.location or 'Unknown Location',
                    'technology': component.technology or 'Unknown',
                    'display_technology': get_simplified_technology(component.technology),
                    'company': component.company_name or 'Unknown',
                    'description': component.description or '',
                    'delivery_year': component.delivery_year or '',
                    'cmu_id': component.cmu_id or '',
                    'detailUrl': f'/component/{component.id}/'
                }
            }
            
            # Cache the component detail
            cache_component_detail(component.id, detail_data)
            count += 1
        
        elapsed = time.time() - start_time
        self.stdout.write(
            f'Cached details for {count} common components in {elapsed:.2f}s'
        )
    
    def get_components_for_technology(self, technology, viewport):
        """Get components for a specific technology within viewport."""
        components = Component.objects.filter(
            geocoded=True,
            latitude__isnull=False,
            longitude__isnull=False,
            latitude__lte=viewport['north'],
            latitude__gte=viewport['south'],
            longitude__lte=viewport['east'],
            longitude__gte=viewport['west']
        )
        
        # Apply technology filter
        if technology:
            # Get all technologies that map to this simplified technology
            matching_techs = []
            for tech_key in Component.objects.values_list('technology', flat=True).distinct():
                if tech_key and get_simplified_technology(tech_key) == technology:
                    matching_techs.append(tech_key)
            
            # Filter components by matching technologies
            if matching_techs:
                components = components.filter(technology__in=matching_techs)
        
        return components
    
    def components_to_features(self, components):
        """Convert components to GeoJSON features."""
        features = []
        
        for comp in components:
            # Create a GeoJSON feature for each component
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [comp.longitude, comp.latitude]
                },
                'properties': {
                    'id': comp.id,
                    'title': comp.location or 'Unknown Location',
                    'technology': comp.technology or 'Unknown',
                    'display_technology': get_simplified_technology(comp.technology),
                    'company': comp.company_name or 'Unknown',
                    'delivery_year': comp.delivery_year or '',
                    'cmu_id': comp.cmu_id or ''
                }
            }
            features.append(feature)
        
        return features
    
    def cluster_components(self, components, zoom_level):
        """
        Create clusters of components based on zoom level.
        
        Simple implementation - in production, you might use a more sophisticated
        clustering algorithm like DBSCAN or a map server like MapBox/Deck.gl
        """
        from django.db.models import Count, Max
        import math
        
        # Define grid size based on zoom level
        # Lower zoom = larger grid cells
        grid_size = 0.5 / (2 ** (zoom_level - 5))  # Example formula
        
        # Group components by grid cells
        clusters = []
        
        # Using Python to perform clustering since the database Round function is problematic
        # Create a dictionary to group components by grid cell
        grid_cells = {}
        
        for comp in components:
            if comp.latitude is not None and comp.longitude is not None:
                # Round to grid cell
                grid_lat = round(comp.latitude / grid_size) * grid_size
                grid_lng = round(comp.longitude / grid_size) * grid_size
                
                # Create a key for this grid cell
                grid_key = f"{grid_lat}:{grid_lng}"
                
                # Add to the appropriate grid cell
                if grid_key not in grid_cells:
                    grid_cells[grid_key] = {
                        'lat': grid_lat,
                        'lng': grid_lng,
                        'count': 0,
                        'technologies': {}
                    }
                
                grid_cells[grid_key]['count'] += 1
                
                # Track technology frequency
                tech = comp.technology or 'Unknown'
                if tech not in grid_cells[grid_key]['technologies']:
                    grid_cells[grid_key]['technologies'][tech] = 0
                grid_cells[grid_key]['technologies'][tech] += 1
        
        # Convert grid cells to clusters
        for grid_key, cell in grid_cells.items():
            # Find the most common technology in this cluster
            main_tech = None
            max_count = 0
            for tech, count in cell['technologies'].items():
                if count > max_count:
                    max_count = count
                    main_tech = tech
            
            # Convert to display_technology here, after database retrieval
            display_tech = get_simplified_technology(main_tech) if main_tech else 'Other'
            
            cluster = {
                'type': 'Cluster',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [cell['lng'], cell['lat']]
                },
                'properties': {
                    'count': cell['count'],
                    'technology': display_tech
                }
            }
            clusters.append(cluster)
        
        return clusters 