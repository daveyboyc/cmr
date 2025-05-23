"""
Map data caching service for the Capacity Market Registry.

This service provides functions to cache and retrieve map data from Redis,
including pre-clustered markers and viewport-specific data. This reduces
database load and improves map performance.
"""
import json
import time
import hashlib
import logging
from typing import Dict, List, Any, Optional, Tuple
from django.core.cache import cache
from django.conf import settings

# Configure logger
logger = logging.getLogger(__name__)

# Constants for cache keys and expiration
MAP_DATA_KEY_PREFIX = "map_data:"
MAP_DATA_EXPIRATION = 60 * 60 * 24 * 7  # 7 days (in seconds)
MAP_CLUSTER_KEY_PREFIX = "map_cluster:"
MAP_CLUSTER_EXPIRATION = 60 * 60 * 24 * 7  # 7 days
MAP_DETAIL_KEY_PREFIX = "map_detail:"
MAP_DETAIL_EXPIRATION = 60 * 60 * 24 * 7  # 7 days


def generate_map_cache_key(params: Dict[str, Any]) -> str:
    """
    Generates a deterministic cache key for map data based on the provided parameters.
    
    Args:
        params: Dictionary of parameters that define the map view (viewport, filters, etc.)
        
    Returns:
        A cache key string for Redis
    """
    # Sort parameters to ensure consistent ordering
    relevant_params = ['technology', 'north', 'south', 'east', 'west', 
                      'company', 'year', 'cmu_id', 'detail_level', 
                      'exact_technology', 'cm_period', 'zoom']
    
    # Filter to only include present parameters
    filtered_params = {k: params.get(k, '') for k in relevant_params if k in params}
    
    # Create a deterministic string representation
    params_string = json.dumps(sorted(filtered_params.items()))
    params_hash = hashlib.md5(params_string.encode('utf-8')).hexdigest()
    
    return f"{MAP_DATA_KEY_PREFIX}{params_hash}"


def get_cached_map_data(params: Dict[str, Any]) -> Optional[str]:
    """
    Retrieves cached map data for the given parameters.
    
    Args:
        params: Dictionary of parameters defining the map view
        
    Returns:
        JSON string of map data if found, None otherwise
    """
    start_time = time.time()
    cache_key = generate_map_cache_key(params)
    
    # Try to get from Redis cache
    cached_data = cache.get(cache_key)
    
    if cached_data and isinstance(cached_data, str):
        elapsed = time.time() - start_time
        logger.info(f"✅ Cache HIT: Retrieved map data from cache in {elapsed:.4f}s (key: {cache_key})")
        return cached_data
    
    logger.info(f"⚠️ Cache MISS: No cached map data found for key: {cache_key}")
    return None


def cache_map_data(params: Dict[str, Any], data: str) -> None:
    """
    Stores map data in Redis cache.
    
    Args:
        params: Dictionary of parameters defining the map view
        data: JSON string of the map data to cache
    """
    cache_key = generate_map_cache_key(params)
    
    # Store in Redis
    cache.set(cache_key, data, MAP_DATA_EXPIRATION)
    
    logger.info(f"✅ Cached map data with key: {cache_key} (expires in {MAP_DATA_EXPIRATION/3600} hours)")


def generate_cluster_cache_key(zoom_level: int, viewport: Dict[str, float], technology: str = None) -> str:
    """
    Generates a cache key for pre-clustered map data at a specific zoom level and technology.
    
    Args:
        zoom_level: The map zoom level
        viewport: Dictionary with north, south, east, west bounds
        technology: Optional technology filter
        
    Returns:
        Cache key string
    """
    # For clustering, we use broader viewport bounds to ensure coverage
    viewport_str = f"{viewport.get('north'):.2f}_{viewport.get('south'):.2f}_{viewport.get('east'):.2f}_{viewport.get('west'):.2f}"
    tech_suffix = f"_{technology}" if technology else ""
    
    return f"{MAP_CLUSTER_KEY_PREFIX}z{zoom_level}_{viewport_str}{tech_suffix}"


def get_cached_clusters(zoom_level: int, viewport: Dict[str, float], technology: str = None) -> Optional[List[Dict]]:
    """
    Retrieves pre-clustered map data for a specific zoom level.
    
    Args:
        zoom_level: The map zoom level
        viewport: Dictionary with north, south, east, west bounds
        technology: Optional technology filter
        
    Returns:
        List of cluster objects if found, None otherwise
    """
    cache_key = generate_cluster_cache_key(zoom_level, viewport, technology)
    
    # Try to get from cache
    cached_clusters = cache.get(cache_key)
    
    if cached_clusters:
        logger.info(f"✅ Cache HIT: Retrieved pre-clustered map data for zoom {zoom_level}")
        return cached_clusters
    
    logger.info(f"⚠️ Cache MISS: No pre-clustered data found for zoom {zoom_level}")
    return None


def cache_clusters(zoom_level: int, viewport: Dict[str, float], clusters: List[Dict], technology: str = None) -> None:
    """
    Stores pre-clustered map data in the cache.
    
    Args:
        zoom_level: The map zoom level
        viewport: Dictionary with north, south, east, west bounds
        clusters: List of cluster objects to cache
        technology: Optional technology filter
    """
    cache_key = generate_cluster_cache_key(zoom_level, viewport, technology)
    
    # Store in Redis
    cache.set(cache_key, clusters, MAP_CLUSTER_EXPIRATION)
    
    logger.info(f"✅ Cached {len(clusters)} clusters for zoom {zoom_level} (expires in {MAP_CLUSTER_EXPIRATION/3600} hours)")


def get_cached_component_detail(component_id: int) -> Optional[Dict]:
    """
    Retrieves cached component detail data for map popups.
    
    Args:
        component_id: The component ID
        
    Returns:
        Component detail dictionary if found, None otherwise
    """
    cache_key = f"{MAP_DETAIL_KEY_PREFIX}{component_id}"
    
    # Try to get from cache
    cached_detail = cache.get(cache_key)
    
    if cached_detail:
        logger.info(f"✅ Cache HIT: Retrieved component detail for ID: {component_id}")
        return cached_detail
    
    logger.info(f"⚠️ Cache MISS: No component detail found for ID: {component_id}")
    return None


def cache_component_detail(component_id: int, detail_data: Dict) -> None:
    """
    Stores component detail data for map popups in the cache.
    
    Args:
        component_id: The component ID
        detail_data: Dictionary containing component details
    """
    cache_key = f"{MAP_DETAIL_KEY_PREFIX}{component_id}"
    
    # Store in Redis
    cache.set(cache_key, detail_data, MAP_DETAIL_EXPIRATION)
    
    logger.info(f"✅ Cached component detail for ID: {component_id}")

def clear_map_cache() -> int:
    """
    Clears all map-related caches.
    
    Returns:
        Number of cache keys cleared
    """
    # This will vary by cache backend - some don't support pattern matching
    try:
        # Try pattern-based deletion first
        map_data_keys = cache.keys(f"{MAP_DATA_KEY_PREFIX}*")
        map_cluster_keys = cache.keys(f"{MAP_CLUSTER_KEY_PREFIX}*")
        map_detail_keys = cache.keys(f"{MAP_DETAIL_KEY_PREFIX}*")
        
        all_keys = map_data_keys + map_cluster_keys + map_detail_keys
        
        if all_keys:
            cache.delete_many(all_keys)
            logger.info(f"Cleared {len(all_keys)} map cache entries")
            return len(all_keys)
            
    except Exception as e:
        logger.warning(f"Error during pattern-based cache clearing: {e}")
        
    logger.info("Unable to perform pattern-based cache clearing")
    return 0 