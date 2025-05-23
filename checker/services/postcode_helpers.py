import json
import os
import logging
from functools import lru_cache
from django.conf import settings
import requests
from django.core.cache import cache
import re
import time

logger = logging.getLogger(__name__)

# Base URL for the postcodes.io API
POSTCODES_IO_BASE_URL = "https://api.postcodes.io"

def validate_postcode(postcode):
    """Validates a postcode using the postcodes.io API."""
    if not postcode or not isinstance(postcode, str):
        return False
    
    postcode_cleaned = postcode.strip().upper().replace(" ", "")
    cache_key = f"postcode_validation_{postcode_cleaned}"
    is_valid = cache.get(cache_key)
    
    if is_valid is None:
        try:
            response = requests.get(f"{POSTCODES_IO_BASE_URL}/postcodes/{postcode_cleaned}/validate")
            response.raise_for_status() # Raise an exception for bad status codes
            is_valid = response.json().get("result", False)
            cache.set(cache_key, is_valid, timeout=3600 * 24) # Cache for 24 hours
        except requests.exceptions.RequestException as e:
            logger.error(f"API Error validating postcode {postcode}: {e}")
            is_valid = False # Assume invalid on API error
        except json.JSONDecodeError as e:
            logger.error(f"API response decode error validating postcode {postcode}: {e}")
            is_valid = False

    return is_valid

def get_nearest_postcodes(postcode, limit=5, radius=1000):
     """Gets nearest postcodes for a given valid postcode."""
     if not validate_postcode(postcode):
         return []

     postcode_cleaned = postcode.strip().upper().replace(" ", "")
     cache_key = f"nearest_postcodes_{postcode_cleaned}_{limit}_{radius}"
     nearest = cache.get(cache_key)

     if nearest is None:
         try:
             # First, lookup the postcode to get its details (needed if input is just outcode)
             lookup_response = requests.get(f"{POSTCODES_IO_BASE_URL}/postcodes/{postcode_cleaned}")
             lookup_response.raise_for_status()
             postcode_details = lookup_response.json().get("result")

             if not postcode_details:
                 logger.warning(f"Could not find details for postcode: {postcode_cleaned}")
                 return []

             # Now find nearest using the full postcode from lookup if available
             response = requests.get(f"{POSTCODES_IO_BASE_URL}/postcodes/{postcode_cleaned}/nearest", params={'limit': limit, 'radius': radius})
             response.raise_for_status()
             results = response.json().get("result")
             nearest = [pc["postcode"] for pc in results if pc] if results else []
             cache.set(cache_key, nearest, timeout=3600 * 6) # Cache for 6 hours
         except requests.exceptions.RequestException as e:
             logger.error(f"API Error getting nearest postcodes for {postcode}: {e}")
             nearest = []
         except json.JSONDecodeError as e:
             logger.error(f"API response decode error getting nearest postcodes {postcode}: {e}")
             nearest = []
         except Exception as e: # Catch unexpected errors during lookup/nearest
             logger.error(f"Unexpected error getting nearest postcodes for {postcode}: {e}")
             nearest = []
             
     return nearest

def get_outcode_details(outcode):
    """Gets details for an outcode, including potential admin districts."""
    if not outcode or not isinstance(outcode, str):
        return None

    outcode_cleaned = outcode.strip().upper()
    cache_key = f"outcode_details_{outcode_cleaned}"
    details = cache.get(cache_key)

    if details is None:
        try:
            response = requests.get(f"{POSTCODES_IO_BASE_URL}/outcodes/{outcode_cleaned}")
            response.raise_for_status()
            result_data = response.json().get("result")
            # Extract relevant details, e.g., admin district or region
            if result_data:
                 details = {
                     "admin_district": result_data.get("admin_district", []),
                     "parliamentary_constituency": result_data.get("parliamentary_constituency", []),
                     "region": result_data.get("region") # Added region
                 }
                 cache.set(cache_key, details, timeout=3600 * 24) # Cache for 24 hours
            else:
                 details = {} # No details found
                 cache.set(cache_key, details, timeout=3600) # Cache negative result shorter

        except requests.exceptions.RequestException as e:
            if e.response is not None and e.response.status_code == 404:
                 logger.debug(f"Outcode {outcode_cleaned} not found via API.")
                 details = {} # Treat 404 as not found
                 cache.set(cache_key, details, timeout=3600) 
            else:
                 logger.error(f"API Error getting details for outcode {outcode_cleaned}: {e}")
                 details = None # Indicate error
        except json.JSONDecodeError as e:
            logger.error(f"API response decode error getting outcode details {outcode_cleaned}: {e}")
            details = None

    return details

def get_location_to_postcodes_mapping(force_rebuild=False):
    """
    Dynamically builds a mapping of locations to postcodes from the database.
    Uses Redis caching with a long TTL for efficiency.
    Further optimized with intermediate caching and more aggressive filtering.
    
    Args:
        force_rebuild (bool): If True, force a complete rebuild from the database
        
    Returns:
        dict: Mapping of location names to lists of postcode outcodes
    """
    # Cache key for the mapping
    CACHE_KEY = "location_to_postcodes_mapping"
    CACHE_VERSION = "1"  # Increment this when the mapping structure changes
    CACHE_TTL = None  # None = no expiration (permanent)
    MAX_BUILD_TIME = 5.0  # Maximum seconds to spend building the mapping

    # STEP 1: Check for permanent cache from build_location_mapping command
    permanent_mapping = cache.get(CACHE_KEY, version=CACHE_VERSION)
    
    if permanent_mapping and not force_rebuild:
        logger.info(f"Using pre-built location mapping from Redis cache ({len(permanent_mapping)} locations)")
        return permanent_mapping
    
    # If we reach here, either the permanent cache doesn't exist or we're forcing a rebuild
    if force_rebuild:
        logger.info("Forced rebuild of location-to-postcodes mapping")
    else:
        logger.info("Building location-to-postcodes mapping from scratch")
        logger.info("Consider running 'python manage.py build_location_mapping' to build a complete mapping without timeout")

    # STEP 2: Build mapping from scratch with timeout protection
    start_time = time.time()
    mapping = {}
    processed = 0
    total = 0
    
    try:
        # Get all unique locations from database
        locations = get_all_unique_locations()
        total = len(locations)
        
        # Start building mapping with timeout protection
        for location_name in locations:
            # Skip empty locations
            if not location_name or location_name == "":
                continue
                
            # Check for timeout
            if time.time() - start_time > MAX_BUILD_TIME:
                logger.warning(f"Location mapping build reached timeout ({MAX_BUILD_TIME}s). Processed {processed}/{total} locations. Returning partial mapping with {len(mapping)} entries.")
                break
                
            # Get postcodes for this location and add to mapping
            postcodes = get_postcodes_for_location(location_name)
            if postcodes:
                mapping[location_name] = postcodes
                
            processed += 1
            
        # Only cache result if we've got a significant number of mappings or processed all
        if (len(mapping) > 0 and processed == total) or len(mapping) > 50:
            # Use a temporary TTL cache to avoid holding up the request for too long
            logger.info(f"Built locations cache with {len(mapping)} locations (took {time.time() - start_time:.3f}s)")
            # Set a TTL cache
            cache.set(CACHE_KEY, mapping, timeout=3600*24, version=CACHE_VERSION)  # 24 hour cache
        
        logger.info(f"Successfully built location mapping with {len(mapping)} locations")
        return mapping
        
    except Exception as e:
        logger.error(f"Error building location mapping: {str(e)}")
        # Return an empty mapping in case of error
        return {}

def startup_check_redis_mapping():
    """
    Check if the Redis location mapping exists at startup and log the status.
    This provides a clear indication of whether we have a pre-built mapping.
    """
    CACHE_KEY = "location_to_postcodes_mapping"
    CACHE_VERSION = "1"
    
    mapping = cache.get(CACHE_KEY, version=CACHE_VERSION)
    
    if mapping:
        location_count = len(mapping)
        logger.info(f"✅ REDIS LOCATION MAPPING LOADED: Found pre-built mapping with {location_count} locations")
        logger.info(f"✅ Location lookups will use Redis cache and NOT timeout or rebuild each time")
        
        # Output a sample of the mapping to confirm it's working
        if location_count > 0:
            sample_location = list(mapping.keys())[0]
            sample_postcodes = mapping[sample_location]
            logger.info(f"✅ Sample mapping - Location: '{sample_location}' → Postcodes: {sample_postcodes}")
            
        return True
    else:
        logger.warning("❌ NO REDIS LOCATION MAPPING FOUND: Run 'python manage.py build_location_mapping' to create one")
        logger.warning("❌ Location lookups will continue to timeout and build partial mappings until fixed")
        return False

def add_location_to_mapping(location, outcodes):
    """
    Add a location and its associated outcodes to the mapping.
    
    Args:
        location (str): The location name
        outcodes (list): List of outcodes associated with the location
        
    Returns:
        bool: True if added successfully, False otherwise
    """
    if not location or not outcodes:
        return False
        
    location = location.lower().strip()
    
    # Cache key for the mapping
    CACHE_KEY = "location_to_postcodes_mapping"
    CACHE_VERSION = "1"  # Must match version in get_location_to_postcodes_mapping
    CACHE_TTL = 3600 * 24  # 24 hours
    
    # Get current mapping from cache or rebuild if needed
    mapping = get_location_to_postcodes_mapping()
    
    # Add or update the mapping
    mapping[location] = [outcode.upper() for outcode in outcodes if outcode]
    
    # Update the cache
    cache.set(CACHE_KEY, mapping, timeout=CACHE_TTL, version=CACHE_VERSION)
    
    # Clear related Django cache entries
    cache_key = f"area_postcodes_{location.replace(' ', '_')}"
    cache.delete(cache_key)
    
    logger.info(f"Added location to mapping: {location} -> {mapping[location]}")
    
    return location in mapping

def get_all_postcodes_for_area(area_name):
    """
    Gets postcode prefixes associated with a given area name.
    Uses both database-driven mappings and database queries to find relevant outcodes.
    Heavily optimized with multi-level caching.
    """
    if not area_name or not isinstance(area_name, str):
        return []
        
    # Clean up the area name - replace spaces with underscores in cache key
    area_name_lower = area_name.lower().strip()
    cache_key = f"area_postcodes_{area_name_lower.replace(' ', '_')}"
    
    # Check cache first (with longer timeout now - 48 hours)
    cached_postcodes = cache.get(cache_key)
    if cached_postcodes is not None:
        logger.debug(f"Cache HIT for area postcodes: {area_name_lower}")
        return cached_postcodes
    
    logger.info(f"Cache MISS for area postcodes: {area_name_lower}")
    
    # OPTIMIZATION: Check if this term appears in many different locations
    # If it does, it's likely a business name or generic term, not a location
    try:
        from ..models import Component
        # Count distinct outward codes where location contains this term
        outcode_count = Component.objects.filter(
            location__icontains=area_name_lower
        ).exclude(
            outward_code=''
        ).values('outward_code').distinct().count()
        
        # If the term appears in many postcodes, it's likely NOT a location but a business/chain
        # For real locations, we expect a smaller number of distinct outcodes
        if outcode_count > 4:  # Threshold - if it appears in >4 different post codes areas, probably not a location
            logger.info(f"Term '{area_name}' appears in {outcode_count} different postcode areas - likely a business name or generic term, not a location")
            cache.set(cache_key, [], timeout=3600*24)  # Cache empty results for 24 hours
            return []
    except Exception as e:
        logger.error(f"Error checking location type for '{area_name}': {e}")
    
    # STEP 1: First check the global mapping cache - this should be very fast
    # This avoids rebuilding the entire mapping on every request
    location_to_postcodes = cache.get("location_to_postcodes_mapping", version="1")
    
    if not location_to_postcodes:
        # Only rebuild if not in cache - this is a heavy operation
        location_to_postcodes = get_location_to_postcodes_mapping()
    
    # Direct match first
    if area_name_lower in location_to_postcodes:
        postcodes = location_to_postcodes[area_name_lower]
        logger.info(f"Using database mapping for exact match '{area_name_lower}': {postcodes}")
        # Cache for 48 hours now (extended from 24)
        cache.set(cache_key, postcodes, timeout=3600*48)
        return postcodes
    
    # Check for known locations within the search term
    # For example, "nottingham hospital" should use postcodes for "nottingham"
    for known_location, postcodes in location_to_postcodes.items():
        # Check if the known location is a word within the search term
        if re.search(r'\b' + re.escape(known_location) + r'\b', area_name_lower):
            logger.info(f"Found known location '{known_location}' within '{area_name_lower}', using its postcodes: {postcodes}")
            cache.set(cache_key, postcodes, timeout=3600*48)  # Cache for 48 hours
            return postcodes
    
    # STEP 2: Check if area_name is already an outcode itself (very fast check)
    outcode = area_name.strip().upper()
    if re.match(r'^[A-Z]{1,2}[0-9]{0,2}$', outcode):  # Basic regex for outcodes
        logger.debug(f"'{area_name}' appears to be an outcode itself")
        cache.set(cache_key, [outcode], timeout=3600*48)  # Cache for 48 hours
        return [outcode]
    
    # STEP 3: Query the database to find any components with this location
    # This is more expensive but we've exhausted the cache options
    try:
        from ..models import Component
        from django.db.models import Q
        
        # Use a more optimized query with exact index matches where possible
        components = Component.objects.filter(
            Q(location__iexact=area_name) | 
            Q(county__iexact=area_name) |
            Q(location__istartswith=area_name) |
            Q(county__istartswith=area_name)
        ).exclude(
            outward_code=''
        ).values_list(
            'outward_code', flat=True
        ).distinct()[:50]  # Limit to 50 results for performance
        
        found_outcodes = list(set([code.upper() for code in components if code]))
        
        if found_outcodes:
            logger.info(f"Found {len(found_outcodes)} outcodes in database for '{area_name}': {found_outcodes}")
            cache.set(cache_key, found_outcodes, timeout=3600*48)  # Cache for 48 hours
            return found_outcodes
            
        # If no exact/prefix matches, try contains as last resort
        # But be stricter with the filters to avoid too many results
        if len(area_name) >= 4:  # Only do contains search for longer terms
            components = Component.objects.filter(
                Q(location__icontains=area_name) | Q(county__icontains=area_name)
            ).exclude(
                outward_code=''
            ).values_list(
                'outward_code', flat=True
            ).distinct()[:30]  # Limit further for contains searches
            
            found_outcodes = list(set([code.upper() for code in components if code]))
            
            if found_outcodes:
                logger.info(f"Found {len(found_outcodes)} outcodes in database with contains search for '{area_name}': {found_outcodes}")
                cache.set(cache_key, found_outcodes, timeout=3600*48)  # Cache for 48 hours
                return found_outcodes
    except Exception as e:
        logger.error(f"Error querying database for outcodes related to '{area_name}': {e}")
    
    # Nothing found - cache empty result with shorter timeout
    logger.warning(f"No postcodes found for area '{area_name}' in any source")
    cache.set(cache_key, [], timeout=3600*6)  # Cache empty results for 6 hours
    return []

def get_area_for_any_postcode(postcode):
    """
    Determine the area/location for any postcode (or outward code).
    
    Args:
        postcode (str): Postcode or outward code to lookup
        
    Returns:
        str: Area/location name, or None if not found
    """
    if not postcode:
        return None

    # Clean and standardize the postcode format
    # For our purposes, we focus on the outward code (first part of postcode)
    outward = None
    
    # First try to extract the outward code
    if " " in postcode:
        outward = postcode.split(" ")[0].strip().upper()
    else:
        # Try to identify the outward code using standard UK postcode patterns
        postcode = postcode.strip().upper()
        
        # Handle full postcodes without spaces (e.g., SW1A1AA)
        if len(postcode) >= 5:  # Full UK postcodes are at least 5 chars
            if len(postcode) == 5:  # e.g., W1A1A
                outward = postcode[:2]
            elif len(postcode) == 6:  # e.g., SW1A1A
                outward = postcode[:3]
            elif len(postcode) == 7:  # e.g., SW1A1AA
                outward = postcode[:4] if postcode[3].isdigit() else postcode[:3]
            else:  # e.g., SW1A1AAA (invalid but handle it)
                outward = postcode[:4] if postcode[3].isdigit() else postcode[:3]
        else:
            # It's likely just an outward code already
            outward = postcode
    
    if not outward:
        return None
    
    # STEP 1: First check the global mapping cache - this should be very fast
    # This avoids rebuilding the entire mapping on every request
    location_to_postcodes = cache.get("location_to_postcodes_mapping", version="1")
    
    if not location_to_postcodes:
        # Only rebuild if not in cache - this is a heavy operation
        location_to_postcodes = get_location_to_postcodes_mapping()
    
    if location_to_postcodes:
        # Check if outward is in any of the postcode lists
        for location, postcodes in location_to_postcodes.items():
            # First try exact match
            if outward in postcodes:
                return location
            
            # Then try matching partial outward codes (e.g., "SW" in "SW1")
            for postcode_prefix in postcodes:
                if outward.startswith(postcode_prefix):
                    return location
    
    # STEP 2: If no match found, search for outward code in database
    try:
        from ..models import Component
        outward_search = outward.replace("'", "''")  # Sanitize against SQL injection for LIKE
        
        matching_locations = Component.objects.filter(
            outward_code__iexact=outward
        ).values_list('location', flat=True).distinct()[:5]
        
        if matching_locations.exists():
            # Use most common location (first is fine for now)
            location = matching_locations[0]
            # Extract place name (first part)
            parts = re.split(r'[,\n]', location)
            place_name = parts[0].strip().lower() if parts else None
            
            # Add to mapping for future lookups
            if place_name and len(place_name) >= 3:
                add_location_to_mapping(place_name, [outward])
                
            return place_name
    
    except Exception as e:
        logger.error(f"Error searching for postcode area: {e}")
    
    # No match found
    return None

# Function to refresh the postcodes mapping from database
def refresh_postcode_mapping():
    """Force a refresh of the location-to-postcodes mapping from database"""
    # Cache key for the mapping
    CACHE_KEY = "location_to_postcodes_mapping"
    CACHE_VERSION = "1"  # Must match version in get_location_to_postcodes_mapping
    
    # Clear the cache
    cache.delete(CACHE_KEY, version=CACHE_VERSION)
    
    # Clear all area postcode caches
    cache.delete_pattern("area_postcodes_*") if hasattr(cache, 'delete_pattern') else None
    
    # Regenerate the mapping from database
    mapping = get_location_to_postcodes_mapping(force_rebuild=True)
    
    # Return the size of the new mapping
    return len(mapping)

def get_all_unique_locations():
    """
    Get all unique locations from the database.
    Uses settings cache if available for better performance.
    """
    try:
        from django.conf import settings
        from checker.models import Component
        from django.db.models import Q, Count
        
        # Try to use cached locations
        if hasattr(settings, 'CACHED_UNIQUE_LOCATIONS'):
            logger.info(f"Using cached unique locations ({len(settings.CACHED_UNIQUE_LOCATIONS)} locations)")
            return [loc['location'] for loc in settings.CACHED_UNIQUE_LOCATIONS if loc.get('location')]
        
        # Otherwise, query the database
        locations = Component.objects.values('location').exclude(
            Q(location='') | Q(location__isnull=True)
        ).annotate(
            count=Count('id')  # Count actual components for each location
        ).distinct()
        
        # Convert to a list
        locations_list = list(locations)
        
        # Store in settings for reuse within this process
        settings.CACHED_UNIQUE_LOCATIONS = locations_list
        
        return [loc['location'] for loc in locations_list if loc.get('location')]
    except Exception as e:
        logger.error(f"Error getting unique locations: {str(e)}")
        return []

def get_postcodes_for_location(location_name):
    """
    Get all postcodes associated with a specific location.
    
    Args:
        location_name (str): Name of the location
        
    Returns:
        list: List of outward postcodes for the location
    """
    # Start with hardcoded mappings for common locations
    hardcoded_mappings = {
        "nottingham": ["NG1", "NG2", "NG3", "NG4", "NG5", "NG6", "NG7", "NG8", "NG9",
                      "NG10", "NG11", "NG12", "NG14", "NG15", "NG16", "NG17", "NG18"],
        "london": ["SW", "SE", "W", "E", "N", "NW", "EC", "WC"],
        "manchester": ["M1", "M2", "M3", "M4", "M8", "M11", "M12", "M13", "M14", "M15", "M16"],
        "birmingham": ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "B10"],
        "clapham": ["SW4", "SW11"],
        "battersea": ["SW11", "SW8"],
        "peckham": ["SE15", "SE5"],
        "sheffield": ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10", "S11"],
        "liverpool": ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "L10"],
        "bristol": ["BS1", "BS2", "BS3", "BS4", "BS5", "BS6", "BS7", "BS8", "BS9"],
        "glasgow": ["G1", "G2", "G3", "G4", "G5", "G11", "G12", "G13", "G14"],
        "edinburgh": ["EH1", "EH2", "EH3", "EH4", "EH5", "EH6", "EH7", "EH8", "EH9"],
        "leeds": ["LS1", "LS2", "LS3", "LS4", "LS5", "LS6", "LS7", "LS8", "LS9"],
        "cardiff": ["CF1", "CF2", "CF3", "CF4", "CF5", "CF10", "CF11"],
        "belfast": ["BT1", "BT2", "BT3", "BT4", "BT5", "BT6", "BT7", "BT8", "BT9"],
    }
    
    # Check if we have a hardcoded mapping
    if location_name.lower() in hardcoded_mappings:
        return hardcoded_mappings[location_name.lower()]
    
    try:
        from checker.models import Component
        import re
        
        # Extract main place name (first part before comma)
        parts = re.split(r'[,\n]', location_name)
        place_name = parts[0].strip().lower() if parts else None
        
        if not place_name or len(place_name) < 3:
            return []
            
        # Find all components with this place name in their location
        # and get their outward codes
        components = Component.objects.filter(
            location__istartswith=place_name
        ).exclude(
            outward_code=''
        ).values_list('outward_code', flat=True).distinct()
        
        # Return outward codes if found
        outward_codes = list(set([code.upper() for code in components if code]))
        return outward_codes
        
    except Exception as e:
        logger.error(f"Error getting postcodes for location '{location_name}': {str(e)}")
        return []

# Add __init__.py if it doesn't exist (important for services to be a package)
# This should ideally be done separately if needed, but let's assume it exists for now. 