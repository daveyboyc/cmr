import json
import os
import logging
from functools import lru_cache
from django.conf import settings
import requests
from django.core.cache import cache

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

def get_all_postcodes_for_area(area_name):
    """
    Gets postcode prefixes associated with a given area name.
    NOTE: postcodes.io doesn't directly map area names to postcodes.
    This function is now less effective and primarily relies on outcode lookups.
    It might return related outcodes based on administrative areas if the area_name matches.
    Consider using geocoding or postcode lookup for better results.
    """
    # This function is difficult to implement reliably with postcodes.io
    # as it doesn't have a direct "area name -> postcodes" endpoint.
    # We could try geocoding the area name to coordinates, then reverse geocode,
    # but that's complex and prone to ambiguity.
    # For now, we'll return an empty list, as the old JSON logic is removed.
    # The search logic should perhaps rely more on direct postcode/outcode matching.
    logger.warning(f"get_all_postcodes_for_area('{area_name}') called - this function has limited effectiveness with the API.")
    
    # --- Attempt using outcode lookup based on area name as a potential outcode ---
    # Check if the area_name might be an outcode (e.g., "SW1A")
    details = get_outcode_details(area_name)
    if details:
        # If it is a valid outcode, return it.
        # We could potentially expand this using nearest neighbour outcodes if needed.
        logger.debug(f"Treating '{area_name}' as an outcode.")
        return [area_name.strip().upper()]

    # If not an outcode, we can't reliably get postcodes for a general area name easily.
    return []

def get_area_for_any_postcode(postcode):
    """
    Gets administrative area(s) or region associated with a given postcode 
    (primarily looks at the outcode part). Returns a list of potential area names.
    """
    if not postcode or not isinstance(postcode, str):
        return [] # Return empty list for invalid input

    postcode_cleaned = postcode.strip().upper()
    outcode = postcode_cleaned.split(' ')[0]
    
    cache_key = f"area_for_postcode_{outcode}" # Cache based on outcode
    areas = cache.get(cache_key)

    if areas is None:
        areas = []
        details = get_outcode_details(outcode)
        
        if details:
            # Collect potential area names from details
            if details.get("admin_district"):
                areas.extend(details["admin_district"])
            if details.get("parliamentary_constituency"):
                 areas.extend(details["parliamentary_constituency"])
            if details.get("region") and details["region"] not in areas: # Add region if distinct
                 areas.append(details["region"])
            
            # Remove duplicates
            areas = list(set(a for a in areas if a)) # Ensure unique and non-empty strings
            
            if areas:
                 logger.debug(f"Found areas {areas} for postcode '{postcode}' (outcode '{outcode}')")
                 cache.set(cache_key, areas, timeout=3600 * 24) # Cache for 24h
            else:
                 logger.debug(f"No specific admin areas found for outcode '{outcode}', caching empty.")
                 cache.set(cache_key, [], timeout=3600) # Cache empty result shorter
        else:
            # Error occurred in get_outcode_details or outcode not found
             logger.debug(f"Could not retrieve details for outcode '{outcode}'.")
             cache.set(cache_key, [], timeout=3600) # Cache empty result shorter

    # Ensure return is always a list
    return areas if areas is not None else []

# Add __init__.py if it doesn't exist (important for services to be a package)
# This should ideally be done separately if needed, but let's assume it exists for now. 