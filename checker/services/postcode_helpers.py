import json
import os
import logging
from functools import lru_cache
from django.conf import settings

logger = logging.getLogger(__name__)

@lru_cache(maxsize=1) # Cache the loaded JSON data
def get_postcode_mappings():
    """Loads the postcode mappings from the JSON file."""
    # Adjust the path to point to the correct location relative to BASE_DIR
    # BASE_DIR in settings.py is likely '/Users/davidcrawford/PycharmProjects/cmr/capacity_checker/'
    # The JSON file is at '/Users/davidcrawford/PycharmProjects/cmr/capacity_checker/data_storage/postcode_mappings.json'
    # So the path relative to BASE_DIR should be 'data_storage/postcode_mappings.json'
    #json_path = os.path.join(settings.BASE_DIR.parent, 'capacity_checker/data_storage/postcode_mappings.json') # Assuming BASE_DIR is cmr/
    json_path = os.path.join(settings.BASE_DIR, 'data_storage/postcode_mappings.json') # Assuming BASE_DIR is cmr/capacity_checker/
    
    if not os.path.exists(json_path):
        logger.error(f"Postcode mappings file not found at: {json_path}")
        return None
        
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        logger.info(f"Successfully loaded postcode mappings from {json_path}")
        return data
    except Exception as e:
        logger.exception(f"Error loading postcode mappings from {json_path}: {e}")
        return None

def get_all_postcodes_for_area(area_name):
    """Gets all postcode prefixes associated with a given area name (case-insensitive)."""
    mappings = get_postcode_mappings()
    if not mappings or 'area_to_postcodes' not in mappings:
        logger.warning("Postcode mappings not loaded or invalid.")
        return []
        
    # Normalize area name for case-insensitive lookup
    normalized_area = area_name.strip().lower()
    
    # Perform case-insensitive lookup
    for area, postcodes in mappings['area_to_postcodes'].items():
        if area.lower() == normalized_area:
            logger.debug(f"Found postcodes for area '{area_name}': {postcodes}")
            return postcodes
            
    logger.debug(f"No postcodes found for area '{area_name}'")
    return []

def get_area_for_any_postcode(postcode):
    """Gets the area name associated with a given postcode (matches the outcode part)."""
    mappings = get_postcode_mappings()
    if not mappings or 'postcode_to_area' not in mappings:
        logger.warning("Postcode mappings not loaded or invalid.")
        return None

    if not postcode or not isinstance(postcode, str):
        return None
        
    # Extract the outcode (part before the space, or the whole string if no space)
    outcode = postcode.strip().split(' ')[0].upper()
    
    # Perform case-insensitive lookup (though keys in JSON seem upper)
    area = mappings['postcode_to_area'].get(outcode)

    if area:
        logger.debug(f"Found area '{area}' for postcode '{postcode}' (outcode '{outcode}')")
        return area
        
    logger.debug(f"No area found for postcode '{postcode}' (outcode '{outcode}')")
    return None

# Add __init__.py if it doesn't exist (important for services to be a package)
# This should ideally be done separately if needed, but let's assume it exists for now. 