import os
import json
import logging
from django.conf import settings

logger = logging.getLogger(__name__)

# Path to our postcode lookup JSON file
POSTCODE_LOOKUP_PATH = os.path.join(settings.BASE_DIR, 'checker', 'data', 'postcodes', 'location_postcodes.json')

# Cache the loaded data to avoid reading the file multiple times
_postcode_data = None

def _load_postcode_data():
    """Load the postcode lookup data from JSON file (with caching)"""
    global _postcode_data
    
    if _postcode_data is not None:
        return _postcode_data
        
    try:
        if os.path.exists(POSTCODE_LOOKUP_PATH):
            with open(POSTCODE_LOOKUP_PATH, 'r') as f:
                _postcode_data = json.load(f)
                logger.info(f"Loaded postcode data with {len(_postcode_data.get('locations', {}))} locations")
                return _postcode_data
        else:
            logger.warning(f"Postcode lookup file not found at {POSTCODE_LOOKUP_PATH}")
            _postcode_data = {"locations": {}, "counties": {}, "outcode_to_county": {}}
            return _postcode_data
    except Exception as e:
        logger.error(f"Error loading postcode data: {str(e)}")
        _postcode_data = {"locations": {}, "counties": {}, "outcode_to_county": {}}
        return _postcode_data

def get_outcodes_for_location(location):
    """
    Get outcodes associated with a location name
    
    Args:
        location (str): Location name like 'nottingham' or 'london'
        
    Returns:
        list: List of outcodes for the location
    """
    location_lower = location.lower().strip()
    data = _load_postcode_data()
    
    # First check if we have a direct match in our locations dictionary
    if location_lower in data.get('locations', {}):
        return data['locations'][location_lower].get('outcodes', [])
    
    # If not, check if it matches any county
    for county, outcodes in data.get('counties', {}).items():
        if location_lower in county or county in location_lower:
            return outcodes
            
    # No match found
    logger.debug(f"No outcode match found for location: {location}")
    return []

def get_county_for_outcode(outcode):
    """
    Get the county associated with an outcode
    
    Args:
        outcode (str): Outcode like 'NG1' or 'SW1'
        
    Returns:
        str: County name or None if not found
    """
    if not outcode:
        return None
        
    outcode = outcode.upper().strip()
    data = _load_postcode_data()
    
    # First, try exact match
    if outcode in data.get('outcode_to_county', {}):
        return data['outcode_to_county'][outcode]
    
    # If not found, try with just the letter prefix (e.g., 'NG' from 'NG1')
    prefix = ''.join([c for c in outcode if c.isalpha()])
    if prefix in data.get('outcode_to_county', {}):
        return data['outcode_to_county'][prefix]
    
    # No match found
    return None

def get_locations_by_county(county):
    """
    Get locations in a specific county
    
    Args:
        county (str): County name like 'Nottinghamshire'
        
    Returns:
        list: List of location names in that county
    """
    county_lower = county.lower().strip()
    data = _load_postcode_data()
    
    matching_locations = []
    
    for location, details in data.get('locations', {}).items():
        counties = [c.lower() for c in details.get('counties', [])]
        if county_lower in counties:
            matching_locations.append(location)
            
    return matching_locations

def get_all_data_for_location(location):
    """
    Get all available data for a location
    
    Args:
        location (str): Location name
        
    Returns:
        dict: Dictionary with outcodes and counties for the location
    """
    location_lower = location.lower().strip()
    data = _load_postcode_data()
    
    if location_lower in data.get('locations', {}):
        return data['locations'][location_lower]
    
    # No direct match, return empty dict
    return {"outcodes": [], "counties": []} 