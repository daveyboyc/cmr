from django.db.models import Q
import logging

logger = logging.getLogger(__name__)

def get_location_filter(location_query):
    """
    Create a filter for location-based searches.
    This helps enhance search by checking location, county, and postcodes.
    
    Args:
        location_query (str): The location search query
        
    Returns:
        Q: Django Q object for filtering
    """
    if not location_query:
        return None
        
    # Basic filter - check location, county, and outward code (postcode)
    location_query = location_query.strip()
    location_filter = (
        Q(location__icontains=location_query) |
        Q(county__icontains=location_query) |
        Q(outward_code__iexact=location_query.upper())
    )
    
    # In the future, this could be enhanced with:
    # 1. Postcode lookup from API
    # 2. Geo-based radius search
    # 3. Multi-word location matching
    
    logger.info(f"Created location filter for query: {location_query}")
    return location_filter 