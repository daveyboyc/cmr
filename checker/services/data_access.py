def get_components_from_database(cmu_id=None, component_id=None, location=None, company_name=None, limit=None):
    """
    Fetch components from the database based on various filters.
    Returns a list of component dictionaries.
    """
    from checker.models import Component
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info(f"Fetching components from database: cmu_id={cmu_id}, component_id={component_id}")
    
    # Build the query
    query = Component.objects.all()
    
    if cmu_id:
        query = query.filter(cmu_id__iexact=cmu_id)
    
    if component_id:
        query = query.filter(component_id=component_id)
        
    if location:
        query = query.filter(location__icontains=location)
        
    if company_name:
        query = query.filter(company_name__icontains=company_name)
    
    # Apply limit if provided
    if limit:
        query = query[:limit]
    
    # Execute query and convert to list of dictionaries
    components = []
    for comp in query:
        # Create a dictionary representation
        comp_dict = {
            "CMU ID": comp.cmu_id,
            "Location and Post Code": comp.location,
            "Description of CMU Components": comp.description,
            "Generating Technology Class": comp.technology,
            "Company Name": comp.company_name,
            "Auction Name": comp.auction_name,
            "Delivery Year": comp.delivery_year,
            "Status": comp.status,
            "Type": comp.type,
            "_id": comp.component_id
        }
        
        # Add any additional data if available
        if comp.additional_data:
            for key, value in comp.additional_data.items():
                if key not in comp_dict:
                    comp_dict[key] = value
                    
        components.append(comp_dict)
    
    logger.info(f"Found {len(components)} components in database")
    return components

def get_component_data_from_json(cmu_id):
    """
    Get component data for a specific CMU ID - prioritizes the database.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    if not cmu_id:
        return []
    
    # STEP 1: Try database first
    logger.info(f"Looking for components in database for CMU ID: {cmu_id}")
    db_components = get_components_from_database(cmu_id=cmu_id)
    
    if db_components:
        logger.info(f"Found {len(db_components)} components in database for {cmu_id}")
        return db_components, {
            'total_count': len(db_components),
            'total_pages': 1,
            'current_page': 1
        }
    
    # STEP 2: Fall back to JSON if not in database
    logger.info(f"No components found in database for {cmu_id}, falling back to JSON")
    
    # Original JSON-based implementation
    import json
    import os
    from pathlib import Path
    
    # Get the first letter of the CMU ID
    first_letter = cmu_id[0].upper()
    
    # Construct the path to the JSON file
    json_file_path = os.path.join(
        Path(__file__).resolve().parent.parent,
        'data',
        f'components_{first_letter}.json'
    )
    
    try:
        # Check if the file exists
        if not os.path.exists(json_file_path):
            logger.warning(f"JSON file not found: {json_file_path}")
            return [], {'total_count': 0, 'total_pages': 0, 'current_page': 1}
        
        # Load the JSON file
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        # Get components for the CMU ID
        components = data.get(cmu_id, [])
        logger.info(f"Found {len(components)} components in JSON for {cmu_id}")
        
        return components, {
            'total_count': len(components),
            'total_pages': 1,
            'current_page': 1
        }
        
    except Exception as e:
        logger.error(f"Error reading JSON file for CMU ID {cmu_id}: {str(e)}")
        return [], {
            'total_count': 0,
            'total_pages': 0,
            'current_page': 1
        }

def fetch_components_for_cmu_id(cmu_id, limit=None, page=1, per_page=500):
    """
    Fetch components for a given CMU ID with database-first approach.
    """
    import time
    import logging
    from django.core.cache import cache
    from checker.utils import get_cache_key
    
    logger = logging.getLogger(__name__)
    logger.info(f"Fetching components for query '{cmu_id}' (page={page}, per_page={per_page})")
    
    start_time = time.time()
    
    # First check cache for performance
    components_cache_key = get_cache_key("components_for_cmu", cmu_id)
    cached_components = cache.get(components_cache_key)
    
    if cached_components:
        logger.info(f"Found {len(cached_components)} components in cache for '{cmu_id}'")
        total_count = len(cached_components)
        
        # Apply pagination
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_components = cached_components[start_idx:end_idx]
        
        metadata = {
            "total_count": total_count,
            "page": page,
            "per_page": per_page,
            "total_pages": (total_count + per_page - 1) // per_page,
            "source": "cache",
            "processing_time": time.time() - start_time
        }
        
        return paginated_components, metadata
    
    # DATABASE FIRST: Try to get from database
    try:
        logger.info(f"Checking database for CMU ID: {cmu_id}")
        db_components = get_components_from_database(cmu_id=cmu_id)
        
        if db_components:
            total_count = len(db_components)
            logger.info(f"Found {total_count} components in database for '{cmu_id}'")
            
            # Apply pagination
            start_idx = (page - 1) * per_page
            end_idx = min(start_idx + per_page, total_count)
            paginated_components = db_components[start_idx:end_idx]
            
            # Cache the full result
            if total_count <= 1000:  # Only cache reasonably sized results
                cache.set(components_cache_key, db_components, 3600)
            
            metadata = {
                "total_count": total_count,
                "page": page,
                "per_page": per_page,
                "total_pages": (total_count + per_page - 1) // per_page,
                "source": "database",
                "processing_time": time.time() - start_time
            }
            
            return paginated_components, metadata
            
    except Exception as e:
        logger.exception(f"Error querying database for CMU ID {cmu_id}: {str(e)}")
        # Fall through to JSON/API fetching
    
    # If not in database, try JSON
    logger.info(f"No components found in database, trying JSON for {cmu_id}")
    json_components, json_metadata = get_component_data_from_json(cmu_id)
    
    if json_components:
        total_count = len(json_components)
        logger.info(f"Found {total_count} components in JSON for {cmu_id}")
        
        # Apply pagination
        start_idx = (page - 1) * per_page
        end_idx = min(start_idx + per_page, total_count)
        paginated_components = json_components[start_idx:end_idx]
        
        metadata = {
            "total_count": total_count,
            "page": page,
            "per_page": per_page,
            "total_pages": (total_count + per_page - 1) // per_page,
            "source": "json",
            "processing_time": time.time() - start_time
        }
        
        return paginated_components, metadata
    
    # If nothing found, return empty result
    logger.info(f"No components found for {cmu_id} in any source")
    return [], {
        "total_count": 0,
        "page": page,
        "per_page": per_page,
        "total_pages": 0,
        "source": "none",
        "processing_time": time.time() - start_time
    } 