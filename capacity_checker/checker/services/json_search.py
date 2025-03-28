def search_all_json_files(query, page=1, per_page=500):
    """
    Search all available JSON files for components matching the query.
    This bypasses API limitations by using locally stored data.
    
    Args:
        query: The search term
        page: Page number for pagination
        per_page: Number of results per page
        
    Returns:
        tuple: (matching_components, metadata)
    """
    import os
    import json
    import glob
    import time
    import logging
    from django.conf import settings
    
    logger = logging.getLogger(__name__)
    logger.info(f"Searching all JSON files for '{query}'")
    start_time = time.time()
    
    # Normalize the query for case-insensitive matching
    norm_query = query.lower()
    
    # Path to the JSON data directory
    json_dir = os.path.join(settings.BASE_DIR, 'json_data')
    
    # Get all JSON files
    json_pattern = os.path.join(json_dir, 'components_*.json')
    json_files = glob.glob(json_pattern)
    logger.info(f"Found {len(json_files)} JSON files to search")
    
    # If no JSON files, return empty results
    if not json_files:
        return [], {"error": "No JSON files found", "processing_time": time.time() - start_time}
    
    # Variable to store all matching components
    all_matching_components = []
    
    # Set to track unique component IDs to avoid duplicates
    seen_ids = set()
    
    # Process each JSON file
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                file_data = json.load(f)
                
                # Each JSON file contains a dict of CMU IDs to component lists
                for cmu_id, components in file_data.items():
                    # Check if this CMU ID matches the query directly
                    cmu_match = norm_query in cmu_id.lower()
                    
                    # Process each component
                    for component in components:
                        # Skip if we've seen this component before
                        component_id = component.get("_id", "")
                        if component_id and component_id in seen_ids:
                            continue
                            
                        # Check if this CMU ID matches or if any component field matches the query
                        matches = cmu_match
                        
                        if not matches:
                            # Check key fields for matches
                            for field in [
                                "Location and Post Code", 
                                "Description of CMU Components",
                                "Company Name",
                                "Generating Technology Class",
                                "Status"
                            ]:
                                if field in component and norm_query in str(component[field]).lower():
                                    matches = True
                                    break
                                    
                                # Special handling for postcode searches
                                if field == "Location and Post Code" and field in component:
                                    # Get the location string and normalize it for postcode comparison
                                    location_str = str(component[field]).lower()
                                    # Remove spaces for better postcode matching
                                    normalized_loc = location_str.replace(" ", "")
                                    normalized_query = norm_query.replace(" ", "")
                                    
                                    # Try matching without spaces
                                    if normalized_query in normalized_loc:
                                        logger.info(f"Postcode match found: '{norm_query}' in '{location_str}'")
                                        matches = True
                                        break
                                        
                                    # Additional UK postcode specific matching
                                    # UK postcodes have format: AA9A 9AA or AA99 9AA
                                    # Check if query might be a partial postcode (e.g., just the first part)
                                    parts = location_str.replace(',', ' ').split()
                                    for part in parts:
                                        # Handle postcodes with or without spaces
                                        if len(part) >= 2 and part.replace(" ", "").startswith(normalized_query):
                                            logger.info(f"Partial postcode match found: '{norm_query}' at start of '{part}' in '{location_str}'")
                                            matches = True
                                            break
                        
                        if matches:
                            # Add CMU ID to component if not present
                            if "CMU ID" not in component:
                                component = component.copy()  # Make a copy to avoid modifying the original
                                component["CMU ID"] = cmu_id
                                
                            all_matching_components.append(component)
                            
                            # Track that we've seen this component
                            if component_id:
                                seen_ids.add(component_id)
                
        except Exception as e:
            logger.error(f"Error processing JSON file {json_file}: {str(e)}")
    
    # Total number of matching components
    total_count = len(all_matching_components)
    logger.info(f"Found {total_count} matching components in JSON files")
    
    # Sort components by Delivery Year if available
    try:
        all_matching_components.sort(
            key=lambda comp: comp.get("Delivery Year", "") or "",
            reverse=True  # Newest first
        )
    except Exception as e:
        logger.error(f"Error sorting components: {str(e)}")
    
    # Apply pagination
    start_idx = (page - 1) * per_page
    end_idx = min(start_idx + per_page, total_count)
    
    # Handle out-of-range pagination
    if start_idx >= total_count and total_count > 0:
        # Adjust to last valid page
        page = (total_count + per_page - 1) // per_page
        start_idx = (page - 1) * per_page
        end_idx = min(start_idx + per_page, total_count)
    
    # Get paginated components
    paginated_components = all_matching_components[start_idx:end_idx]
    
    # Calculate pagination metadata
    total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
    
    metadata = {
        "total_count": total_count,
        "displayed_count": len(paginated_components),
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "source": "json_files",
        "processing_time": time.time() - start_time
    }
    
    logger.info(f"Returning {len(paginated_components)} components (page {page} of {total_pages})")
    return paginated_components, metadata 