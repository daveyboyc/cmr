import time
import json
import os
import requests
import pandas as pd
from django.conf import settings
from django.core.cache import cache
import logging
import glob

from ..utils import normalize, get_cache_key, get_json_path, ensure_directory_exists


# CMU DATA ACCESS FUNCTIONS

def get_cmu_data_from_json():
    """
    Get all CMU data from the JSON file.
    If the JSON doesn't exist, returns None.
    """
    json_path = os.path.join(settings.BASE_DIR, 'cmu_data.json')

    # Check if the file exists
    if not os.path.exists(json_path):
        return None

    try:
        with open(json_path, 'r') as f:
            all_cmu_data = json.load(f)
        return all_cmu_data
    except Exception as e:
        print(f"Error reading CMU data from JSON: {e}")
        return None


def save_cmu_data_to_json(cmu_records):
    """
    Save all CMU records to a JSON file.
    Creates the file if it doesn't exist, otherwise replaces it.
    """
    json_path = os.path.join(settings.BASE_DIR, 'cmu_data.json')

    # Write the data to the file
    try:
        with open(json_path, 'w') as f:
            json.dump(cmu_records, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving CMU data to JSON: {e}")
        return False


def fetch_all_cmu_records(limit=None):
    """
    Fetch all CMU records from the API.
    First checks if we have them stored in JSON.
    """
    # Check if we have the data in our JSON file
    json_cmu_data = get_cmu_data_from_json()
    if json_cmu_data is not None:
        print(f"Using JSON-stored CMU data, found {len(json_cmu_data)} records")
        return json_cmu_data, 0

    params = {
        "resource_id": "25a5fa2e-873d-41c5-8aaf-fbc2b06d79e6",
        "limit": limit,
        "offset": 0
    }
    all_records = []
    total_time = 0
    while True:
        start_time = time.time()
        response = requests.get(
            "https://api.neso.energy/api/3/action/datastore_search",
            params=params,
            timeout=20
        )
        response.raise_for_status()
        total_time += time.time() - start_time
        result = response.json()["result"]
        records = result["records"]
        all_records.extend(records)
        if len(all_records) >= result["total"]:
            break
        params["offset"] += limit

    # Save to JSON for future use
    save_cmu_data_to_json(all_records)

    return all_records, total_time


def get_cmu_dataframe():
    """
    Get and process CMU data as a DataFrame.
    Returns a pandas DataFrame with normalized fields.
    """
    cmu_df = cache.get("cmu_df")
    api_time = 0

    if cmu_df is None:
        try:
            all_records, api_time = fetch_all_cmu_records(limit=5000)
            cmu_df = pd.DataFrame(all_records)

            # Set up necessary columns
            cmu_df["Name of Applicant"] = cmu_df.get("Name of Applicant", pd.Series()).fillna("").astype(str)
            cmu_df["Parent Company"] = cmu_df.get("Parent Company", pd.Series()).fillna("").astype(str)
            cmu_df["Delivery Year"] = cmu_df.get("Delivery Year", pd.Series()).fillna("").astype(str)

            # Identify CMU ID field
            possible_cmu_id_fields = ["CMU ID", "cmu_id", "CMU_ID", "cmuId", "id", "identifier", "ID"]
            cmu_id_field = next((field for field in possible_cmu_id_fields if field in cmu_df.columns), None)
            if cmu_id_field:
                cmu_df["CMU ID"] = cmu_df[cmu_id_field].fillna("N/A").astype(str)
            else:
                cmu_df["CMU ID"] = "N/A"

            # Set Full Name
            cmu_df["Full Name"] = cmu_df["Name of Applicant"].str.strip()
            cmu_df["Full Name"] = cmu_df.apply(
                lambda row: row["Full Name"] if row["Full Name"] else row["Parent Company"],
                axis=1
            )

            # Add normalized fields for searching
            cmu_df["Normalized Full Name"] = cmu_df["Full Name"].apply(normalize)
            cmu_df["Normalized CMU ID"] = cmu_df["CMU ID"].apply(normalize)

            # Create complete mapping of all CMU IDs to company names
            cmu_to_company_mapping = {}
            for _, row in cmu_df.iterrows():
                cmu_id = row.get("CMU ID", "").strip()
                if cmu_id and cmu_id != "N/A":
                    cmu_to_company_mapping[cmu_id] = row.get("Full Name", "")

            cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 3600)
            cache.set("cmu_df", cmu_df, 900)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching CMU data: {e}")
            return None, api_time

    return cmu_df, api_time


# COMPONENT DATA ACCESS FUNCTIONS

def get_json_path(cmu_id):
    """
    Get the path to the JSON file for a given CMU ID.
    This is a utility function to avoid duplicating path logic.
    """
    if not cmu_id:
        return None
        
    # Get first character of CMU ID as folder name
    prefix = cmu_id[0].upper()
    
    # Path for this specific CMU's components
    json_dir = os.path.join(settings.BASE_DIR, 'json_data')
    json_path = os.path.join(json_dir, f'components_{prefix}.json')
    
    return json_path


def get_component_data_from_json(cmu_id):
    """
    Get component data from JSON for a specific CMU ID.
    Returns the components as a list or None if not found.
    """
    logger = logging.getLogger(__name__)
    
    if not cmu_id:
        logger.warning("No CMU ID provided to get_component_data_from_json")
        return None

    # Special handling for LIMEJUMP LTD CMU IDs
    is_limejump_cmu = cmu_id.startswith("CM_LJ")

    logger.info(f"Getting component data from JSON for CMU ID: {cmu_id}")
    json_path = get_json_path(cmu_id)
    logger.info(f"JSON path for {cmu_id}: {json_path}")

    # Check if the file exists
    if not os.path.exists(json_path):
        logger.warning(f"JSON file does not exist: {json_path}")
        # Try the old path as fallback
        old_json_path = os.path.join(settings.BASE_DIR, 'component_data.json')
        logger.info(f"Trying old JSON path: {old_json_path}")
        if os.path.exists(old_json_path):
            try:
                logger.info(f"Old JSON file exists, trying to load it")
                with open(old_json_path, 'r') as f:
                    all_components = json.load(f)
                if cmu_id in all_components:
                    logger.info(f"Found {len(all_components[cmu_id])} components for {cmu_id} in old JSON")
                    return all_components.get(cmu_id)
                logger.warning(f"CMU ID {cmu_id} not found in old JSON")
            except Exception as e:
                logger.error(f"Error reading component data from old JSON: {e}")
        return None

    try:
        logger.info(f"Loading JSON file: {json_path}")
        with open(json_path, 'r') as f:
            all_components = json.load(f)

        # Try exact match first
        if cmu_id in all_components:
            logger.info(f"Found exact match for {cmu_id} with {len(all_components[cmu_id])} components")
            return all_components[cmu_id]

        # If not found, try case-insensitive match
        for file_cmu_id in all_components.keys():
            if file_cmu_id.lower() == cmu_id.lower():
                logger.info(f"Found case-insensitive match: {file_cmu_id} for {cmu_id} with {len(all_components[file_cmu_id])} components")
                return all_components[file_cmu_id]
                
        # If still not found, try prefix match (e.g., "TS17" matching "TS17_1", "TS17_2", etc.)
        matching_components = []
        matching_cmu_ids = []
        for file_cmu_id, components in all_components.items():
            if file_cmu_id.startswith(cmu_id + "_") or file_cmu_id == cmu_id:
                logger.info(f"Found prefix match: {file_cmu_id} for {cmu_id} with {len(components)} components")
                matching_components.extend(components)
                matching_cmu_ids.append(file_cmu_id)
                
        if matching_components:
            logger.info(f"Combining {len(matching_components)} components from {len(matching_cmu_ids)} CMU IDs that match prefix '{cmu_id}'")
            # Use a set to deduplicate components by location
            unique_components = []
            seen_locations = set()
            
            for component in matching_components:
                location = component.get("Location and Post Code", "")
                if location and location not in seen_locations:
                    seen_locations.add(location)
                    unique_components.append(component)
                    
            logger.info(f"Returning {len(unique_components)} unique components after deduplication")
            return unique_components

        # If still not found, return None
        logger.warning(f"No match found for CMU ID {cmu_id} in JSON")
        return None
    except Exception as e:
        logger.error(f"Error reading component data from JSON: {e}")
        return None


def save_component_data_to_json(cmu_id, components):
    """
    Save component data to JSON for a specific CMU ID.
    Returns True if successful, False otherwise.
    """
    if not cmu_id:
        return False

    # Get first character of CMU ID as folder name
    prefix = cmu_id[0].upper()

    # Create a directory for split files if it doesn't exist
    json_dir = os.path.join(settings.BASE_DIR, 'json_data')
    ensure_directory_exists(json_dir)

    # Path for this specific CMU's components
    json_path = os.path.join(json_dir, f'components_{prefix}.json')

    # Initialize or load existing data
    all_components = {}
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r') as f:
                all_components = json.load(f)
        except Exception as e:
            print(f"Error reading existing component data: {e}")

    # Get company name from mapping cache
    cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
    company_name = cmu_to_company_mapping.get(cmu_id, "")

    # Try case-insensitive match if needed
    if not company_name:
        for mapping_cmu_id, mapping_company in cmu_to_company_mapping.items():
            if mapping_cmu_id.lower() == cmu_id.lower():
                company_name = mapping_company
                break

    # Make sure each component has the Company Name field
    updated_components = []
    for component in components:
        if "Company Name" not in component and company_name:
            component = component.copy()  # Make a copy to avoid modifying the original
            component["Company Name"] = company_name

        # Add CMU ID to the component for reference
        if "CMU ID" not in component:
            component = component.copy()
            component["CMU ID"] = cmu_id

        updated_components.append(component)

    # Add or update the components for this CMU ID
    all_components[cmu_id] = updated_components

    # Write the updated data back to the file
    try:
        with open(json_path, 'w') as f:
            json.dump(all_components, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving component data to JSON: {e}")
        return False


def fetch_components_for_cmu_id(cmu_id, limit=None, page=1, per_page=500):
    """
    Fetch components for a given CMU ID with pagination support.
    Now accurately counts components for all searches and uses 500 per page.
    """
    import time
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info(f"Fetching components for query '{cmu_id}' (page={page}, per_page={per_page})")
    
    # First check JSON cache
    start_time = time.time()
    all_components = get_component_data_from_json(cmu_id)
    
    # If components found in JSON cache
    if all_components:
        logger.info(f"Found {len(all_components)} components in JSON cache for '{cmu_id}'")
        total_count = len(all_components)  # Use actual count, not API estimate
        
        # Apply pagination
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        
        # Safety check for valid indices
        if start_idx >= len(all_components):
            start_idx = 0
            end_idx = min(per_page, len(all_components))
        
        paginated_components = all_components[start_idx:end_idx]
        
        # Calculate total pages
        total_pages = (total_count + per_page - 1) // per_page
        
        metadata = {
            "total_count": total_count,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
            "source": "json_cache",
            "processing_time": time.time() - start_time
        }
        
        logger.info(f"Returning {len(paginated_components)} components (page {page} of {total_pages})")
        return paginated_components, metadata
    
    # If not in JSON cache, try API
    try:
        # Get an accurate total count for this query
        total_count = get_accurate_total_count(cmu_id)
        logger.info(f"Accurate count for '{cmu_id}': {total_count}")
        
        # Now fetch the actual page of data
        api_limit = per_page  # Only fetch what we need for this page
        api_offset = (page - 1) * per_page
        
        params = {
            "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
            "q": cmu_id,
            "limit": api_limit,
            "offset": api_offset
        }
        
        logger.info(f"Making API request for '{cmu_id}' with limit={api_limit}, offset={api_offset}")
        import requests
        response = requests.get(
            "https://data.nationalgrideso.com/api/3/action/datastore_search",
            params=params,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json().get("result", {})
            all_api_components = result.get("records", [])
            
            # Log what we received
            actual_count = len(all_api_components)
            logger.info(f"API returned {actual_count} components for page {page}")
            
            # IMPORTANT CHECK: If we got 0 components but our page number is reasonable,
            # there might be an API limitation. Set a minimum count.
            if actual_count == 0 and page > 1 and page < 100 and total_count > 0:
                logger.warning(f"No components returned for page {page} despite positive total count. API limitation detected.")
                
                # Try to deduce the actual per-page limit the API is using
                detected_limit = (page - 1) * per_page
                logger.info(f"Detected API limit around {detected_limit}")
                
                # Adjust the total count if needed to avoid confusion with pagination
                if detected_limit < total_count:
                    logger.info(f"Adjusting total count from {total_count} to match detected limit of {detected_limit}")
                    total_count = detected_limit
            
            # If this is page 1 and we got fewer results than expected but more than 0,
            # check if we need to adjust our total count
            if page == 1 and actual_count < total_count and actual_count > 0:
                # Try to verify if there are more pages by checking page 2
                page2_params = {
                    "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
                    "q": cmu_id,
                    "limit": 1,
                    "offset": per_page
                }
                
                try:
                    page2_response = requests.get(
                        "https://data.nationalgrideso.com/api/3/action/datastore_search",
                        params=page2_params,
                        timeout=10
                    )
                    
                    if page2_response.status_code == 200:
                        page2_result = page2_response.json().get("result", {})
                        page2_records = page2_result.get("records", [])
                        
                        if not page2_records:
                            # No results on page 2, adjust total count
                            logger.info(f"No results found on page 2, adjusting count from {total_count} to {actual_count}")
                            total_count = actual_count
                except Exception as e:
                    logger.warning(f"Error checking page 2: {str(e)}")
            
            # Save to JSON cache for future use
            if all_api_components:
                save_component_data_to_json(cmu_id, all_api_components)
                logger.info(f"Saved {len(all_api_components)} components to JSON cache")
            
            # Calculate total pages using the accurate total count
            total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
            
            # Ensure reasonable output
            if total_count == 0 and actual_count > 0:
                total_count = actual_count
                total_pages = 1
            
            metadata = {
                "total_count": total_count,
                "displayed_count": actual_count,
                "page": page,
                "per_page": per_page,
                "total_pages": total_pages,
                "source": "api",
                "processing_time": time.time() - start_time
            }
            
            logger.info(f"Returning {len(all_api_components)} components (page {page} of {total_pages})")
            return all_api_components, metadata
        else:
            logger.error(f"API error: {response.status_code}")
            return [], {"error": f"API error: {response.status_code}"}
    except Exception as e:
        logger.exception(f"Exception in fetch_components_for_cmu_id: {str(e)}")
        return [], {"error": str(e)}


def fetch_component_search_results(query, limit=1000, sort_order="desc"):
    """
    Fetch components based on a search query.
    """
    search_cache_key = get_cache_key("components_search", query)
    records = cache.get(search_cache_key)
    api_time = 0

    if records is None:
        try:
            start_time = time.time()
            response = requests.get(
                "https://api.neso.energy/api/3/action/datastore_search",
                params={
                    "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
                    "q": query,
                    "limit": limit
                },
                timeout=20
            )
            response.raise_for_status()
            api_time = time.time() - start_time
            data = response.json()["result"]
            records = data.get("records", [])
            cache.set(search_cache_key, records, 300)  # Cache for 5 minutes

            # Also cache component records by CMU ID for use in company search
            if records:
                for record in records:
                    cmu_id = record.get("CMU ID", "")
                    if cmu_id:
                        components_cache_key = get_cache_key("components_for_cmu", cmu_id)
                        existing_components = cache.get(components_cache_key, [])
                        if record not in existing_components:
                            existing_components.append(record)
                            cache.set(components_cache_key, existing_components, 3600)

                            # Also save to JSON for persistence
                            json_components = get_component_data_from_json(cmu_id) or []
                            if record not in json_components:
                                json_components.append(record)
                                save_component_data_to_json(cmu_id, json_components)

        except Exception as e:
            print(f"Error fetching components data: {e}")
            api_time = time.time() - start_time if 'start_time' in locals() else 0
            return [], api_time, str(e)

    return records, api_time, None


def get_component_total_count():
    """Get the total count of components in the database."""
    total_cache_key = "components_overall_total"
    overall_total = cache.get(total_cache_key)
    api_time = 0

    if overall_total is None:
        try:
            start_time = time.time()
            count_response = requests.get(
                "https://api.neso.energy/api/3/action/datastore_search",
                params={
                    "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
                    "limit": 1
                },
                timeout=20
            )
            count_response.raise_for_status()
            api_time = time.time() - start_time
            count_data = count_response.json()["result"]
            overall_total = count_data.get("total", 0)
            cache.set(total_cache_key, overall_total, 3600)  # Cache for 1 hour
        except Exception as e:
            print(f"Error fetching overall total: {e}")
            api_time = time.time() - start_time if 'start_time' in locals() else 0
            overall_total = 0

    return overall_total, api_time


def get_accurate_total_count(query):
    """
    Make multiple API calls to determine a more accurate total count for large result sets.
    This function works for any search that might have many results.
    """
    import requests
    import time
    import logging
    logger = logging.getLogger(__name__)
    
    # Start with the basic count request
    count_params = {
        "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
        "q": query,
        "limit": 1
    }
    
    try:
        logger.info(f"Making initial count API request for '{query}'")
        count_response = requests.get(
            "https://data.nationalgrideso.com/api/3/action/datastore_search",
            params=count_params,
            timeout=10
        )
        
        if count_response.status_code == 200:
            count_result = count_response.json().get("result", {})
            initial_count = count_result.get("total", 0)
            logger.info(f"API initially reports {initial_count} total matching components")
            
            # If count is small, we can trust it
            if initial_count < 2000:
                return initial_count
                
            # For larger counts, verify by checking if we have results at higher offsets
            test_offsets = [initial_count - 500, initial_count, initial_count + 500]
            highest_with_results = 0
            
            for offset in test_offsets:
                if offset <= 0:
                    continue
                    
                sample_params = {
                    "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
                    "q": query,
                    "limit": 1,
                    "offset": offset
                }
                
                try:
                    sample_response = requests.get(
                        "https://data.nationalgrideso.com/api/3/action/datastore_search",
                        params=sample_params,
                        timeout=10
                    )
                    
                    if sample_response.status_code == 200:
                        sample_result = sample_response.json().get("result", {})
                        sample_records = sample_result.get("records", [])
                        
                        if sample_records:
                            logger.info(f"Found records at offset {offset}")
                            highest_with_results = max(highest_with_results, offset)
                        else:
                            logger.info(f"No records found at offset {offset}")
                except Exception as e:
                    logger.warning(f"Error checking offset {offset}: {str(e)}")
                    
                # Sleep briefly to avoid rate limiting
                time.sleep(0.2)
            
            # Adjust the count based on our findings
            if highest_with_results > 0:
                # We found results at a higher offset than initially reported
                if highest_with_results > initial_count:
                    # The API underreported the count
                    adjusted_count = highest_with_results + 1000
                    logger.info(f"Adjusting count from {initial_count} to {adjusted_count} based on sampling")
                    return adjusted_count
                else:
                    # The initial count seems reasonable
                    return initial_count
            else:
                # If we didn't find any records at test offsets, use initial count
                return initial_count
        else:
            logger.error(f"Count API error: {count_response.status_code}")
            return 0
    except Exception as e:
        logger.exception(f"Exception in get_accurate_total_count: {str(e)}")
        return 0


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
                        
                        if matches:
                            # Add CMU ID to component if not present
                            if "CMU ID" not in component:
                                component = component.copy()
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


def get_cmu_data_by_id(cmu_id):
    """
    Fetch additional data from cmu_data.json for a specific CMU ID.
    
    Args:
        cmu_id: The CMU ID to look up
        
    Returns:
        A dictionary containing the additional data for the CMU ID or None if not found
    """
    logger = logging.getLogger(__name__)
    
    if not cmu_id:
        logger.warning("No CMU ID provided to get_cmu_data_by_id")
        return None
    
    # Try to get from cache first
    cache_key = f"cmu_data_{cmu_id}"
    cached_data = cache.get(cache_key)
    if cached_data:
        logger.info(f"Using cached CMU data for {cmu_id}")
        return cached_data
    
    # Get the data from cmu_data.json
    all_cmu_data = get_cmu_data_from_json()
    if not all_cmu_data:
        logger.warning("No CMU data available from JSON")
        return None
    
    # Find the matching CMU ID
    for cmu_data in all_cmu_data:
        if str(cmu_data.get("CMU ID", "")) == str(cmu_id):
            logger.info(f"Found matching CMU data for {cmu_id}")
            # Cache the result for future use (1 hour)
            cache.set(cache_key, cmu_data, 3600)
            return cmu_data
    
    logger.warning(f"No matching CMU data found for {cmu_id}")
    return None


def analyze_component_duplicates(components):
    """
    Analyze a list of components to identify duplicates.
    
    Args:
        components: List of component dictionaries
        
    Returns:
        Dictionary with analysis results:
        - total_components: Total number of components
        - unique_locations: Number of unique locations
        - location_counts: Dictionary of location counts
        - duplicate_locations: List of locations that appear more than once
    """
    if not components:
        return {
            "total_components": 0,
            "unique_locations": 0,
            "location_counts": {},
            "duplicate_locations": []
        }
    
    # Count locations
    location_counts = {}
    for component in components:
        location = component.get("Location and Post Code", "")
        if not location:
            location = "(No Location)"
        
        if location in location_counts:
            location_counts[location] += 1
        else:
            location_counts[location] = 1
    
    # Find duplicates
    duplicate_locations = [
        {"location": loc, "count": count} 
        for loc, count in location_counts.items() 
        if count > 1
    ]
    
    return {
        "total_components": len(components),
        "unique_locations": len(location_counts),
        "location_counts": location_counts,
        "duplicate_locations": duplicate_locations
    }