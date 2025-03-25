import time
import json
import os
import requests
import pandas as pd
from django.conf import settings
from django.core.cache import cache
import logging

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


def fetch_components_for_cmu_id(cmu_id, limit=None, page=1, per_page=100):
    """
    Fetch components for a given CMU ID with pagination support
    """
    import time
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info(f"Fetching components for CMU ID {cmu_id} (page={page}, per_page={per_page}, limit={limit})")
    
    # First check JSON cache
    start_time = time.time()
    all_components = get_component_data_from_json(cmu_id)
    
    # If components found in JSON cache
    if all_components:
        logger.info(f"Found {len(all_components)} components in JSON cache for {cmu_id}")
        total_count = len(all_components)
        
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
        # Set actual limit to much higher than per_page to ensure we get all records
        api_limit = 2000 if limit is None else limit  # High enough to get reasonably accurate counts
        # Calculate API offset based on requested page and items per page
        # e.g. page 2 with 100 per page means start at record 100
        api_offset = (page - 1) * per_page
        
        params = {
            "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
            "q": cmu_id,
            "limit": api_limit,
            "offset": api_offset
        }
        
        logger.info(f"Making API request for {cmu_id} with limit={api_limit}")
        import requests
        response = requests.get(
            "https://data.nationalgrideso.com/api/3/action/datastore_search",
            params=params,
            timeout=30  # Increased timeout for larger results
        )
        
        if response.status_code == 200:
            result = response.json().get("result", {})
            all_api_components = result.get("records", [])
            # IMPORTANT: Use the actual count of components we received 
            # NOT the API's reported total which might be inaccurate
            actual_count = len(all_api_components)
            api_reported = result.get("total", 0)
            total_count = actual_count  # Use actual count for pagination
            
            logger.info(f"API returned {len(all_api_components)} components (total: {total_count})")
            
            # Save to JSON cache for future use
            if all_api_components:
                save_component_data_to_json(cmu_id, all_api_components)
                logger.info(f"Saved {len(all_api_components)} components to JSON cache")
            
            # Apply pagination on the API results ourselves
            start_idx = 0  # Start from beginning since we already have the offset from API
            end_idx = min(per_page, len(all_api_components))
            
            # Safety check
            if start_idx >= len(all_api_components):
                start_idx = 0
                end_idx = min(per_page, len(all_api_components))
                
            paginated_components = all_api_components[start_idx:end_idx]
            
            # Calculate total pages
            total_pages = (api_reported + per_page - 1) // per_page  # Use API reported for total pages
            
            
            metadata = {
                "total_count": api_reported,
                "actual_returned": actual_count,
                "page": page,
                "per_page": per_page,
                "total_pages": total_pages,
                "source": "api",
                "processing_time": time.time() - start_time
            }
            
            logger.info(f"Returning {len(paginated_components)} components (page {page} of {total_pages})")
            return paginated_components, metadata
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