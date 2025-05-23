import time
import json
import os
import requests
import pandas as pd
from django.conf import settings
from django.core.cache import cache
import logging
import glob
from django.db.models import Q, Count
from django.db import connection
import re
import traceback
import sys

from ..utils import normalize, get_cache_key, get_json_path, ensure_directory_exists
from ..models import Component

# Import the postcode/area helper functions correctly
from .postcode_helpers import get_all_postcodes_for_area, get_area_for_any_postcode
logger = logging.getLogger(__name__)


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
    Compatibility function that provides a DataFrame-like interface
    while using the database under the hood.
    
    Returns:
        tuple: (cmu_dataframe, api_time)
    """
    import time
    import pandas as pd
    import logging
    from ..models import Component
    
    logger = logging.getLogger(__name__)
    start_time = time.time()
    
    try:
        # Check if we have cache
        cmu_df = cache.get("cmu_df")
        if cmu_df is not None:
            return cmu_df, 0
        
        # Get data from database
        cmu_records = Component.objects.values(
            'cmu_id', 'company_name', 'delivery_year', 'auction_name'
        ).distinct()
        
        # Convert to a DataFrame for backward compatibility
        df_data = []
        for record in cmu_records:
            df_data.append({
                "CMU ID": record['cmu_id'],
                "Name of Applicant": record['company_name'],
                "Full Name": record['company_name'],
                "Delivery Year": record['delivery_year'],
                "Auction Name": record['auction_name']
            })
        
        cmu_df = pd.DataFrame(df_data)
        
        # Add normalized fields for searching
        cmu_df["Normalized Full Name"] = cmu_df["Full Name"].apply(normalize)
        cmu_df["Normalized CMU ID"] = cmu_df["CMU ID"].apply(normalize)
        
        # Cache the mappings for future use
        cmu_to_company_mapping = {}
        for _, row in cmu_df.iterrows():
            cmu_id = row.get("CMU ID", "").strip()
            if cmu_id and cmu_id != "N/A":
                cmu_to_company_mapping[cmu_id] = row.get("Full Name", "")
                
        cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 3600)
        cache.set("cmu_df", cmu_df, 900)
        
        api_time = time.time() - start_time
        return cmu_df, api_time
    
    except Exception as e:
        logger.exception(f"Error creating CMU dataframe: {str(e)}")
        # If all else fails, try original implementation
        try:
            all_records, api_time = fetch_all_cmu_records(limit=5000)
            cmu_df = pd.DataFrame(all_records)
            
            # Set up necessary columns as before
            cmu_df["Name of Applicant"] = cmu_df.get("Name of Applicant", pd.Series()).fillna("").astype(str)
            cmu_df["Parent Company"] = cmu_df.get("Parent Company", pd.Series()).fillna("").astype(str)
            cmu_df["Delivery Year"] = cmu_df.get("Delivery Year", pd.Series()).fillna("").astype(str)
            
            possible_cmu_id_fields = ["CMU ID", "cmu_id", "CMU_ID", "cmuId", "id", "identifier", "ID"]
            cmu_id_field = next((field for field in possible_cmu_id_fields if field in cmu_df.columns), None)
            if cmu_id_field:
                cmu_df["CMU ID"] = cmu_df[cmu_id_field].fillna("N/A").astype(str)
            else:
                cmu_df["CMU ID"] = "N/A"
                
            cmu_df["Full Name"] = cmu_df["Name of Applicant"].str.strip()
            cmu_df["Full Name"] = cmu_df.apply(
                lambda row: row["Full Name"] if row["Full Name"] else row["Parent Company"],
                axis=1
            )
            
            cmu_df["Normalized Full Name"] = cmu_df["Full Name"].apply(normalize)
            cmu_df["Normalized CMU ID"] = cmu_df["CMU ID"].apply(normalize)
            
            cache.set("cmu_df", cmu_df, 900)
            return cmu_df, api_time
        except:
            return None, 0


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
    DEPRECATED: This function now redirects to the database version.
    Get component data for a specific CMU ID from the database.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    logger.warning(f"get_component_data_from_json is deprecated, redirecting to database lookup for {cmu_id}")
    
    # Call the new database-first function but force page 1 with a large limit
    components, _ = fetch_components_for_cmu_id(cmu_id, page=1, per_page=1000)
    return components if components else None


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


def detect_potential_duplicates(components):
    """
    Analyze a list of components to detect potential duplicates based on various criteria.
    Returns a dict mapping component IDs to lists of potential duplicate IDs.
    """
    if not components:
        return {}
        
    # Create lookup dictionaries for different matching criteria
    location_matches = {}  # Components with same location
    desc_matches = {}     # Components with same description
    cmu_matches = {}      # Components with same CMU ID
    
    # First pass: build lookup dictionaries
    for comp in components:
        loc = comp.get("Location and Post Code", "").strip().lower()
        desc = comp.get("Description of CMU Components", "").strip().lower()
        cmu_id = comp.get("CMU ID", "").strip()
        comp_id = comp.get("_id", "")
        
        if not comp_id:  # Skip if no component ID
            continue
            
        # Add to location matches
        if loc:
            if loc not in location_matches:
                location_matches[loc] = []
            location_matches[loc].append(comp_id)
            
        # Add to description matches
        if desc:
            if desc not in desc_matches:
                desc_matches[desc] = []
            desc_matches[desc].append(comp_id)
            
        # Add to CMU matches
        if cmu_id:
            if cmu_id not in cmu_matches:
                cmu_matches[cmu_id] = []
            cmu_matches[cmu_id].append(comp_id)
    
    # Second pass: identify potential duplicates
    duplicates = {}
    
    for comp in components:
        comp_id = comp.get("_id", "")
        if not comp_id:
            continue
            
        loc = comp.get("Location and Post Code", "").strip().lower()
        desc = comp.get("Description of CMU Components", "").strip().lower()
        cmu_id = comp.get("CMU ID", "").strip()
        
        potential_dupes = set()
        
        # Find components that match on location AND description
        if loc and desc:
            loc_matches_set = set(location_matches.get(loc, []))
            desc_matches_set = set(desc_matches.get(desc, []))
            strong_matches = loc_matches_set.intersection(desc_matches_set)
            
            # Remove self from matches
            strong_matches.discard(comp_id)
            
            if strong_matches:
                potential_dupes.update(strong_matches)
        
        # Add components with same CMU ID and similar descriptions
        if cmu_id:
            for other_id in cmu_matches.get(cmu_id, []):
                if other_id != comp_id:
                    # Find the other component's description
                    other_comp = next((c for c in components if c.get("_id") == other_id), None)
                    if other_comp:
                        other_desc = other_comp.get("Description of CMU Components", "").strip().lower()
                        # Use similarity threshold
                        if desc and other_desc and (desc in other_desc or other_desc in desc):
                            potential_dupes.add(other_id)
        
        if potential_dupes:
            duplicates[comp_id] = list(potential_dupes)
    
    return duplicates


def fetch_components_for_cmu_id(cmu_id, limit=None, page=1, per_page=100, sort_order="desc"):
    """
    Fetch components for a given CMU ID using a multi-term search approach 
    that handles space-separated queries properly.
    """
    import time
    import logging
    from django.core.cache import cache
    from ..models import Component
    from ..utils import get_cache_key
    
    logger = logging.getLogger(__name__)
    start_time = time.time()
    
    # For company name searches that might return too many components, set a sane default limit
    # This helps with searches like "Tata Steel" which would otherwise return hundreds of components
    default_component_limit = 50  # Default max components to show initially
    
    # Check cache first for performance
    # Include sort_order in the cache key to ensure different sort orders have different cache entries
    components_cache_key = get_cache_key(f"components_for_cmu_p{page}_s{per_page}_sort{sort_order}", cmu_id)
    cached_components = cache.get(components_cache_key)
    
    # If found in cache, apply pagination and return
    if cached_components:
        logger.info(f"Found {len(cached_components)} components in cache for '{cmu_id}'")
        total_count = len(cached_components)
        
        # Apply pagination
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_components = cached_components[start_idx:end_idx]
        
        # Add duplicate detection to metadata
        duplicates = detect_potential_duplicates(paginated_components)
        metadata = {
            "total_count": total_count,
            "page": page,
            "per_page": per_page,
            "total_pages": (total_count + per_page - 1) // per_page,
            "source": "cache",
            "processing_time": time.time() - start_time,
            "potential_duplicates": duplicates,
            "has_duplicates": bool(duplicates)
        }
        
        return paginated_components, metadata
    
    try:
        # Impose a timeout on database queries
        from django.db import connection
        # Save original timeout
        original_timeout = connection.cursor().connection.gettimeout() if hasattr(connection.cursor().connection, 'gettimeout') else None
        
        # Set a reasonable timeout to prevent hanging
        try:
            if hasattr(connection.cursor().connection, 'settimeout'):
                connection.cursor().connection.settimeout(15)  # 15 seconds max
        except:
            pass  # Some backends don't support timeout
        
        # First, check if this is a direct CMU ID search
        if ' ' not in cmu_id and (cmu_id.upper().startswith('CM') or cmu_id.upper().startswith('T-')):
            # Direct CMU ID search - case insensitive  
            queryset = Component.objects.filter(cmu_id__iexact=cmu_id)
            logger.info(f"Direct CMU ID search for: {cmu_id}")
        else:
            # Multi-term search approach - split query into terms
            query_terms = cmu_id.lower().split()
            
            # Start with empty query
            from django.db.models import Q
            query_filter = Q()
            
            # For multi-term searches with many terms, limit the query complexity
            if len(query_terms) > 3:
                # Prioritize the first few terms for performance
                query_terms = query_terms[:3]
                logger.info(f"Limiting search to first 3 terms for query: '{cmu_id}'")
            
            # Process each search term independently
            for term in query_terms:
                if len(term) >= 3:  # Only use terms with at least 3 characters
                    term_filter = (
                        Q(company_name__icontains=term) | 
                        Q(location__icontains=term) | 
                        Q(description__icontains=term) |
                        Q(cmu_id__icontains=term)
                    )
                    # Add each term with OR logic
                    query_filter |= term_filter
            
            # Apply the combined filter
            queryset = Component.objects.filter(query_filter)
            logger.info(f"Multi-term search for: {cmu_id}")
        
        # Make the queryset distinct to avoid duplicates
        queryset = queryset.distinct()
        
        # Get total count for pagination
        total_count = queryset.count()
        logger.info(f"Found {total_count} total components for query: {cmu_id}")
        
        # Convert to integer to avoid comparison issues
        total_count_int = int(total_count) if isinstance(total_count, (str, float)) else total_count
        
        # For searches returning many results, check if this is likely a company name search
        # and apply a more reasonable limit for the initial display
        is_likely_company_search = False
        if total_count_int > 100 and ' ' not in cmu_id and not cmu_id.upper().startswith('CM') and not cmu_id.upper().startswith('T-'):
            # Check if this query matches any company names
            company_match_count = Component.objects.filter(company_name__icontains=cmu_id).values('company_name').distinct().count()
            if company_match_count > 0:
                is_likely_company_search = True
                logger.info(f"Query '{cmu_id}' appears to be a company name search with {company_match_count} matching companies")
        
        # If we have a very large result set, limit it further 
        if total_count_int > 5000:
            logger.warning(f"Very large result set ({total_count_int}) for query '{cmu_id}', limiting to 5000")
            queryset = queryset[:5000]
            total_count_int = min(total_count_int, 5000)
        # For likely company searches, set a lower initial limit
        elif is_likely_company_search and per_page > default_component_limit:
            logger.info(f"Limiting initial component display to {default_component_limit} for company search '{cmu_id}'")
            per_page = default_component_limit
        
        # Restore original timeout if we changed it
        try:
            if hasattr(connection.cursor().connection, 'settimeout') and original_timeout:
                connection.cursor().connection.settimeout(original_timeout)
        except:
            pass
        
        # Apply sorting based on sort_order
        if sort_order == "asc":
            # Oldest first - ascending delivery year
            queryset = queryset.order_by('delivery_year')
            logger.info("Sorting by delivery year (oldest first)")
        else:
            # Newest first - descending delivery year (default)
            queryset = queryset.order_by('-delivery_year')
            logger.info("Sorting by delivery year (newest first)")
        
        # Apply pagination
        start = (page - 1) * per_page
        end = start + per_page
        paginated_queryset = queryset[start:end]
        
        # Convert to list of dictionaries
        components = []
        for comp in paginated_queryset:
            # Create a dictionary representation
            comp_dict = {
                "CMU ID": comp.cmu_id,
                "Location and Post Code": comp.location or '',
                "Description of CMU Components": comp.description or '',
                "Generating Technology Class": comp.technology or '',
                "Company Name": comp.company_name or '',
                "Auction Name": comp.auction_name or '',
                "Delivery Year": comp.delivery_year or '',
                "Status": comp.status or '',
                "Type": comp.type or '',
                "_id": comp.id,  # Use database ID (pk) for links
                "component_id_str": comp.component_id or '' # Add the string component_id from the model field (might be source _id)
            }
            
            # Add any additional data if available, AND try to get the actual Component ID
            actual_component_id = None
            if comp.additional_data:
                for key, value in comp.additional_data.items():
                    if key not in comp_dict:
                        comp_dict[key] = value
                    # Explicitly look for the key 'Component ID' from the source data
                    if key == "Component ID": 
                        actual_component_id = value
            
            # Add the actual component ID if found
            comp_dict["actual_component_id"] = actual_component_id or ''
                        
            components.append(comp_dict)
        
        logger.info(f"Returning {len(components)} components for page {page}")
        
        # Cache results if not too large
        if total_count_int <= 1000:  # Only cache reasonably sized results
            # Store all component results (not just paginated) in the cache with the sort order
            all_components = list(queryset)
            cache.set(components_cache_key, all_components, 3600)  # 1 hour
        elif total_count_int <= 2000:
            all_components = list(queryset)
            cache.set(components_cache_key, all_components, 1800)  # 30 minutes
        elif total_count_int <= 5000:
            all_components = list(queryset)
            cache.set(components_cache_key, all_components, 600)   # 10 minutes
        
        # Add duplicate detection to metadata
        duplicates = detect_potential_duplicates(components)
        metadata = {
            "total_count": total_count_int,
            "page": page,
            "per_page": per_page,
            "total_pages": (total_count_int + per_page - 1) // per_page if total_count_int > 0 else 1,
            "source": "database",
            "processing_time": time.time() - start_time,
            "potential_duplicates": duplicates,
            "has_duplicates": bool(duplicates)
        }
        
        return components, metadata
    
    except Exception as e:
        logger.exception(f"Error in database search for '{cmu_id}': {str(e)}")
        
        # API FALLBACK: If database query fails, try API
        try:
            api_start_time = time.time()
            params = {
                "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
                "q": cmu_id,
                "limit": per_page,
                "offset": (page-1) * per_page
            }
            
            response = requests.get(
                "https://data.nationalgrideso.com/api/3/action/datastore_search",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json().get("result", {})
                all_api_components = result.get("records", [])
                
                # Sort the API results based on sort_order
                if all_api_components:
                    try:
                        if sort_order == "asc":
                            # Sort oldest first
                            all_api_components = sorted(all_api_components, key=lambda x: x.get("Delivery Year", "0"))
                            logger.info("API results sorted by delivery year (oldest first)")
                        else:
                            # Sort newest first (default)
                            all_api_components = sorted(all_api_components, key=lambda x: x.get("Delivery Year", "0"), reverse=True)
                            logger.info("API results sorted by delivery year (newest first)")
                    except Exception as sort_error:
                        logger.error(f"Error sorting API results: {str(sort_error)}")
                
                # Try to get total count
                total_count = result.get("total", len(all_api_components))
                # Ensure total_count is an integer
                total_count_int = int(total_count) if isinstance(total_count, (str, float)) else total_count
                
                # Try to save to database for future use
                try:
                    save_components_to_database(cmu_id, all_api_components)
                except Exception as db_error:
                    logger.error(f"Error saving to database: {str(db_error)}")
                
                # Add duplicate detection to metadata
                duplicates = detect_potential_duplicates(all_api_components)
                metadata = {
                    "total_count": total_count_int,
                    "page": page,
                    "per_page": per_page,
                    "total_pages": (total_count_int + per_page - 1) // per_page,
                    "source": "api",
                    "processing_time": time.time() - start_time,
                    "api_time": time.time() - api_start_time,
                    "potential_duplicates": duplicates,
                    "has_duplicates": bool(duplicates)
                }
                
                return all_api_components, metadata
            else:
                logger.error(f"API error: {response.status_code}")
                return [], {"error": f"API error: {response.status_code}"}
        except Exception as api_error:
            logger.exception(f"API fallback error: {str(api_error)}")
            return [], {
                "error": str(e) + " (API fallback also failed: " + str(api_error) + ")",
                "total_count": 0,
                "processing_time": time.time() - start_time
            }


def save_components_to_database(cmu_id, components):
    """
    Save components to the database.
    This is called whenever we fetch components from the API.
    """
    from django.db import transaction
    from ..models import Component
    
    if not components:
        return
    
    # Extract component_ids for efficient existence check
    component_ids = [c.get("_id", "") for c in components if c.get("_id")]
    
    # Only do one database query to find all existing IDs
    existing_ids = set()
    if component_ids:
        existing_ids = set(Component.objects.filter(
            component_id__in=component_ids
        ).values_list('component_id', flat=True))
    
    # Create list of new components only
    new_components = []
    for component in components:
        component_id = component.get("_id", "")
        
        # Skip if we already have this component in the database
        # FIXED THIS CONDITION - only skip if it's already in existing_ids
        if component_id and component_id in existing_ids:
            continue
        
        # --- Calculate derated_capacity_mw --- 
        derated_capacity_mw = None
        capacity_str = component.get("De-Rated Capacity")
        if capacity_str is not None:
            try:
                derated_capacity_mw = float(capacity_str)
            except (ValueError, TypeError):
                pass # Keep as None if conversion fails
        # --- End calculation ---
        
        # Extract standard fields
        new_components.append(Component(
            component_id=component_id,
            cmu_id=cmu_id,
            location=component.get("Location and Post Code", ""),
            description=component.get("Description of CMU Components", ""),
            technology=component.get("Generating Technology Class", ""),
            company_name=component.get("Company Name", ""),
            auction_name=component.get("Auction Name", ""),
            delivery_year=component.get("Delivery Year", ""),
            status=component.get("Status", ""),
            type=component.get("Type", ""),
            additional_data=component,
            derated_capacity_mw=derated_capacity_mw # Set the new field
        ))
    
    # Only do the bulk create if we have new components
    if new_components:
        # Use a transaction for better performance and atomicity
        with transaction.atomic():
            # Bulk create all components at once
            Component.objects.bulk_create(new_components, ignore_conflicts=True)


def fetch_component_search_results(query, limit=1000, sort_order="desc"):
    """
    Fetch components based on a search query.
    Legacy function that now uses fetch_components_for_cmu_id.
    """
    components, metadata = fetch_components_for_cmu_id(query, page=1, per_page=limit, sort_order=sort_order)
    api_time = metadata.get("processing_time", 0)
    error = metadata.get("error")
    
    return components, api_time, error


def get_component_total_count():
    """Get the total count of components in the database."""
    from ..models import Component
    total_cache_key = "components_overall_total"
    overall_total = cache.get(total_cache_key)
    api_time = 0

    if overall_total is None:
        try:
            start_time = time.time()
            # Get count from database 
            overall_total = Component.objects.count()
            api_time = time.time() - start_time
            cache.set(total_cache_key, overall_total, 3600)  # Cache for 1 hour
        except Exception as e:
            print(f"Error fetching overall total from DB: {e}")
            
            # Fall back to API if database fails
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
            except Exception as api_e:
                print(f"Error fetching overall total from API: {api_e}")
                api_time = time.time() - start_time if 'start_time' in locals() else 0
                overall_total = 0

    return overall_total, api_time


def get_accurate_total_count(query):
    """
    Get an accurate count of components matching a query.
    Now uses database with multi-term search.
    """
    import time
    import logging
    from django.db.models import Q
    from ..models import Component
    
    logger = logging.getLogger(__name__)
    start_time = time.time()
    
    try:
        # Use the same multi-term search logic as fetch_components_for_cmu_id
        if ' ' not in query and (query.upper().startswith('CM') or query.upper().startswith('T-')):
            # Direct CMU ID search
            total_count = Component.objects.filter(cmu_id__iexact=query).count()
        else:
            # Multi-term search
            query_terms = query.lower().split()
            query_filter = Q()
            
            for term in query_terms:
                if len(term) >= 3:
                    term_filter = (
                        Q(company_name__icontains=term) | 
                        Q(location__icontains=term) | 
                        Q(description__icontains=term) |
                        Q(cmu_id__icontains=term)
                    )
                    query_filter |= term_filter
            
            total_count = Component.objects.filter(query_filter).distinct().count()
            
        logger.info(f"Database reports {total_count} total matching components for '{query}' in {time.time() - start_time:.2f}s")
        return total_count
        
    except Exception as e:
        logger.exception(f"Error getting accurate count from database: {str(e)}")
        
        # Fall back to original implementation using API
        try:
            count_params = {
                "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
                "q": query,
                "limit": 1
            }
            
            count_response = requests.get(
                "https://data.nationalgrideso.com/api/3/action/datastore_search",
                params=count_params,
                timeout=10
            )
            
            if count_response.status_code == 200:
                count_result = count_response.json().get("result", {})
                initial_count = count_result.get("total", 0)
                logger.info(f"API reports {initial_count} total matching components")
                return initial_count
            else:
                return 0
        except Exception as api_e:
            logger.exception(f"API fallback error for accurate count: {str(api_e)}")
            return 0


def search_all_json_files(query, page=1, per_page=500, sort_order="desc"):
    """
    DEPRECATED: Search JSON files for components.
    This function now redirects to database search.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    logger.warning("search_all_json_files is deprecated, using database search instead")
    return fetch_components_for_cmu_id(query, page=page, per_page=per_page, sort_order=sort_order)


def get_cmu_data_by_id(cmu_id):
    """
    Fetch additional data for a specific CMU ID.
    Prioritizes fetching the full raw data from cmu_data.json.
    Falls back to basic details from the database if JSON fails.
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
        
    # --- MODIFIED LOGIC: Prioritize JSON --- 
    # Try getting full raw data from JSON first
    all_cmu_data = get_cmu_data_from_json()
    if all_cmu_data:
        # Find the matching CMU ID
        for cmu_data_item in all_cmu_data:
            # Compare as strings for robustness
            if str(cmu_data_item.get("CMU ID", "")) == str(cmu_id):
                logger.info(f"Found matching raw CMU data for {cmu_id} in JSON")
                # Cache the full result for future use (1 hour)
                cache.set(cache_key, cmu_data_item, 3600)
                return cmu_data_item # Return the full dictionary
        logger.info(f"CMU ID {cmu_id} not found in cmu_data.json")
    else:
        logger.warning("No CMU data available from JSON file.")

    # --- Fallback to Database if not found in JSON --- 
    logger.info(f"Falling back to database lookup for basic CMU details for {cmu_id}")
    try:
        component = Component.objects.filter(cmu_id=cmu_id).first()
        if component:
            logger.info(f"Found basic CMU data in database for {cmu_id}")
            # Construct limited dictionary as fallback
            fallback_data = {
                "CMU ID": cmu_id,
                "Name of Applicant": component.company_name,
                "Delivery Year": component.delivery_year,
                "Auction Name": component.auction_name,
                "_source_note": "Limited data from DB fallback"
            }
            # Cache the limited fallback data
            cache.set(cache_key, fallback_data, 3600) # Cache fallback for same duration
            return fallback_data
    except Exception as db_e:
        logger.warning(f"Error during database fallback lookup for CMU {cmu_id}: {str(db_e)}")
    
    # If not found anywhere
    logger.warning(f"No CMU data found for {cmu_id} in JSON or Database fallback.")
    return None


def analyze_component_duplicates(components):
    """
    Analyze a list of components to identify duplicates.
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


def get_components_from_database(cmu_id=None, component_id=None, location=None, company_name=None, search_term=None, limit=100, page=None, per_page=None, sort_order="desc"):
    """
    Fetch components from the database based on various filters with optimization.
    Accepts specific filters (cmu_id, location, company_name) and a general search_term.
    Includes pagination support.
    Returns a tuple of (components list, total count).
    """
    import logging
    from django.db.models import Q
    
    # Import the enhanced postcode functions
    from .postcode_helpers import get_all_postcodes_for_area, get_area_for_any_postcode
    logger = logging.getLogger(__name__)
    
    # Try importing PostgreSQL specific tools
    try:
        from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank
        using_postgres = True
    except ImportError:
        using_postgres = False
        logger.warning("PostgreSQL specific search features not available. Falling back to basic icontains.")

    logger.info(f"DB Query: cmu={cmu_id}, comp={component_id}, loc={location}, company={company_name}, term={search_term}, page={page}, per_page={per_page}, sort={sort_order}")
    
    # Build the query
    query_set = Component.objects.all()
    filters = Q()
    has_filter = False
    vector = None # Initialize vector here
    query = None # Initialize query here
    rank_annotation = None # Initialize rank_annotation here

    if cmu_id:
        filters &= Q(cmu_id__iexact=cmu_id)
        has_filter = True
    
    if component_id:
        filters &= Q(component_id=component_id)
        has_filter = True
        
    if location:
        # Starting with a tighter location filter - only exact matches have priority
        logger.info(f"Starting location search with filters for: {location}")
        
        # Set up a more precise filtering approach using OR conditions with priorities
        exact_location_filter = Q(location__iexact=location)  # Exact match has highest priority
        
        # Create a filter for locations that start with our search term
        # This catches places where the location is "Manchester M1 1AA" when searching for "Manchester"
        starts_with_filter = Q(location__istartswith=f"{location} ") | Q(location__istartswith=f"{location},")
        
        # Check if location is an area name and get related postcodes
        related_postcodes = get_all_postcodes_for_area(location)
        postcode_filter = Q()
        if related_postcodes:
            # Limit number of postcodes to prevent overly broad searches
            # Focus on the most relevant postcodes (usually the first 5-10 are most central to the area)
            limited_postcodes = related_postcodes[:10]  # Take at most 10 postcodes
            logger.info(f"Found {len(related_postcodes)} related postcodes for area: {location}, using {len(limited_postcodes)}")
            
            # For each postcode, create a filter that requires the postcode to appear as a whole word
            # This prevents matching substrings like "SE1" within "ESSEX"
            for postcode in limited_postcodes:
                # To match whole postcodes, look for postcodes with space/comma before or after
                # This avoids matching "SE1" in "ESSEX" but catches "SE1 9RT" or "SE1, London"
                postcode_filter |= (
                    Q(location__iregex=f"\\b{postcode}\\b") |  # Match SE1 as a whole word
                    Q(location__iregex=f"\\b{postcode}[0-9A-Z]") |  # Match SE1 followed by more characters like SE16
                    Q(location__icontains=f" {postcode},") |  # Match " SE1,"
                    Q(location__icontains=f" {postcode} ")    # Match " SE1 "
                )
        
        # Check if location might be a postcode and get related area
        areas = get_area_for_any_postcode(location)
        area_filter = Q()
        if areas:
            logger.info(f"Found related areas for postcode {location}: {areas}")
            # Combine filters for all found areas using OR
            for area_name in areas:
                if area_name:
                    # Only match the area name as a complete word, not as a substring
                    area_filter |= Q(location__iregex=f"\\b{area_name}\\b")
        
        # Combine all filters with priority - exact matches first, then starts_with, then postcodes, then area names
        # Use | (OR) between filters, so any match will work, but prioritize the filters when combining
        location_filter = exact_location_filter | starts_with_filter | postcode_filter | area_filter
        
        # Basic substring match as last resort, but with lower weight in results
        contains_filter = Q(location__icontains=location)
        
        # Combine all filters - priority filters have precedence, contains is last resort
        filters &= (location_filter | contains_filter)
        has_filter = True

    if company_name:
        filters &= Q(company_name__icontains=company_name)
        has_filter = True
        
    # Apply general search term if provided - use more precise matching to ensure components
    # actually contain the search term in at least one field
    if search_term:
        search_term_lower = search_term.lower().strip()
        
        # Special handling for "vital" search - we want to prioritize VITAL ENERGI and exclude locations in Leeds
        if search_term_lower == "vital":
            logger.info("Applying strict 'vital' search filter (VITAL ENERGI, exclude Leeds)")
            # ALWAYS apply this specific filter for 'vital', overwriting any previous general search filters
            filters = Q(company_name__icontains="VITAL ENERGI") & ~Q(location__icontains="Leeds")
            has_filter = True # Ensure we mark that a filter has been applied
        else:
            # --- MODIFIED: Integrate location expansion and RANKING into general search --- 
            logger.info(f"Applying general search for: '{search_term_lower}'")
            
            # Initialize filters
            filters = Q() 
            location_expansion_filter = Q() # Separate for clarity
            
            # Try to expand location based on search term using helper functions
            try:
                related_postcodes = get_all_postcodes_for_area(search_term_lower)
                if related_postcodes:
                    limited_postcodes = related_postcodes[:10] # Limit for performance
                    logger.info(f"Expanding search with {len(limited_postcodes)} related postcodes for area: {search_term_lower}")
                    for postcode in limited_postcodes:
                        location_expansion_filter |= (
                            Q(location__iregex=f"\\b{postcode}\\b") | 
                            Q(location__iregex=f"\\b{postcode}[0-9A-Z]") | 
                            Q(location__icontains=f" {postcode},") | 
                            Q(location__icontains=f" {postcode} ")
                        )
                
                related_areas = get_area_for_any_postcode(search_term_lower)
                if related_areas:
                    logger.info(f"Expanding search with related areas: {related_areas}")
                    # Combine filters for all found areas using OR
                    for area_name in related_areas:
                        if area_name:
                            location_expansion_filter |= Q(location__iregex=f"\\b{area_name}\\b")

            except Exception as e:
                logger.error(f"Error during location expansion for term '{search_term_lower}': {e}")
                # Continue without expansion if helpers fail

            # --- Ranking Logic --- 
            if using_postgres:
                # Define search vector and query for ranking
                vector = SearchVector('company_name', 'location', 'description', 'cmu_id', config='english') # Added cmu_id to vector
                query = SearchQuery(search_term_lower, config='english')
                # --- ANNOTATE FIRST --- 
                query_set = query_set.annotate(searchvector=vector)
                # Filter based on search query OR location expansion
                filters = Q(searchvector=query) | location_expansion_filter
                # Annotate for ranking
                rank_annotation = SearchRank(vector, query)
            else:
                # Fallback for non-PostgreSQL: standard icontains OR location expansion
                base_text_filter = (
                    Q(company_name__icontains=search_term_lower) | 
                    Q(location__icontains=search_term_lower) | 
                    Q(description__icontains=search_term_lower) | 
                    Q(cmu_id__icontains=search_term_lower)
                )
                filters = base_text_filter | location_expansion_filter

            has_filter = True
            # --- END MODIFICATION ---
            
    # Only apply filters if any were added
    if has_filter:
        query_set = query_set.filter(filters)
    else:
        # If no filters/search term, maybe return empty or all? 
        # Returning empty for now to avoid loading everything.
        logger.warning("get_components_from_database called with no filters or search term. Returning empty.")
        return [], 0

    # Apply distinct if complex filters were used
    if has_filter:
        query_set = query_set.distinct()
    
    # Get total count before applying pagination
    total_count = query_set.count()
    logger.info(f"Total matching records: {total_count}")
    
    # Apply ordering based on sort_order
    # --- Modified Ordering: Prioritize Rank if available --- 
    if rank_annotation:
        logger.info(f"Ordering by relevance rank, then delivery year ({sort_order})")
        if sort_order == "asc":
            query_set = query_set.annotate(rank=rank_annotation).order_by('-rank', 'delivery_year')
        else:
            query_set = query_set.annotate(rank=rank_annotation).order_by('-rank', '-delivery_year')
    else:
        # Fallback ordering if not using ranking
        logger.info(f"Ordering by delivery year only ({sort_order})")
        if sort_order == "asc":
            query_set = query_set.order_by('delivery_year')
        else:
            query_set = query_set.order_by('-delivery_year')
    # --- End Modified Ordering ---

    # Apply pagination if provided
    if page is not None and per_page is not None:
        start = (page - 1) * per_page
        paginated_queryset = query_set[start:start + per_page]
    # Apply limit if pagination not used
    elif limit:
        paginated_queryset = query_set[:limit]
    else:
        paginated_queryset = query_set # Or apply a default limit?
    
    # Execute query and convert to list of dictionaries
    components = []
    for comp in paginated_queryset: # Iterate over the paginated/limited queryset
        comp_dict = {
            "CMU ID": comp.cmu_id,
            "Location and Post Code": comp.location or '',
            "Description of CMU Components": comp.description or '',
            "Generating Technology Class": comp.technology or '',
            "Company Name": comp.company_name or '',
            "Auction Name": comp.auction_name or '',
            "Delivery Year": comp.delivery_year or '',
            "_id": comp.id, # Use pk as _id for linking
            "component_id_str": comp.component_id or '' # Keep original string ID if present
        }
        if comp.additional_data:
            for key, value in comp.additional_data.items():
                if key not in comp_dict:
                    comp_dict[key] = value
        components.append(comp_dict)
    
    logger.info(f"Found {len(components)} components in database matching filters.")
    return components, total_count