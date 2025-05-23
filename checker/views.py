from django.shortcuts import render, redirect, get_object_or_404
from django.http import HttpResponse, JsonResponse, Http404, HttpResponseBadRequest
from django.views.decorators.http import require_http_methods
from django.core.paginator import Paginator
import urllib.parse
from django.conf import settings
import os
import json
import glob
from django.db.models import Count, Q, Sum
# Remove Component import from here
# Remove unused checker import
# from capacity_checker import checker 

# Import service functions first
from .services.company_search import search_companies_service, get_company_years, get_cmu_details, company_detail # Import company_detail only once
from .services.component_search import search_components_service
from .services.component_detail import get_component_details
from .utils import safe_url_param, from_url_param, normalize
from .services.data_access import get_component_data_from_json, get_json_path, fetch_components_for_cmu_id

# Now import the models
from .models import Component, CMURegistry

# Define logger after imports
import logging
import heapq # Import heapq for efficient top N selection
logger = logging.getLogger(__name__)

import time
import re
import pandas as pd
from django.core.cache import cache

from . import services
from .services import data_access
from django.template.loader import render_to_string

import stripe
from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.urls import reverse
import hashlib
from django.views.decorators.cache import cache_page
from django.utils.decorators import method_decorator
from django.core.cache import caches

# Configure Stripe with your secret key
stripe.api_key = settings.STRIPE_SECRET_KEY

# Clear all search caches for previously cached queries that might return irrelevant results
# This is a one-time action when the server starts to ensure consistency
try:
    all_keys = cache.keys("search_results_*")
    if all_keys:
        logger.info(f"Clearing {len(all_keys)} cached search queries for consistency")
        cache.delete_many(all_keys)
except:
    # Some cache backends don't support keys() method
    logger.info("Unable to bulk clear search cache - will be refreshed gradually")

import traceback

# --- Technology Mapping --- START ---
TECHNOLOGY_MAPPING = {
    # Exact Matches First
    "Combined Heat and Power (CHP)": "CHP",
    "Solar Photovoltaic": "Solar",
    "Onshore Wind": "Wind",
    "Offshore Wind": "Wind",
    "Open Cycle Gas Turbine (OCGT)": "Gas",
    "Combined Cycle Gas Turbine (CCGT)": "Gas",
    "Reciprocating engines": "Gas", # Or maybe "Engines"?
    "Energy from Waste": "Biomass", # Or "Waste"?
    "Coal/biomass": "Biomass",
    "Storage": "Battery",
    # Add missing OCGT and engine technologies
    "OCGT and Reciprocating Engines": "Gas",
    "OCGT and Reciprocating Engines (Fuel Type - Diesel)": "Gas",
    "Oil-fired steam generators": "Gas",
    # Add other known exact full names here...
    "Hydro": "Hydro",
    "Nuclear": "Nuclear",
    "Coal": "Coal",
    "Biomass": "Biomass",
    "Gas": "Gas", # If plain "Gas" exists
    "DSR": "DSR",
    "Interconnector": "Interconnector", # Simplified?
    "BritNED (Netherlands)": "Interconnector",
    "Eleclink (France)": "Interconnector",
    "EWIC (Ireland)": "Interconnector",
    "EWIC (Republic of Ireland)": "Interconnector",
    "Greenlink (Republic of Ireland)": "Interconnector",
    "IFA2 (France)": "Interconnector",
    "IFA (France)": "Interconnector",
    "Moyle (Northern Ireland)": "Interconnector",
    "NEMO (Belgium)": "Interconnector",
    "NeuConnect (Germany)": "Interconnector",
    "NSL (Norway)": "Interconnector",
    "VikingLink (Denmark)": "Interconnector",
}

# Fallback mapping based on keywords (case-insensitive)
# Order matters here - more specific keywords first
KEYWORD_MAPPING = [
    ("storage", "Battery"),
    ("battery", "Battery"),
    ("wind", "Wind"),
    ("solar", "Solar"),
    ("hydro", "Hydro"),
    ("nuclear", "Nuclear"),
    ("interconnector", "Interconnector"),
    ("gas", "Gas"), # Catch gas turbines, etc.
    ("chp", "CHP"),
    ("biomass", "Biomass"),
    ("waste", "Biomass"), # Map waste to biomass?
    ("coal", "Coal"),
    ("dsr", "DSR"),
]

def get_simplified_technology(full_tech_name):
    if not full_tech_name:
        return "Unknown"
    
    # 1. Check for exact match in the primary mapping
    if full_tech_name in TECHNOLOGY_MAPPING:
        return TECHNOLOGY_MAPPING[full_tech_name]
    
    # 2. Check for keyword matches (case-insensitive)
    lower_tech_name = full_tech_name.lower()
    for keyword, simplified_name in KEYWORD_MAPPING:
        if keyword in lower_tech_name:
            return simplified_name
            
    # 3. If no mapping found, return the original name (or a default like "Other")
    # Returning original might lead to grey dots if not in frontend map
    return "Other" # Default to "Other" if no match
# --- Technology Mapping --- END ---

def search_companies(request):
    """View function for searching companies using the unified search"""
    # Get the query parameter
    query = request.GET.get("q", "").strip()
    
    # If no query, or if query, delegate ALL logic (company+component search & render)
    # to search_components_service. It will handle defaults and context.
    logger.info(f"Delegating search for query '{query}' to search_components_service")
    return search_components_service(request)


def search_components(request):
    """View function FOR REDIRECTING component searches or handling Ajax requests""" 
    # This view handles two cases:
    # 1. Normal requests: redirects back to the main search page with appropriate params
    # 2. Ajax requests: returns JSON response for "Load More" functionality
    
    # Get the query parameter
    query = request.GET.get("q", "").strip()
    
    # Check if this is an Ajax request (for load more functionality)
    is_ajax = request.GET.get('format') == 'json' or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
    
    if is_ajax:
        try:
            # Handle the Ajax request for Load More functionality
            # Call search_components_service with return_json=True to get JSON response
            logger.info(f"Handling AJAX request for query '{query}' from /components/ endpoint")
            
            # Ensure the normalize filter is available by explicitly loading it
            from django.template.defaultfilters import register
            from .templatetags.checker_tags import normalize_filter
            
            # Load the template directly to make sure we can render it
            from django.template.loader import get_template
            # Pre-load template to check for errors
            template = get_template('checker/components/_group_result_item.html')
            
            return search_components_service(request, return_json=True)
            
        except Exception as e:
            # Log the error and return a JSON error response
            logger.error(f"Error handling AJAX request: {str(e)}")
            from django.http import JsonResponse
            return JsonResponse({
                'error': str(e),
                'success': False,
                'items': []
            }, status=500)
    
    # Otherwise handle as a normal request (redirect to home page)
    # If no query, just redirect to home
    if not query:
        return redirect("/")
        
    # Smart pagination - adjust page size based on query complexity
    page = int(request.GET.get("page", 1))
    
    # --- USE 50 as the base/max per_page for component searches --- 
    per_page = 50 
    # Adjust lower if needed based on query complexity (optional refinement)
    # if len(query) < 5 or ' ' in query:  
    #     per_page = 30 
    # elif len(query) < 3:  
    #     per_page = 15
        
    # Add parameters to the redirect URL (pointing back to the main view)
    redirect_url = f"/?q={urllib.parse.quote(query)}&page={page}&per_page={per_page}"
    
    # Add all other parameters from the request
    for param, value in request.GET.items():
        if param not in ['q', 'page', 'per_page'] and value:
            redirect_url += f"&{param}={urllib.parse.quote(value)}"
        
    # Log the pagination settings to help with debugging
    logger.info(f"Redirecting component search from /components/ for query '{query}': page={page}, per_page={per_page}")
    
    return redirect(redirect_url)


@require_http_methods(["GET"])
def htmx_company_years(request, company_id, year, auction_name=None):
    """HTMX endpoint for lazy loading company year details"""
    # Convert year and auction_name from URL format (underscores) back to spaces
    year = from_url_param(year)
    if auction_name:
        auction_name = from_url_param(auction_name)

    # Get the HTML for the year details
    years_html = get_company_years(company_id, year, auction_name)
    return HttpResponse(years_html)


@require_http_methods(["GET"])
def component_detail(request, pk):
    """View function for component details page"""
    return get_component_details(request, pk)


@require_http_methods(["GET"])
def htmx_auction_components(request, company_id, year, auction_name):
    """
    HTMX endpoint for getting auction components
    """
    import time
    from django.core.cache import cache
    
    # Create a cache key based on the request parameters
    cache_key = f"auction_components:{company_id}:{year}:{auction_name}"
    
    # Check if we have a cached response
    cached_response = cache.get(cache_key)
    if cached_response:
        # Add cache control headers
        cached_response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        cached_response['Pragma'] = 'no-cache'
        cached_response['Expires'] = '0'
        cached_response['X-Source'] = 'cache'
        return cached_response
    
    # Add request start time for performance tracking
    request.META['HTTP_REQUEST_START_TIME'] = str(time.time())
    
    # Pass to service function
    from .services.company_search import auction_components
    
    # Get the HTML response
    response = auction_components(request, company_id, year, auction_name)
    
    # Add cache control headers to prevent caching
    response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response['Pragma'] = 'no-cache'
    response['Expires'] = '0'
    response['X-Source'] = 'fresh'
    
    # Cache the response for a short time
    cache.set(cache_key, response, 30)  # Cache for 30 seconds
    
    return response

def debug_mapping_cache(request):
    """Debug view to examine the cached CMU ID to company name mapping"""
    from django.core.cache import cache
    from django.http import JsonResponse
    
    # Get mapping from cache
    cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
    
    # Build response
    response = {
        "cache_exists": "cmu_to_company_mapping" in cache,
        "mapping_count": len(cmu_to_company_mapping),
        "sample_entries": dict(list(cmu_to_company_mapping.items())[:10]) if cmu_to_company_mapping else {}
    }
    
    # If company name is provided, look it up in the mapping
    company_name = request.GET.get("company", "")
    if company_name:
        # Find all CMU IDs for this company
        company_cmus = []
        for cmu_id, comp_name in cmu_to_company_mapping.items():
            if comp_name == company_name:
                company_cmus.append(cmu_id)
                
        response["company_name"] = company_name
        response["company_cmus"] = company_cmus
        response["company_cmu_count"] = len(company_cmus)
        
    # If cmu_id is provided, look it up in the mapping
    cmu_id = request.GET.get("cmu_id", "")
    if cmu_id:
        company_name = cmu_to_company_mapping.get(cmu_id, "")
        
        # Try case-insensitive match if needed
        if not company_name:
            for mapping_cmu_id, mapping_company in cmu_to_company_mapping.items():
                if mapping_cmu_id.lower() == cmu_id.lower():
                    company_name = mapping_company
                    break
                    
        response["cmu_id"] = cmu_id
        response["company_name_for_cmu"] = company_name
        
    return JsonResponse(response)


def debug_component_retrieval(request, cmu_id):
    """Debug view to troubleshoot component retrieval for a specific CMU ID"""
    from django.http import JsonResponse
    from .services.data_access import get_component_data_from_json, get_json_path
    import os
    
    # Get component data using the standard function
    components = get_component_data_from_json(cmu_id)
    
    # Get expected path for debugging
    json_path = get_json_path(cmu_id)
    
    # Create response
    response = {
        "cmu_id": cmu_id,
        "components_found": components is not None,
        "components_count": len(components) if components else 0,
        "json_path": json_path,
        "json_file_exists": os.path.exists(json_path) if json_path else False
    }
    
    # Include some component sample data if available
    if components and len(components) > 0:
        response["sample_component"] = components[0]
        
        # Check if components have Company Name
        has_company_name = any(c.get("Company Name") for c in components if isinstance(c, dict))
        response["has_company_name"] = has_company_name
        
        if has_company_name:
            company_names = set()
            for component in components:
                if isinstance(component, dict) and component.get("Company Name"):
                    company_names.add(component["Company Name"])
            response["company_names"] = list(company_names)
    
    return JsonResponse(response)


def debug_company_components(request):
    """Debug view to examine components for a specific company"""
    company_name = request.GET.get("company", "").strip()
    results = {}
    
    if not company_name:
        return render(request, "checker/debug_components.html", {
            "error": "Please provide a company name",
            "results": {}
        })
    
    # Get CMU dataframe to find all CMU IDs for this company
    from .services.company_search import get_cmu_dataframe
    cmu_df, _ = get_cmu_dataframe()
    
    if cmu_df is None:
        return render(request, "checker/debug_components.html", {
            "error": "Error loading CMU data",
            "company_name": company_name,
            "results": {}
        })
    
    # Find all records for this company
    company_records = cmu_df[cmu_df["Full Name"] == company_name]
    
    if company_records.empty:
        return render(request, "checker/debug_components.html", {
            "error": f"No CMU records found for company: {company_name}",
            "company_name": company_name,
            "results": {}
        })
    
    # Get all CMU IDs for this company
    cmu_ids = company_records["CMU ID"].unique().tolist()
    
    # Check each JSON file for components
    json_dir = os.path.join(settings.BASE_DIR, 'json_data')
    all_components = {}
    found_count = 0
    missing_count = 0
    
    for cmu_id in cmu_ids:
        # Get expected JSON path for this CMU ID
        json_path = get_json_path(cmu_id)
        file_exists = os.path.exists(json_path)
        
        # Get components using the standard function
        components = get_component_data_from_json(cmu_id)
        
        if components:
            found_count += 1
            all_components[cmu_id] = {
                "file_path": json_path,
                "file_exists": file_exists,
                "component_count": len(components),
                "components": components
            }
        else:
            missing_count += 1
            all_components[cmu_id] = {
                "file_path": json_path,
                "file_exists": file_exists,
                "component_count": 0,
                "components": []
            }
    
    # Also search through all JSON files for any components with this company name
    found_in_files = []
    json_files = glob.glob(os.path.join(json_dir, 'components_*.json'))
    
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                file_components = json.load(f)
                
            for file_cmu_id, cmu_components in file_components.items():
                if file_cmu_id in all_components:
                    continue  # Already processed this CMU ID
                    
                for component in cmu_components:
                    if isinstance(component, dict) and component.get("Company Name") == company_name:
                        if file_cmu_id not in all_components:
                            all_components[file_cmu_id] = {
                                "file_path": json_file,
                                "file_exists": True,
                                "component_count": len(cmu_components),
                                "components": cmu_components,
                                "note": "Found by company name, not in CMU dataframe"
                            }
                            found_in_files.append(file_cmu_id)
                        break
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
    
    return render(request, "checker/debug_components.html", {
        "company_name": company_name,
        "cmu_ids": cmu_ids,
        "all_components": all_components,
        "found_count": found_count,
        "missing_count": missing_count,
        "found_in_files": found_in_files
    })


def debug_cache(request, cmu_id):
    """Debug view to examine cache for a CMU ID"""
    from django.core.cache import cache
    from django.http import JsonResponse
    from .utils import get_cache_key
    
    debug_info = {
        "cmu_id": cmu_id,
        "cache_keys": [],
        "cache_values": {}
    }
    
    # Get direct cache key
    components_cache_key = get_cache_key("components_for_cmu", cmu_id)
    cached_components = cache.get(components_cache_key)
    
    debug_info["cache_keys"].append(components_cache_key)
    debug_info["cache_values"][components_cache_key] = {
        "exists": cached_components is not None,
        "length": len(cached_components) if cached_components else 0
    }
    
    # Get mapping
    cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
    company_name = cmu_to_company_mapping.get(cmu_id)
    
    debug_info["cmu_to_company_mapping_exists"] = "cmu_to_company_mapping" in cache
    debug_info["company_name_from_mapping"] = company_name
    
    # Get component data from JSON
    from .services.data_access import get_component_data_from_json, get_json_path
    json_path = get_json_path(cmu_id)
    components = get_component_data_from_json(cmu_id)
    
    debug_info["json_path"] = json_path
    debug_info["json_components_exists"] = components is not None
    debug_info["json_components_length"] = len(components) if components else 0
    
    return JsonResponse(debug_info)


def auction_components(request, company_id, year, auction_name):
    """
    API endpoint for fetching components for a specific auction
    """
    # Call the HTMX function directly
    return htmx_auction_components(request, company_id, year, auction_name)


def debug_auction_components(request, company_id, year, auction_name):
    """
    Debug endpoint for troubleshooting component matching issues
    """
    # Import required modules
    import json
    import re
    from django.http import JsonResponse
    from .services.data_access import get_cmu_dataframe, get_component_data_from_json, fetch_components_for_cmu_id
    from .utils import normalize
    
    # For security, only allow access to debug endpoints in development or for specific users
    # You can add more security checks if needed
    if not settings.DEBUG and not request.user.is_staff:
        return JsonResponse({"error": "Access denied. Debug endpoints are only available in development mode or for staff."}, status=403)
    
    # Start collecting debug information
    debug_info = {
        "request_info": {
            "company_id": company_id,
            "year": year,
            "auction_name": auction_name,
        },
        "company_info": {},
        "cmu_ids": [],
        "components": [],
        "matched_components": [],
        "component_matching": {
            "t_number": {"extracted": None, "matches": 0, "non_matches": 0},
            "year_pattern": {"extracted": None, "matches": 0, "non_matches": 0},
        }
    }
    
    # Step 1: Get company name from ID
    cmu_df, _ = get_cmu_dataframe()
    company_name = None
    
    if cmu_df is not None:
        from_company_id_df = cmu_df[cmu_df["Normalized Full Name"] == company_id]
        if not from_company_id_df.empty:
            company_name = from_company_id_df.iloc[0]["Full Name"]
            debug_info["company_info"]["name"] = company_name
            debug_info["company_info"]["name_source"] = "direct_match"
        else:
            # Try to find the company by de-normalizing the ID
            normalized_companies = list(cmu_df["Normalized Full Name"].unique())
            for norm_name in normalized_companies:
                if norm_name == company_id:
                    matching_rows = cmu_df[cmu_df["Normalized Full Name"] == norm_name]
                    if not matching_rows.empty:
                        company_name = matching_rows.iloc[0]["Full Name"]
                        debug_info["company_info"]["name"] = company_name
                        debug_info["company_info"]["name_source"] = "denormalized_match"
                        break
    
    if not company_name:
        debug_info["error"] = "Company not found"
        return JsonResponse(debug_info)
    
    # Step 2: Get all CMU IDs for this company
    company_records = cmu_df[cmu_df["Full Name"] == company_name]
    all_cmu_ids = company_records["CMU ID"].unique().tolist()
    debug_info["cmu_ids"] = all_cmu_ids
    debug_info["cmu_id_count"] = len(all_cmu_ids)
    
    # Step 3: Extract T-1 or T-4 from auction name
    t_number = ""
    auction_upper = auction_name.upper() if auction_name else ""
    if "T-1" in auction_upper or "T1" in auction_upper or "T 1" in auction_upper:
        t_number = "T-1"
    elif "T-4" in auction_upper or "T4" in auction_upper or "T 4" in auction_upper:
        t_number = "T-4"
    
    debug_info["component_matching"]["t_number"]["extracted"] = t_number
    
    # Step 4: Extract year range pattern from auction name
    year_range_pattern = ""
    if auction_name:
        # First try to match pattern with slash (2020/21)
        matches = re.findall(r'\d{4}/\d{2}', auction_name)
        if matches:
            year_range_pattern = matches[0]
        else:
            # Try to match pattern with space (2020 21)
            matches = re.findall(r'\d{4}\s+\d{1,2}', auction_name)
            if matches:
                year_range_pattern = matches[0]
            else:
                # Try to extract just a 4-digit year
                matches = re.findall(r'\d{4}', auction_name)
                if matches:
                    year_range_pattern = matches[0]
    
    debug_info["component_matching"]["year_pattern"]["extracted"] = year_range_pattern
    
    # Step 5: Process all CMU IDs (limited to 10 for performance)
    cmu_ids_to_process = all_cmu_ids[:min(10, len(all_cmu_ids))]
    debug_info["processing_limited"] = len(all_cmu_ids) > len(cmu_ids_to_process)
    debug_info["cmu_ids_processed"] = cmu_ids_to_process
    
    all_components = []
    matching_components = []
    
    for cmu_id in cmu_ids_to_process:
        cmu_debug = {"cmu_id": cmu_id, "components_found": 0, "components_matched": 0}
        
        # Get components for this CMU ID
        components = get_component_data_from_json(cmu_id)
        if not components:
            components, _ = fetch_components_for_cmu_id(cmu_id)
        
        if components:
            cmu_debug["components_found"] = len(components)
            
            # Process each component
            for comp in components:
                comp_auction = comp.get("Auction Name", "")
                comp_auction_upper = comp_auction.upper() if comp_auction else ""
                
                # Prepare component info for debug
                comp_debug = {
                    "cmu_id": cmu_id,
                    "auction_name": comp_auction,
                    "location": comp.get("Location and Post Code", ""),
                    "description": comp.get("Description of CMU Components", ""),
                    "matching": {
                        "t_number_match": False,
                        "year_match": False,
                        "overall_match": False
                    }
                }
                
                # Check T-number match
                t_number_match = not t_number or (
                    (t_number == "T-1" and ("T-1" in comp_auction_upper or "T1" in comp_auction_upper or "T 1" in comp_auction_upper)) or
                    (t_number == "T-4" and ("T-4" in comp_auction_upper or "T4" in comp_auction_upper or "T 4" in comp_auction_upper))
                )
                
                comp_debug["matching"]["t_number_match"] = t_number_match
                
                if t_number_match:
                    debug_info["component_matching"]["t_number"]["matches"] += 1
                else:
                    debug_info["component_matching"]["t_number"]["non_matches"] += 1
                
                # Check year match with enhanced logic
                year_match = False
                
                if not year_range_pattern:
                    year_match = True
                else:
                    # First try exact substring match
                    if year_range_pattern in comp_auction:
                        year_match = True
                    else:
                        # Extract years from component auction name for comparison
                        comp_years_slash = re.findall(r'\d{4}[-/]\d{1,2}', comp_auction)
                        comp_years_space = re.findall(r'\d{4}\s+\d{1,2}', comp_auction)
                        comp_years_single = re.findall(r'\d{4}', comp_auction)
                        
                        # Extract the first 4-digit year from our pattern for fallback comparison
                        pattern_year = re.findall(r'\d{4}', year_range_pattern)[0] if re.findall(r'\d{4}', year_range_pattern) else ""
                        
                        # Try to extract the second part of the year range (after space/dash)
                        pattern_second_year = None
                        if " " in year_range_pattern:
                            parts = year_range_pattern.split(" ")
                            if len(parts) > 1 and parts[1].isdigit():
                                pattern_second_year = parts[1]
                        
                        # Normalized pattern matching - convert the patterns for comparisons
                        normalized_pattern = None
                        if pattern_year and pattern_second_year:
                            normalized_pattern = f"{pattern_year}-{pattern_second_year}"
                        
                        # Check for normalized matches first (this handles 2020 21 matching 2020-21)
                        if normalized_pattern:
                            for comp_year in comp_years_slash:
                                # Compare with dashes/slashes replaced to standardize
                                norm_comp_year = comp_year.replace("/", "-")
                                if norm_comp_year == normalized_pattern:
                                    year_match = True
                                    break
                                    
                        # If no normalized match, check if any component year pattern contains our target year
                        if not year_match:
                            for comp_pattern in comp_years_slash + comp_years_space + comp_years_single:
                                if pattern_year and pattern_year in comp_pattern:
                                    # Looser matching - just check if the first year is in the component pattern
                                    year_match = True
                                    break
                
                comp_debug["matching"]["year_match"] = year_match
                
                if year_match:
                    debug_info["component_matching"]["year_pattern"]["matches"] += 1
                else:
                    debug_info["component_matching"]["year_pattern"]["non_matches"] += 1
                
                # Overall match
                overall_match = year_match and t_number_match
                comp_debug["matching"]["overall_match"] = overall_match
                
                # Add to debug info
                all_components.append(comp_debug)
                
                # If matched, add to matching components
                if overall_match:
                    comp_copy = comp.copy()
                    comp_copy["CMU ID"] = cmu_id
                    matching_components.append(comp_copy)
                    cmu_debug["components_matched"] += 1
        
        debug_info["components"].append(cmu_debug)
    
    # Add matched components to debug info
    debug_info["matched_components"] = [
        {
            "cmu_id": comp.get("CMU ID", ""),
            "location": comp.get("Location and Post Code", ""),
            "description": comp.get("Description of CMU Components", ""),
            "auction": comp.get("Auction Name", "")
        }
        for comp in matching_components
    ]
    debug_info["matched_component_count"] = len(matching_components)
    
    # Return the debug info as JSON
    return JsonResponse(debug_info)


def fetch_components_for_cmu_id(cmu_id, limit=None, page=1, per_page=100):
    """Wrapper around the data_access version to avoid import errors"""
    from .services.data_access import fetch_components_for_cmu_id as fetch_components
    
    # Pass the pagination parameters to the data_access function
    # If the data_access function doesn't support these parameters yet, 
    # you'll need to update it separately
    return fetch_components(cmu_id, limit=limit, page=page, per_page=per_page)


def statistics_view(request):
    """View function for displaying database statistics"""
    from .utils import normalize
    from django.db.models import Count, Q, Sum # Ensure Sum is imported
    import logging # Add logging
    logger = logging.getLogger(__name__)
    
    # Adjust limits
    COMPANY_LIMIT = 25 # Changed from 50
    TECH_LIMIT = 25    # Stays at 25
    DERATED_LIMIT = 20 # Limit for the new list
    
    # --- Determine Company Sort Method --- 
    company_sort = request.GET.get('company_sort', 'count') # Default to count
    company_order = request.GET.get('company_order', 'desc') # Default to descending
    if company_sort not in ['count', 'capacity']:
        company_sort = 'count' # Fallback to default
    if company_order not in ['asc', 'desc']:
        company_order = 'desc' # Fallback to default
        
    # Determine DB sort prefix
    db_sort_prefix = '-' if company_order == 'desc' else ''
        
    # --- Fetch Top Companies based on Sort Method --- 
    top_companies_data = []
    if company_sort == 'count':
        # Sort by Component Count
        sort_field = f"{db_sort_prefix}count"
        top_companies_data = Component.objects.exclude(company_name__isnull=True) \
                                 .exclude(company_name='') \
                                 .values('company_name') \
                                 .annotate(count=Count('id')) \
                                 .order_by(sort_field)[:COMPANY_LIMIT]
        # Add company_id and calculate percentage for display
        total_components_for_pct = Component.objects.count() # Need total for percentage
        for company in top_companies_data:
            company['company_id'] = normalize(company['company_name'])
            if total_components_for_pct > 0:
                 company['percentage'] = (company['count'] / total_components_for_pct) * 100
            else:
                 company['percentage'] = 0
        logger.info(f"Fetched top {len(top_companies_data)} companies sorted by count ({company_order}).")
    else: 
        # Sort by Total De-rated Capacity
        sort_field = f"{db_sort_prefix}total_capacity"
        top_companies_data = Component.objects.exclude(company_name__isnull=True) \
                                 .exclude(company_name='') \
                                 .exclude(derated_capacity_mw__isnull=True) \
                                 .values('company_name') \
                                 .annotate(total_capacity=Sum('derated_capacity_mw')) \
                                 .order_by(sort_field)[:COMPANY_LIMIT]
        # Add company_id for links
        for company in top_companies_data:
            company['company_id'] = normalize(company['company_name'])
        logger.info(f"Fetched top {len(top_companies_data)} companies sorted by total capacity ({company_order}).")
        
    top_companies_data = list(top_companies_data) # Convert queryset to list

    # --- Technology Distribution Logic --- 
    # Determine Technology Sort Method
    tech_sort = request.GET.get('tech_sort', 'count') # Default to count
    tech_order = request.GET.get('tech_order', 'desc') # Default to descending
    if tech_sort not in ['count', 'capacity']:
        tech_sort = 'count'
    if tech_order not in ['asc', 'desc']:
        tech_order = 'desc'
    tech_db_sort_prefix = '-' if tech_order == 'desc' else ''

    # Count total distinct technologies (only needed for show_all check now)
    total_distinct_tech_count = Component.objects.values('technology').distinct().count()
    TECH_DISPLAY_LIMIT = 25
    show_all_techs = total_distinct_tech_count <= TECH_DISPLAY_LIMIT

    # Base query excluding nulls/empties
    base_tech_query = Component.objects.exclude(technology__isnull=True).exclude(technology='')

    # Annotate and order based on sort selection
    if tech_sort == 'count':
        tech_queryset = base_tech_query.values('technology') \
                                     .annotate(count=Count('id')) \
                                     .order_by(f'{tech_db_sort_prefix}count')
        logger.info(f"Fetching technologies sorted by count ({tech_order}).")
    else: # tech_sort == 'capacity'
        # Exclude components with null capacity for this sort
        tech_queryset = base_tech_query.exclude(derated_capacity_mw__isnull=True) \
                                     .values('technology') \
                                     .annotate(total_capacity=Sum('derated_capacity_mw')) \
                                     .order_by(f'{tech_db_sort_prefix}total_capacity')
        logger.info(f"Fetching technologies sorted by total capacity ({tech_order}).")

    # Apply limit if not showing all
    if not show_all_techs:
        tech_distribution = tech_queryset[:TECH_DISPLAY_LIMIT]
        logger.info(f"Displaying top {TECH_DISPLAY_LIMIT} technologies.")
    else:
        tech_distribution = tech_queryset
        logger.info(f"Displaying all {tech_distribution.count()} technologies.")

    # Get delivery year distribution - include all years
    year_distribution = Component.objects.exclude(delivery_year__isnull=True) \
                                 .exclude(delivery_year='') \
                                 .values('delivery_year') \
                                 .annotate(count=Count('id')) \
                                 .order_by('delivery_year')  # Order by year ascending
    
    # --- New: Get Top Components by De-Rated Capacity (Using DB Field) --- 
    top_derated_components = []
    try:
        # Fetch top N components directly from the database, ordered by the new field
        # Exclude null values as they cannot be meaningfully ranked
        top_derated_components = Component.objects.exclude(derated_capacity_mw__isnull=True) \
                                            .order_by('-derated_capacity_mw') \
                                            .values('id', 'location', 'company_name', 'derated_capacity_mw')[:DERATED_LIMIT]
        
        # Convert QuerySet to list of dicts (if needed for template consistency)
        top_derated_components = list(top_derated_components)
        
        # Rename key for template compatibility
        for comp in top_derated_components:
             comp['derated_capacity'] = comp.pop('derated_capacity_mw')
            
        logger.info(f"Fetched top {len(top_derated_components)} components by de-rated capacity using database field.")
        
    except Exception as e:
        logger.error(f"Error fetching de-rated capacity ranking using DB field: {e}")
    # --- End New Section --- 

    # Get total counts
    total_components = Component.objects.count()
    total_cmus = Component.objects.values('cmu_id').distinct().count()
    total_companies = Component.objects.exclude(company_name__isnull=True) \
                              .exclude(company_name='') \
                              .values('company_name').distinct().count()
    # Get count of unique locations
    total_unique_locations = Component.objects.exclude(location__isnull=True) \
                                    .exclude(location='') \
                                    .values('location').distinct().count()
                                    
    # Get components currently in capacity market (2025-2028)
    current_market_base_query = Component.objects.filter(
        delivery_year__gte='2025',
        delivery_year__lte='2028'
    )
    current_market_count = current_market_base_query.count() # Get total count first
    current_market_components = current_market_base_query.order_by('location')[:20]  # Then slice for display

    # Get components that were in the market before 2025 but are no longer in later years
    # Find CMUs not in current market
    past_market_cmus = (
        Component.objects
        .filter(delivery_year__lt='2025')
        .exclude(cmu_id__in=Component.objects.filter(delivery_year__gte='2025').values_list('cmu_id', flat=True))
        .values('cmu_id')
        .distinct()
    )
    
    # For each CMU, get the most recent instance before 2025
    past_market_components = []
    for cmu_entry in past_market_cmus[:20]:  # Limit to first 20 CMUs
        cmu_id = cmu_entry['cmu_id']
        latest_comp = Component.objects.filter(
            cmu_id=cmu_id, 
            delivery_year__lt='2025'
        ).order_by('-delivery_year').first()
        
        if latest_comp:
            past_market_components.append(latest_comp)
    
    # Sort the components by location
    past_market_components.sort(key=lambda x: x.location or '')
    
    # Calculate percentages for visual representation and add normalized company IDs
    for company in top_companies_data:
        company['company_id'] = normalize(company['company_name'])
        
    # Re-enable percentage calculation for tech distribution (always based on count for now)
    total_components_for_pct = Component.objects.count()
    for tech in tech_distribution:
        # If sorting by capacity, we need to add the component count separately if needed for display
        # For simplicity, let's just calculate percentage based on total components count
        # We might need to re-query the count if sorting by capacity and count is needed.
        # For now, let's assume count is implicitly available or not strictly required for percentage display.
        # A potential improvement: add count annotation even when sorting by capacity.
        # Simplified approach: Calculate percentage based on total components if count field exists
        current_count = tech.get('count', 0) # Get count if available
        if total_components_for_pct > 0 and current_count > 0:
            tech['percentage'] = (current_count / total_components_for_pct) * 100
        else:
            tech['percentage'] = 0 # Default if count is missing or total is zero
        
    for year in year_distribution:
        if total_components_for_pct > 0:
            year['percentage'] = (year['count'] / total_components_for_pct) * 100
        else:
             year['percentage'] = 0
    
    # --- Prepare Data for Charts --- #
    # Top Companies (limit to e.g., 10 for readability in chart, plus 'Other')
    CHART_LIMIT = 10
    
    # Company Chart Data (by Count)
    company_count_chart_data = Component.objects.exclude(company_name__isnull=True).exclude(company_name='') \
                                     .values('company_name') \
                                     .annotate(count=Count('id')) \
                                     .order_by('-count')
    company_count_chart_labels = [c['company_name'] for c in company_count_chart_data[:CHART_LIMIT]]
    company_count_chart_values = [c['count'] for c in company_count_chart_data[:CHART_LIMIT]]
    # Add 'Other' category if needed
    if company_count_chart_data.count() > CHART_LIMIT:
        other_count = sum(c['count'] for c in company_count_chart_data[CHART_LIMIT:])
        company_count_chart_labels.append('Other')
        company_count_chart_values.append(other_count)

    # Company Chart Data (by Capacity)
    company_capacity_chart_data = Component.objects.exclude(company_name__isnull=True).exclude(company_name='') \
                                        .exclude(derated_capacity_mw__isnull=True) \
                                        .values('company_name') \
                                        .annotate(total_capacity=Sum('derated_capacity_mw')) \
                                        .order_by('-total_capacity')
    company_capacity_chart_labels = [c['company_name'] for c in company_capacity_chart_data[:CHART_LIMIT]]
    company_capacity_chart_values = [float(c['total_capacity']) for c in company_capacity_chart_data[:CHART_LIMIT]] # Ensure float
    # Add 'Other' category if needed
    if company_capacity_chart_data.count() > CHART_LIMIT:
        other_capacity = sum(float(c['total_capacity']) for c in company_capacity_chart_data[CHART_LIMIT:])
        company_capacity_chart_labels.append('Other')
        company_capacity_chart_values.append(other_capacity)

    # Technology Chart Data (by Count)
    tech_chart_data = Component.objects.exclude(technology__isnull=True).exclude(technology='') \
                          .values('technology') \
                          .annotate(count=Count('id')) \
                          .order_by('-count')
    tech_chart_labels = [t['technology'] for t in tech_chart_data[:CHART_LIMIT]]
    tech_chart_values = [t['count'] for t in tech_chart_data[:CHART_LIMIT]]
    # Add 'Other' category if needed
    if tech_chart_data.count() > CHART_LIMIT:
        other_tech_count = sum(t['count'] for t in tech_chart_data[CHART_LIMIT:])
        tech_chart_labels.append('Other')
        tech_chart_values.append(other_tech_count)

    # Technology Chart Data (by Capacity)
    tech_capacity_chart_data = Component.objects.exclude(technology__isnull=True).exclude(technology='') \
                                   .exclude(derated_capacity_mw__isnull=True) \
                                   .values('technology') \
                                   .annotate(total_capacity=Sum('derated_capacity_mw')) \
                                   .order_by('-total_capacity')
    tech_capacity_chart_labels = [t['technology'] for t in tech_capacity_chart_data[:CHART_LIMIT]]
    tech_capacity_chart_values = [float(t['total_capacity']) for t in tech_capacity_chart_data[:CHART_LIMIT]] # Ensure float
    # Add 'Other' category if needed
    if tech_capacity_chart_data.count() > CHART_LIMIT:
        other_tech_capacity = sum(float(t['total_capacity']) for t in tech_capacity_chart_data[CHART_LIMIT:])
        tech_capacity_chart_labels.append('Other')
        tech_capacity_chart_values.append(other_tech_capacity)

    # --- End Chart Data Preparation ---

    context = {
        'top_companies_data': top_companies_data,
        'company_sort': company_sort, 
        'company_order': company_order,
        'tech_distribution': tech_distribution,
        'tech_sort': tech_sort,
        'tech_order': tech_order,
        'year_distribution': year_distribution,
        'top_derated_components': top_derated_components, # Add the new data
        'total_components': total_components,
        'total_cmus': total_cmus,
        'total_companies': total_companies,
        'total_unique_locations': total_unique_locations,
        'current_market_components': current_market_components,
        'past_market_components': past_market_components,
        'current_market_count': current_market_count,
        'past_market_count': Component.objects.filter(cmu_id__in=past_market_cmus.values('cmu_id')).count(), # Pass the actual count
        'show_all_techs': show_all_techs, # Pass this flag to the template
        
        # Chart Data - Pass raw Python lists/dicts
        'company_count_chart_labels': company_count_chart_labels, 
        'company_count_chart_values': company_count_chart_values,
        'company_capacity_chart_labels': company_capacity_chart_labels, 
        'company_capacity_chart_values': company_capacity_chart_values,
        'tech_chart_labels': tech_chart_labels, 
        'tech_chart_values': tech_chart_values,
        'tech_capacity_chart_labels': tech_capacity_chart_labels, 
        'tech_capacity_chart_values': tech_capacity_chart_values,
        'is_initial_load': False, # Add this line
    }
    
    return render(request, 'checker/statistics.html', context)


# --- Market Components List Views ---
@require_http_methods(["GET"])
def current_market_list(request):
    """View for paginated list of components currently in the capacity market (2025-2028)"""
    import time
    start_time = time.time()

    # --- Sorting ---
    sort_by = request.GET.get('sort_by', 'location') # Default sort
    order = request.GET.get('order', 'asc') # Default order
    db_sort_prefix = '-' if order == 'desc' else ''
    if sort_by == 'year':
        db_sort_field = f'{db_sort_prefix}delivery_year'
    else: # Default to location
        sort_by = 'location' # Ensure sort_by reflects the actual field
        db_sort_field = f'{db_sort_prefix}location'
    # --- End Sorting ---

    # Base query
    base_query = Component.objects.filter(
        delivery_year__gte='2025',
        delivery_year__lte='2028'
    )

    # Get total count before sorting/pagination
    total_count = base_query.count()
    print(f"Found {total_count} components in current market")

    # Apply sorting
    sorted_query = base_query.order_by(db_sort_field)

    # Get page number from request
    page_number = request.GET.get('page', 1) # Keep as string for Paginator
    per_page = 20

    # Create paginator instance with the SORTED queryset
    paginator = Paginator(sorted_query, per_page)
    
    try:
        page_obj = paginator.page(page_number)
    except PageNotAnInteger:
        # If page is not an integer, deliver first page.
        page_obj = paginator.page(1)
        page_number = 1
    except EmptyPage:
        # If page is out of range (e.g. 9999), deliver last page of results.
        page_obj = paginator.page(paginator.num_pages)
        page_number = paginator.num_pages

    print(f"Processed current market page {page_number}/{paginator.num_pages} sorted by {db_sort_field} in {time.time() - start_time:.2f} seconds")
    
    context = {
        'page_obj': page_obj,
        'total_count': total_count,
        'badge_class': 'bg-success',
        # Pass sorting info to template
        'sort_by': sort_by,
        'order': order,
    }

    return render(request, 'checker/component_list.html', context)

@require_http_methods(["GET"])
def past_market_list(request):
    """View for paginated list of components that were in the market before 2025 but are no longer"""
    import time
    start_time = time.time()

    # --- Sorting ---
    sort_by = request.GET.get('sort_by', 'location') # Default sort
    order = request.GET.get('order', 'asc') # Default order
    reverse_sort = (order == 'desc')
    if sort_by not in ['location', 'year']:
        sort_by = 'location' # Fallback
    # --- End Sorting ---

    # Check if we have the components cached
    from django.core.cache import cache
    
    cache_key = f"past_market_components_{sort_by}_{order}"
    all_latest_components = cache.get(cache_key)
    
    if all_latest_components is None:
        print("Components not in cache, computing from scratch...")
        
        # Get a list of all CMU IDs that existed before 2025
        past_cmus_query = Component.objects.filter(
            delivery_year__lt='2025'
        ).values_list('cmu_id', flat=True).distinct()
    
        # Get a list of all CMU IDs that exist in 2025+
        current_cmus_query = Component.objects.filter(
            delivery_year__gte='2025'
        ).values_list('cmu_id', flat=True).distinct()
    
        print(f"Computing past CMUs (this may take a moment)...")
    
        # Convert to Python sets for faster difference calculation
        past_cmus_set = set(past_cmus_query)
        current_cmus_set = set(current_cmus_query)
    
        # Find CMUs that only exist before 2025
        past_only_cmus = list(past_cmus_set - current_cmus_set)
        total_cmu_count = len(past_only_cmus)
        print(f"Found {total_cmu_count} CMUs that are only in past years")
    
        # For efficiency, limit to fetching 100 components at a time
        # This prevents the query from hanging
        batch_size = 100
        past_only_cmus = past_only_cmus[:batch_size]
        
        # For each past-only CMU, get the most recent component instance
        all_latest_components = []
        print(f"Fetching latest component for each of the {len(past_only_cmus)} past-only CMUs (limited to {batch_size})...")
        
        for cmu_id in past_only_cmus:
            latest = Component.objects.filter(
                cmu_id=cmu_id,
                delivery_year__lt='2025'
            ).order_by('-delivery_year').first()
    
            if latest:
                all_latest_components.append(latest)
        
        # Cache the results for one hour
        cache.set(cache_key, all_latest_components, 60*60)
        print(f"Cached {len(all_latest_components)} components for future requests")
    else:
        print(f"Using {len(all_latest_components)} components from cache")
    
    total_count = len(all_latest_components)
    print(f"Processing {total_count} latest components for past-only CMUs.")

    # --- Sort the ENTIRE list in Python ---
    if sort_by == 'location':
        all_latest_components.sort(key=lambda x: x.location or '', reverse=reverse_sort)
        print(f"Sorted {total_count} components by location ({order})")
    elif sort_by == 'year':
        all_latest_components.sort(key=lambda x: x.delivery_year or '', reverse=reverse_sort)
        print(f"Sorted {total_count} components by year ({order})")
    # --- End Sorting ---

    # --- Apply Pagination to the Sorted List ---
    page_number = request.GET.get('page', 1)
    try:
        page_number = int(page_number)
    except ValueError:
        page_number = 1
    per_page = 20

    # Calculate slice for current page from the sorted list
    start_idx = (page_number - 1) * per_page
    end_idx = start_idx + per_page
    components_to_display = all_latest_components[start_idx:end_idx]
    # --- End Apply Pagination ---

    # Create a manually constructed page object (similar to Django's Page)
    class ManualPage:
        def __init__(self, object_list, number, paginator):
            self.object_list = object_list
            self.number = number
            self.paginator = paginator
        
        # Make the page object itself iterable
        def __iter__(self):
            return iter(self.object_list)
        
        def __len__(self):
            return len(self.object_list)
            
        def has_previous(self):
            return self.number > 1
        
        def has_next(self):
            return self.number < self.paginator.num_pages
        
        # ADDED: Check if there are other pages
        def has_other_pages(self):
            return self.paginator.num_pages > 1
        
        def previous_page_number(self):
            return self.number - 1
        
        def next_page_number(self):
            return self.number + 1
        
        def start_index(self):
            return (self.number - 1) * self.paginator.per_page + 1
        
        def end_index(self):
            return min(self.start_index() + len(self.object_list) - 1, 
                       self.paginator.count)
    
    class ManualPaginator:
        def __init__(self, count, per_page):
            self.count = count
            self.per_page = per_page
            self.num_pages = (count + per_page - 1) // per_page
            self.page_range = range(1, self.num_pages + 1)
    
    paginator = ManualPaginator(total_count, per_page)
    page_obj = ManualPage(components_to_display, page_number, paginator)

    print(f"Processed past market components page {page_number}/{paginator.num_pages} in {time.time() - start_time:.2f} seconds")

    context = {
        'page_obj': page_obj,
        'total_count': total_count,
        # Pass sorting info to template
        'sort_by': sort_by,
        'order': order,
    }

    return render(request, 'checker/past_market_list.html', context)

@require_http_methods(["GET"])
def component_detail_by_id(request, component_id):
    """View function for component details page when looking up by component_id"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Try to find component by component_id
        component = Component.objects.filter(component_id=component_id).first()
        
        if component:
            # If found, redirect to the primary key URL
            return redirect('component_detail', pk=component.id)
        else:
            # If not found, show error
            return render(request, "checker/error.html", {
                "error": f"Component not found with ID: {component_id}",
                "suggestion": "This component ID may be invalid or the component may have been removed."
            })
            
    except Exception as e:
        logger.error(f"Error looking up component by ID {component_id}: {str(e)}")
        return render(request, "checker/error.html", {
            "error": f"Error looking up component: {str(e)}",
            "suggestion": "Please try again or contact support if the problem persists."
        })

# --- Helper Function for Search Logic --- 
# Removing unused helper function
# def _get_search_context(query, per_page, sort_by='relevance', sort_order='desc', debug_mode=False, request_get=None):
#     ...


# --- Original Search View (Modified) ---
def search(request):
    query = request.GET.get('q', '').strip()
    start_time = time.time()

    # Debug mode
    debug_mode = request.GET.get('debug', '').lower() == 'true'

    # Determine per_page
    default_per_page = 10 # <-- Changed default from 50 to 10 for testing
    if query.lower() == 'vital' and 'per_page' not in request.GET:
        per_page = 100 # Keep vital special case high if needed
    else:
        try:
            per_page = int(request.GET.get('per_page', str(default_per_page))) # Use variable for default
            if per_page <= 0: per_page = default_per_page # Ensure positive integer
        except ValueError:
            per_page = default_per_page # Default if invalid value provided

    # --- Determine sort parameters --- START ---
    sort_by = request.GET.get('sort_by', 'relevance').lower()
    sort_order = request.GET.get('sort_order', 'desc').lower()
    if sort_order not in ['asc', 'desc']:
        sort_order = 'desc'

    # Map UI sort fields to potential DB fields
    valid_sort_fields = {
        'location': 'location',
        'date': 'delivery_year', # Map 'date' to the model field
        'relevance': 'relevance' # Keep relevance as a valid option
    }
    backend_sort_by = valid_sort_fields.get(sort_by, 'relevance') # Default to relevance if invalid
    if backend_sort_by != sort_by:
         logger.warning(f"Search sort_by '{sort_by}' mapped to '{backend_sort_by}' or defaulted.")
         sort_by = backend_sort_by # Update sort_by passed to context if defaulted
    # --- Determine sort parameters --- END ---

    if not query:
        return render(request, 'checker/search_results.html', {
            'query': query, 'components': [], 'total_count': 0, 'company_count': 0,
            'per_page': per_page, 'sort_by': sort_by, 'sort_order': sort_order # Pass sort info even for empty query
        })

    # Normalize query for caching
    normalized_query = query.lower().strip()

    # --- Update Cache Key --- START ---
    cache_key = f"search_results_{normalized_query}_per{per_page}_sort{sort_by}_{sort_order}"
    # --- Update Cache Key --- END ---

    # Skip cache in debug mode
    if not debug_mode:
        cached_result = cache.get(cache_key)
        if cached_result:
            logger.info(f"Using cached search results for '{query}' with sort '{sort_by} {sort_order}'")
            # Ensure cached context has the sort parameters for the template
            cached_result.setdefault('sort_by', sort_by)
            cached_result.setdefault('sort_order', sort_order)
            cached_result.setdefault('per_page', per_page)
            cached_result.setdefault('query', query)
            cached_result['from_cache'] = True
            cached_result['api_time'] = (time.time() - start_time) + cached_result.get('original_time', 0)
            return render(request, 'checker/search_results.html', cached_result)

    try:
        # --- Pass sort parameters to data_access --- START ---
        components_raw, total_count = data_access.get_components_from_database(
            search_term=query,
            limit=per_page,
            sort_by=backend_sort_by, # Pass the potentially mapped field name
            sort_order=sort_order
        )
        # --- Pass sort parameters to data_access --- END ---
        logger.info(f"DB returned {len(components_raw)} components for '{query}'. Total matches: {total_count}")

        # Normalize component keys
        normalized_components = []
        for comp in components_raw:
            normalized_comp = {}
            if 'CMU ID' in comp: normalized_comp['cmu_id'] = comp['CMU ID']
            if 'Company Name' in comp: normalized_comp['company_name'] = comp['Company Name']
            if 'Location and Post Code' in comp: normalized_comp['location'] = comp['Location and Post Code']
            if 'Description of CMU Components' in comp: normalized_comp['description'] = comp['Description of CMU Components']
            if 'Delivery Year' in comp: normalized_comp['delivery_year'] = comp['Delivery Year']
            if 'Auction Name' in comp: normalized_comp['auction_name'] = comp['Auction Name']
            if 'De-Rated Capacity' in comp: normalized_comp['derated_capacity_mw'] = comp.get('De-Rated Capacity')

            for key, value in comp.items():
                if key not in normalized_comp:
                    normalized_comp[key] = value
            
            normalized_comp.setdefault('cmu_id', '')
            normalized_comp.setdefault('company_name', '')
            normalized_comp.setdefault('location', 'Unknown Location')
            normalized_comp.setdefault('description', '')
            normalized_comp.setdefault('delivery_year', '')
            normalized_comp.setdefault('auction_name', '')
            normalized_comp.setdefault('derated_capacity_mw', None)
            normalized_comp.setdefault('_id', None)
            normalized_components.append(normalized_comp)
        components = normalized_components

        # Calculate relevance score
        strictly_matching_components = []
        suspicious_components = []
        for comp in components:
            relevance_score = 0
            has_any_match = False
            field_matches = []
            company_name = comp.get('company_name', '')
            if company_name and query.lower() in company_name.lower():
                relevance_score += 100; has_any_match = True; field_matches.append('company_name')
                if query.lower() == company_name.lower(): relevance_score += 50
            cmu_id = comp.get('cmu_id', '')
            if cmu_id and query.lower() in cmu_id.lower():
                relevance_score += 80; has_any_match = True; field_matches.append('cmu_id')
            location = comp.get('location', '')
            if location and query.lower() in location.lower():
                relevance_score += 60; has_any_match = True; field_matches.append('location')
            description = comp.get('description', '')
            if description and query.lower() in description.lower():
                relevance_score += 40; has_any_match = True; field_matches.append('description')
            comp['relevance_score'] = relevance_score
            if debug_mode: comp['debug_matched_fields'] = field_matches
            if has_any_match: strictly_matching_components.append(comp)
            elif debug_mode and len(suspicious_components) < 5:
                suspicious_components.append({k: comp.get(k, '')[:50] for k in ['_id', 'company_name', 'location', 'description']})

        # Apply strict filtering if needed
        # --- REMOVE STRICT PYTHON FILTERING --- 
        # Always use the components returned from the database query initially
        # The relevance score can be used for sorting later if needed
        filtered_components = components 
        # --- END REMOVE --- 

        # --- REMOVE Python Relevance Sort --- START ---
        # filtered_components = sorted(filtered_components, key=lambda x: x.get('relevance_score', 0), reverse=True)
        # --- REMOVE Python Relevance Sort --- END ---

        # Group components by company
        companies_dict = {}
        for comp in filtered_components:
            # ... (existing grouping logic remains unchanged) ...
            company_name = comp.get('company_name', '')
            if not company_name: continue
            if company_name not in companies_dict:
                companies_dict[company_name] = {'name': company_name, 'component_count': 0, 'cmu_ids': set(), 'relevance_score': comp.get('relevance_score', 0)}
            else:
                companies_dict[company_name]['relevance_score'] = max(companies_dict[company_name]['relevance_score'], comp.get('relevance_score', 0))
            companies_dict[company_name]['component_count'] += 1
            cmu_id = comp.get('cmu_id', '')
            if cmu_id: companies_dict[company_name]['cmu_ids'].add(cmu_id)
        
        company_list = []
        for name, data in companies_dict.items():
             # ... (existing list creation logic remains unchanged) ...
            cmu_ids = list(data['cmu_ids'])
            display_limit = 3
            cmu_ids_display = ', '.join(cmu_ids[:display_limit]) + (f" and {len(cmu_ids) - display_limit} more" if len(cmu_ids) > display_limit else "")
            company_list.append({'name': name, 'component_count': data['component_count'], 'cmu_ids': cmu_ids, 'cmu_ids_display': cmu_ids_display, 'relevance_score': data['relevance_score']})

        # Sort companies by relevance score and then by component count
        company_list = sorted(company_list, key=lambda x: (-x['relevance_score'], -x['component_count']))

        api_time = time.time() - start_time

        # Prepare context, ensuring sort parameters are included
        context = {
            'query': query,
            'components': filtered_components,
            'total_count': total_count,
            'api_time': api_time,
            'companies': company_list,
            'company_count': len(company_list),
            'from_cache': False, # Ensure from_cache is always present
            'per_page': per_page,
            'sort_by': sort_by, # Pass current sort info
            'sort_order': sort_order,
            'debug_mode': debug_mode,
            'suspicious_components': suspicious_components if debug_mode else []
        }

        # Cache the processed context
        if not debug_mode:
            cache_data_to_store = context.copy()
            cache_data_to_store['original_time'] = api_time
            cache.set(cache_key, cache_data_to_store, 3600) # Use updated cache_key

        return render(request, 'checker/search_results.html', context)

    except Exception as e:
        logger.exception(f"Error during search processing for query '{query}': {e}")
        return render(request, 'checker/search_results.html', {
            'query': query,
            'error': f"An error occurred during the search: {e}",
            'components': [], 'total_count': 0, 'companies': [], 'company_count': 0,
            'api_time': time.time() - start_time,
            'from_cache': False, # Add from_cache here too
            'per_page': per_page,
            'sort_by': sort_by, # Pass sort info even on error
            'sort_order': sort_order,
            'debug_mode': debug_mode,
        })

# Removing the test view function
# def search_results_test(request):
#    ...

# --- Original Component Detail View (Ensure it uses DB ID) ---
@require_http_methods(["GET"])
def component_detail(request, pk):
    """View function for component details page using Database Primary Key"""
    # Ensure we use the service function which should handle DB lookup by pk
    return services.component_detail.get_component_details(request, pk)

def index_info(request):
    """Display database indexes information"""
    from django.db import connection
    import json
    
    # Get DB version
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    db_info = cursor.fetchone()[0]
    
    # Get indexes on the Component table
    cursor.execute("""
    SELECT
        i.relname as index_name,
        a.attname as column_name,
        am.amname as index_type
    FROM
        pg_class t,
        pg_class i,
        pg_index ix,
        pg_attribute a,
        pg_am am
    WHERE
        t.oid = ix.indrelid
        and i.oid = ix.indexrelid
        and a.attrelid = t.oid
        and a.attnum = ANY(ix.indkey)
        and t.relkind = 'r'
        and t.relname = 'checker_component'
        and i.relam = am.oid
    ORDER BY
        t.relname,
        i.relname;
    """)
    
    indexes = cursor.fetchall()
    
    # Check for pg_trgm extension
    cursor.execute("SELECT * FROM pg_extension WHERE extname = 'pg_trgm';")
    has_pg_trgm = bool(cursor.fetchone())
    
    # Check database statistics
    cursor.execute("SELECT reltuples::bigint AS row_count FROM pg_class WHERE relname = 'checker_component';")
    row_count = cursor.fetchone()[0]
    
    # Format the results
    index_list = []
    for idx_name, column, idx_type in indexes:
        index_list.append({
            'name': idx_name,
            'column': column,
            'type': idx_type
        })
    
    # Count GIN indexes
    gin_indexes = [idx for idx in index_list if idx['type'] == 'gin']
    
    report = {
        'database_info': db_info,
        'total_indexes': len(indexes),
        'indexes': index_list,
        'gin_indexes_count': len(gin_indexes),
        'has_pg_trgm': has_pg_trgm,
        'estimated_row_count': row_count
    }
    
    return HttpResponse(json.dumps(report, indent=2), content_type='application/json')

@require_http_methods(["GET"])
def technology_search_results(request, technology_name_encoded):
    """Displays search results filtered specifically by technology name."""
    from .services.component_search import format_component_record # Reuse formatter
    from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
    import urllib.parse
    from django.http import JsonResponse
    
    # Check if this is an AJAX request for the "Load More" feature
    is_ajax = request.GET.get('format') == 'json' or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
    
    logger.info(f"Technology search requested for encoded: {technology_name_encoded} (AJAX: {is_ajax})")
    
    # Decode the technology name
    try:
        technology_name = urllib.parse.unquote(technology_name_encoded)
        # --- ADDED: Strip trailing slash if present --- 
        if technology_name.endswith('/'):
            technology_name = technology_name[:-1]
        # --- END ADDED --- 
        logger.info(f"Decoded and stripped technology name: {technology_name}")
    except Exception as e:
        logger.error(f"Error decoding technology name '{technology_name_encoded}': {e}")
        # Handle error - perhaps render an error page or redirect
        return render(request, "checker/search.html", {
            "error": "Invalid technology name format.",
            "component_results": {},
            "component_count": 0,
        })
        
    # Get sorting and pagination parameters
    sort_field = request.GET.get("sort_by", "date") # Default to date sorting
    sort_order = request.GET.get("sort_order", "desc") # Default newest first (changed 'order' to 'sort_order' to match search.html)
    try:
        page = int(request.GET.get("page", "1"))
    except ValueError:
        page = 1
    try:
        per_page = int(request.GET.get("per_page", "50"))
    except ValueError:
        per_page = 50
    
    logger.info(f"Technology search parameters: sort_field={sort_field}, sort_order={sort_order}, page={page}, per_page={per_page}")
    
    # Determine sort field for database query
    db_sort_prefix = '-' if sort_order == 'desc' else ''
    if sort_field == "date":
        db_sort_field = f'{db_sort_prefix}delivery_year'
    elif sort_field == "derated_capacity":
        db_sort_field = f'{db_sort_prefix}derated_capacity_mw' # Use the new DB field
    elif sort_field == "mw":
        # Still need to sort Connection Capacity in Python as it's not a DB field
        db_sort_field = '-delivery_year' # Default sort for initial query
    else:
        db_sort_field = '-delivery_year' # Default to date if sort_field is invalid
    
    start_time = time.time()
    components_list = []
    total_component_count = 0
    error_message = None
    
    try:
        # Query components filtered by technology (case-insensitive)
        component_queryset = Component.objects.filter(technology__iexact=technology_name)
        
        # Apply database sorting only for date field                                
        if sort_field == "date":
            component_queryset = component_queryset.order_by(db_sort_field)
                                          
        total_component_count = component_queryset.count()
        logger.info(f"Found {total_component_count} components for technology '{technology_name}'")
        
        # Apply sorting
        if sort_field in ["date", "derated_capacity"]:
             # Apply DB sorting for indexed fields
            component_queryset = component_queryset.order_by(db_sort_field)
            logger.info(f"Applying DB sort: {db_sort_field}")
            paginator = Paginator(component_queryset, per_page)
        else:
            # Fetch all components if sorting by non-indexed field (MW/Connection Capacity)
            # This might still be slow for large technologies, consider adding Connection Capacity to DB too if needed.
            logger.info(f"Fetching all {total_component_count} components for Python sort: {sort_field}")
            all_components = list(component_queryset)
            
            if sort_field == "mw":
                # Sort by MW (Connection Capacity) in Python
                def get_connection_capacity(comp):
                    if comp.additional_data and "Connection Capacity" in comp.additional_data:
                        try:
                            return float(comp.additional_data["Connection Capacity"])
                        except (ValueError, TypeError):
                            return 0 # Treat errors as 0
                    return 0 # Treat missing as 0
                
                all_components.sort(
                    key=get_connection_capacity,
                    reverse=(sort_order == "desc")
                )
                logger.info(f"Applied Python sort for MW ({sort_order})")
            # Apply pagination after Python sorting
            paginator = Paginator(all_components, per_page)
            
        # Apply pagination
        try:
            components_page = paginator.page(page)
        except PageNotAnInteger:
            components_page = paginator.page(1)
        except EmptyPage:
            components_page = paginator.page(paginator.num_pages)
            
        # Get the object list for the current page
        components_list = list(components_page.object_list)
        
    except Exception as e:
        error_message = f"Error fetching components for technology: {e}"
        logger.exception(error_message)

    # Format results for display (similar to search_components_service)
    formatted_components = []
    if components_list:
        # Need to convert model objects to dicts expected by formatter
        for comp in components_list:
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
                "_id": comp.id,  # Use database ID (pk)
                "db_id": comp.id, # Add db_id specifically for the template link
                "component_id_str": comp.component_id or ''
            }
            if comp.additional_data:
                 comp_dict["De-Rated Capacity"] = comp.additional_data.get("De-Rated Capacity", "N/A")
                 comp_dict["Connection Capacity"] = comp.additional_data.get("Connection Capacity", "N/A")
                 for key, value in comp.additional_data.items():
                    if key not in comp_dict:
                        comp_dict[key] = value
            formatted_components.append(format_component_record(comp_dict, {}))

    # Group the components by location similar to the main search
    from collections import defaultdict
    
    components_by_location = defaultdict(list)
    for comp in components_list:
        # Use location and description as the grouping key
        location_key = f"{comp.location}|{comp.description}"
        components_by_location[location_key].append(comp)
    
    # Create grouped component objects similar to search.html template expectations
    grouped_components = []
    for location_key, comps in components_by_location.items():
        # Sort components within group by delivery year (newest first)
        comps.sort(key=lambda x: x.delivery_year if x.delivery_year else '', reverse=True)
        
        # Get first component for display
        first_component = comps[0]
        
        # Extract all CMU IDs from the group
        cmu_ids = list(set(c.cmu_id for c in comps if c.cmu_id))
        
        # Extract all auction years/names from the group
        auction_names = list(set(f"{c.delivery_year} {c.auction_name}" for c in comps 
                               if c.delivery_year and c.auction_name))
        # Sort auction names by delivery year (newest first)
        auction_names.sort(reverse=True)
        
        # Create mapping of auction names to component IDs for linking
        auction_to_components = {}
        for c in comps:
            if c.delivery_year and c.auction_name:
                auction_key = f"{c.delivery_year} {c.auction_name}"
                if auction_key not in auction_to_components:
                    auction_to_components[auction_key] = []
                auction_to_components[auction_key].append(c.id)
        
        # Determine if any component is "Active" (auction year 2024-25 or later)
        active_status = any(c.delivery_year and c.delivery_year >= '2024' for c in comps)
        
        # Create the group object
        group = {
            'location': first_component.location,
            'description': first_component.description,
            'first_component': first_component,
            'cmu_ids': cmu_ids,
            'auction_names': auction_names,
            'auction_to_components': auction_to_components,
            'active_status': active_status
        }
        
        grouped_components.append(group)
    
    # If sorting by location, ensure proper sorting of the groups
    if sort_field == 'location':
        reverse_sort = (sort_order == 'desc')
        try:
            # Case-insensitive sort by location, handling None values safely
            grouped_components.sort(
                key=lambda x: (x.get('location', '') or '').lower(), 
                reverse=reverse_sort
            )
            logger.info(f"Successfully sorted {len(grouped_components)} groups by location ({sort_order})")
        except Exception as e:
            logger.error(f"Error sorting grouped components: {e}")
    
    # Create a manual page object for the grouped components
    from django.core.paginator import Page
    class ManualPage(Page):
        def __init__(self, object_list, number, paginator):
            self.object_list = object_list
            self.number = number
            self.paginator = paginator
        
        def __iter__(self):
            return iter(self.object_list)
        
        def __len__(self):
            return len(self.object_list)
        
        def has_previous(self):
            return self.number > 1
        
        def has_next(self):
            return self.number < self.paginator.num_pages
        
        def has_other_pages(self):
            return self.paginator.num_pages > 1
        
        def previous_page_number(self):
            return self.number - 1
        
        def next_page_number(self):
            return self.number + 1
        
        def start_index(self):
            return (self.number - 1) * self.paginator.per_page + 1
        
        def end_index(self):
            return min(self.number * self.paginator.per_page, self.paginator.count)
    
    class ManualPaginator:
        def __init__(self, count, per_page):
            self.count = count
            self.per_page = per_page
            self.num_pages = max(1, (count + per_page - 1) // per_page)
        
        def page(self, number):
            # Calculate the slice of grouped_components to return for this page
            start_idx = (int(number) - 1) * self.per_page
            end_idx = start_idx + self.per_page
            page_items = grouped_components[start_idx:end_idx]
            return ManualPage(page_items, int(number), self)
        
        def get_elided_page_range(self, number=1, on_each_side=3, on_ends=2):
            """Return a list of page numbers with elision."""
            page_range = list(range(1, self.num_pages + 1))
            if self.num_pages <= (on_each_side + on_ends) * 2:
                return page_range
            
            # Add Ellipsis constant
            ELLIPSIS = 0  # Special sentinel value
            
            # Calculate ranges
            head = list(range(1, on_ends + 1))
            middle = list(range(max(on_ends + 1, number - on_each_side), 
                               min(number + on_each_side + 1, self.num_pages - on_ends + 1)))
            tail = list(range(self.num_pages - on_ends + 1, self.num_pages + 1))
            
            # Build output with ellipsis
            output = []
            output.extend(head)
            if head[-1] + 1 < middle[0]:
                output.append(ELLIPSIS)
            output.extend(middle)
            if middle[-1] + 1 < tail[0]:
                output.append(ELLIPSIS)
            output.extend(tail)
            
            return output
    
    # Create a manual paginator for the grouped components
    grouped_paginator = ManualPaginator(len(grouped_components), per_page)
    grouped_page = grouped_paginator.page(page)

    component_results_dict = {technology_name: formatted_components} if formatted_components else {}
    api_time = time.time() - start_time
    
    # Figure out sort description for the template
    sort_description = ""
    if sort_field == "date":
        sort_description = "delivery year"
    elif sort_field == "derated_capacity":
        sort_description = "de-rated capacity"
    elif sort_field == "mw":
        sort_description = "connection capacity (MW)"
    
    context = {
        "query": technology_name, # Use tech name as the effective query
        "note": f"Showing components with technology: {technology_name}",
        "company_links": [], # No company results on this page
        "component_results": component_results_dict, 
        "component_count": total_component_count,
        "displayed_component_count": len(formatted_components),
        "error": error_message,
        "api_time": api_time,
        "comp_sort": sort_order,
        "sort_field": sort_field,
        "sort_order": sort_order,
        "sort_description": sort_description,
        "is_technology_search": True, # Flag for the template if needed
        "unified_search": True, # Keep this flag to match template condition
        # Pagination context
        "page_obj": grouped_page, 
        "paginator": grouped_paginator,
        "page": grouped_page.number, 
        "per_page": per_page,
        "total_pages": grouped_paginator.num_pages,
        "has_prev": grouped_page.has_previous(),
        "has_next": grouped_page.has_next(),
        "page_range": grouped_paginator.get_elided_page_range(number=grouped_page.number, on_each_side=1, on_ends=1),
        # Debug info
        "debug_info": {
            "grouped_component_count": len(grouped_components)
        }
    }

    # If this is an AJAX request, return JSON response for "Load More" feature
    if is_ajax:
        try:
            # Format items as HTML for direct insertion
            html_items = []
            # Get the template from file
            from django.template.loader import get_template, render_to_string
            template = get_template('checker/components/_group_result_item.html')
            
            # Render each group item using the template
            for group in grouped_page.object_list:
                try:
                    # Create context for the template rendering
                    from django.template.defaultfilters import slugify
                    item_context = {
                        'group': group,
                        'query': technology_name,
                        'slugify': slugify,
                        'is_technology_search': True,
                        'sort_by': sort_field,
                        'sort_order': sort_order,
                    }
                    # Render the template with the context
                    item_html = template.render(item_context)
                    # Add the rendered HTML to the items list
                    html_items.append(item_html)
                except Exception as render_err:
                    logger.error(f"Error rendering group to HTML in technology view: {render_err}")
                    # Fallback to simple HTML if template rendering fails
                    location = group.get('location', 'Unknown Location')
                    description = group.get('description', 'No description available')
                    html_items.append(f"<div class='result-item mb-3 border-bottom pb-3'><h5>{location}</h5><p>{description}</p></div>")
            
            # Create JSON response with HTML items
            json_response = {
                'items': html_items,
                'has_more': grouped_page.has_next(),
                'next_page': grouped_page.next_page_number() if grouped_page.has_next() else None,
                'current_page': grouped_page.number,
                'total_pages': grouped_paginator.num_pages,
                'component_count': total_component_count,
                'displayed_count': len(grouped_components),
                'success': True
            }
            
            logger.info(f"Returning AJAX response with {len(html_items)} items for technology '{technology_name}'")
            return JsonResponse(json_response)
            
        except Exception as ajax_err:
            logger.error(f"Error preparing AJAX response: {ajax_err}")
            return JsonResponse({
                'error': str(ajax_err),
                'success': False,
                'items': []
            }, status=500)
            
    # For normal requests, render the template
    return render(request, "checker/search.html", context) # Reuse search template

@require_http_methods(["GET"])
def derated_capacity_list(request):
    """Displays a full, paginated list of components ranked by De-rated Capacity."""
    from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
    
    logger.info("Full De-rated Capacity list requested")
    page = request.GET.get("page", 1)
    sort_order_param = request.GET.get("sort", "desc") # Default to descending (largest first)
    if sort_order_param not in ["asc", "desc"]:
        sort_order_param = "desc" # Fallback to default if invalid value
    
    per_page = 50
    start_time = time.time()
    
    # Determine database sort order based on parameter
    db_sort_prefix = '-' if sort_order_param == 'desc' else ''
    db_sort_field = f'{db_sort_prefix}derated_capacity_mw'
    
    all_processed_components = []
    error_message = None
    total_count = 0
    components_page = None
    
    try:
        # Query the database directly using the new field, excluding nulls
        component_queryset = Component.objects.exclude(derated_capacity_mw__isnull=True) \
                                          .order_by(db_sort_field) \
                                          .only('id', 'location', 'company_name', 'derated_capacity_mw')
                                          
        total_count = component_queryset.count()
        logger.info(f"Found {total_count} components with de-rated capacity, sorting by {db_sort_field}")
        
        # Apply pagination directly to the queryset
        paginator = Paginator(component_queryset, per_page)
        try:
            components_page = paginator.page(page)
        except PageNotAnInteger:
            components_page = paginator.page(1)
        except EmptyPage:
            components_page = paginator.page(paginator.num_pages)
            
        # Prepare data for the template (rename field)
        all_processed_components = []
        for comp in components_page.object_list:
            all_processed_components.append({
                'id': comp.id,
                'location': comp.location or "N/A",
                'company_name': comp.company_name or "N/A",
                'derated_capacity': comp.derated_capacity_mw # Use the numeric value
            })
            
    except Exception as e:
        logger.error(f"Error processing de-rated capacity list using DB field: {e}")
        error_message = f"Error processing component list: {e}"
        # Ensure variables are in a safe state for the template
        all_processed_components = []
        total_count = 0
        components_page = None 
        paginator = Paginator([], per_page) # Empty paginator
        components_page = paginator.page(1)
    
    api_time = time.time() - start_time
    
    context = {
        # Use the processed list for the current page object
        "page_obj": components_page,
        "object_list": all_processed_components, # Pass the formatted list
        "paginator": paginator,
        "total_count": total_count,
        "api_time": api_time,
        "error": error_message,
        "sort_order": sort_order_param, # Pass sort order to template
        # Add other necessary context variables if the template requires them
        "page": components_page.number if components_page else 1, 
        "per_page": per_page,
        "total_pages": paginator.num_pages,
        "has_prev": components_page.has_previous() if components_page else False,
        "has_next": components_page.has_next() if components_page else False,
        "page_range": paginator.get_elided_page_range(number=components_page.number if components_page else 1, on_each_side=1, on_ends=1)
    }

    return render(request, "checker/derated_capacity_list.html", context)

@require_http_methods(["GET"])
def company_capacity_list(request):
    """Displays a full, paginated list of companies ranked by total De-rated Capacity."""
    from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
    from django.db.models import Sum
    from .utils import normalize
    
    logger.info("Full Company list by Total Capacity requested")
    page = request.GET.get("page", 1)
    sort_order_param = request.GET.get("sort", "desc") # Default to descending (largest first)
    if sort_order_param not in ["asc", "desc"]:
        sort_order_param = "desc" # Fallback to default
    
    per_page = 50 # Companies per page
    start_time = time.time()
    
    # Determine database sort order based on parameter
    db_sort_prefix = '-' if sort_order_param == 'desc' else ''
    db_sort_field = f'{db_sort_prefix}total_capacity'
    
    company_list = []
    error_message = None
    total_count = 0
    companies_page = None
    paginator = None
    
    try:
        # Query companies ranked by total capacity (similar to statistics view)
        company_queryset = Component.objects.exclude(company_name__isnull=True) \
                                    .exclude(company_name='') \
                                    .exclude(derated_capacity_mw__isnull=True) \
                                    .values('company_name') \
                                    .annotate(total_capacity=Sum('derated_capacity_mw')) \
                                    .order_by(db_sort_field)
                                    
        total_count = company_queryset.count()
        logger.info(f"Found {total_count} companies with de-rated capacity, sorting by {db_sort_field}")
        
        # Apply pagination directly to the queryset
        paginator = Paginator(company_queryset, per_page)
        try:
            companies_page = paginator.page(page)
        except PageNotAnInteger:
            companies_page = paginator.page(1)
        except EmptyPage:
            companies_page = paginator.page(paginator.num_pages)
            
        # Prepare data for the template (add company_id)
        company_list = []
        for comp_data in companies_page.object_list:
             # Normalize company name to create an ID for the link
            comp_data['company_id'] = normalize(comp_data['company_name'])
            company_list.append(comp_data)
            
    except Exception as e:
        logger.error(f"Error processing company capacity list: {e}")
        error_message = f"Error processing company list: {e}"
        # Ensure variables are in a safe state for the template
        company_list = []
        total_count = 0
        companies_page = None 
        paginator = Paginator([], per_page) # Empty paginator
        companies_page = paginator.page(1)
    
    api_time = time.time() - start_time
    
    context = {
        "page_obj": companies_page,
        "object_list": company_list, # Pass the processed list for the current page
        "paginator": paginator,
        "total_count": total_count,
        "api_time": api_time,
        "error": error_message,
        "sort_order": sort_order_param, # Pass sort order to template
        # Pagination context variables
        "page": companies_page.number if companies_page else 1, 
        "per_page": per_page,
        "total_pages": paginator.num_pages if paginator else 0,
        "has_prev": companies_page.has_previous() if companies_page else False,
        "has_next": companies_page.has_next() if companies_page else False,
        "page_range": paginator.get_elided_page_range(number=companies_page.number if companies_page else 1, on_each_side=1, on_ends=1) if paginator else []
    }

    return render(request, "checker/company_capacity_list.html", context)

def technology_list_view(request):
    """Displays a full, paginated list of technologies ranked by component count."""
    from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
    from django.db.models import Count
    from .models import Component
    import logging
    import time

    logger = logging.getLogger(__name__)
    start_time = time.time()

    page = request.GET.get('page', 1)
    per_page = 50  # Or adjust as needed
    error_message = None

    try:
        # Query all non-empty technologies, count components, order by count
        tech_queryset = Component.objects.exclude(technology__isnull=True) \
                                 .exclude(technology='') \
                                 .values('technology') \
                                 .annotate(count=Count('id')) \
                                 .order_by('-count')
        
        total_count = tech_queryset.count()
        logger.info(f"Found {total_count} distinct non-empty technologies.")

        # Apply pagination
        paginator = Paginator(tech_queryset, per_page)
        try:
            tech_page = paginator.page(page)
        except PageNotAnInteger:
            tech_page = paginator.page(1)
        except EmptyPage:
            tech_page = paginator.page(paginator.num_pages)
            
        # Calculate percentages for display relative to total components
        total_components = Component.objects.count()
        for tech_data in tech_page.object_list:
            if total_components > 0:
                tech_data['percentage'] = (tech_data['count'] / total_components) * 100
            else:
                tech_data['percentage'] = 0

    except Exception as e:
        logger.error(f"Error processing technology list: {e}")
        error_message = f"Error processing technology list: {e}"
        # Ensure variables are in a safe state for the template
        tech_page = None 
        total_count = 0
        paginator = Paginator([], per_page) # Empty paginator
        tech_page = paginator.page(1)

    api_time = time.time() - start_time

    context = {
        'page_obj': tech_page,
        'total_count': total_count,
        'paginator': paginator,
        'api_time': api_time,
        'error': error_message,
    }

    return render(request, 'checker/technology_list.html', context)

# -------- Map View and API --------- START --------

def map_view(request):
    """Render the map page with Google Maps integration"""
    # Get technology options for filters
    technologies = Component.objects.exclude(technology__isnull=True) \
                               .exclude(technology='') \
                               .values_list('technology', flat=True) \
                               .distinct() \
                               .order_by('technology')
    
    # Get company options for filters
    companies = Component.objects.exclude(company_name__isnull=True) \
                                .exclude(company_name='') \
                                .values_list('company_name', flat=True) \
                                .distinct() \
                                .order_by('company_name')[:100]  # Limit to avoid too many options
    
    # Get delivery year options
    delivery_years = Component.objects.exclude(delivery_year__isnull=True)\
                                .exclude(delivery_year='')\
                                .values_list('delivery_year', flat=True)\
                                .distinct()\
                                .order_by('-delivery_year') # Newest first
                                
    # Get top 20 companies by component count
    from django.db.models import Count
    top_companies = Component.objects.exclude(company_name__isnull=True)\
                               .exclude(company_name='')\
                               .exclude(company_name__in=["AXLE ENERGY LIMITED", "OCTOPUS ENERGY LIMITED"])\
                               .values('company_name')\
                               .annotate(count=Count('id'))\
                               .order_by('-count')[:20]
    
    # Get stats
    geocoded_count = Component.objects.filter(geocoded=True).count()
    total_count = Component.objects.count()
    
    context = {
        'api_key': settings.GOOGLE_MAPS_API_KEY,
        'technologies': technologies,
        'companies': companies, # Keep existing list for now
        'delivery_years': delivery_years, # Add years list
        'top_companies': top_companies, # Add top companies list
        'geocoded_count': geocoded_count,
        'total_count': total_count,
    }
    
    return render(request, 'checker/map.html', context)

def map_data_api(request):
    """
    Main map data API for fetching components by viewport and filters.
    """
    # --- START: Performance Tracking ---
    import time, json, hashlib
    from django.core.cache import cache
    
    start_time = time.time()
    stage_times = {}
    
    def record_stage_time(stage):
        current = time.time()
        elapsed = current - start_time
        stage_times[stage] = elapsed
        return elapsed
    # --- END: Performance Tracking ---
    
    # --- START: Cache Check ---
    params = request.GET.copy()
    # Add detail_level to relevant params for cache key
    relevant_params = ['technology', 'north', 'south', 'east', 'west', 'company', 'year', 'cmu_id', 'detail_level', 'exact_technology', 'cm_period']
    filtered_params = {k: params.get(k, '') for k in relevant_params if k in params}

    # Create a deterministic string representation of the parameters
    params_string = json.dumps(sorted(filtered_params.items()))
    params_hash = hashlib.md5(params_string.encode('utf-8')).hexdigest()
    cache_key = f"map_data_{params_hash}"

    # DEBUGGING: Print the exact cache key being generated
    print(f"DEBUG: Generated cache key: '{cache_key}'")
    
    # Check if we have a cached response in Django cache
    cached_response = cache.get(cache_key)
    print(f"DEBUG: cache.get({cache_key}) returned: {type(cached_response)} (None means cache miss)")
    
    if cached_response and isinstance(cached_response, str):
        cache_time = record_stage_time("cache_check")
        print(f"✅ Cache HIT: Returning cached map data from Django cache for key: {cache_key}")
        
        # Return cached JSON string as HttpResponse with correct content type
        from django.http import HttpResponse
        return HttpResponse(cached_response, content_type="application/json")
    
    # Also try file-based cache
    import os
    cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_cache')
    cache_file = os.path.join(cache_dir, f"{cache_key}.json")
    
    if os.path.exists(cache_file):
        try:
            # Get file modified time
            file_mod_time = os.path.getmtime(cache_file)
            current_time = time.time()
            
            # Check if file is newer than cache timeout (1 hour = 3600 seconds)
            if current_time - file_mod_time < 3600:
                with open(cache_file, 'r') as f:
                    file_cached_response = f.read()
                
                cache_time = record_stage_time("cache_check")
                print(f"✅ Cache HIT: Returning cached map data from FILE for key: {cache_key}")
                
                # Return cached JSON string from file
                from django.http import HttpResponse
                return HttpResponse(file_cached_response, content_type="application/json")
            else:
                print(f"Cache file exists but is expired: {cache_file}")
        except Exception as e:
            print(f"Error reading cache file: {e}")
    
    record_stage_time("cache_check")
    print(f"⚠️ Cache MISS: Processing map data for key: {cache_key}")
    # --- END: Cache Check ---

    # --- START: Early exit if no technology selected ---
    technology_param_exists = 'technology' in request.GET
    company_filter = request.GET.get('company', '') 
    
    # Only require technology if no company is specified
    if not technology_param_exists and not company_filter:
        empty_time = record_stage_time("early_exit")
        print(f"No technology parameter and no company filter provided. Returning empty. (in {empty_time:.3f}s)")
        empty_response = {
            'type': 'FeatureCollection',
            'features': [],
            'metadata': { 'count': 0, 'total': 0, 'filtered': False }
        }
        from django.http import HttpResponse
        return HttpResponse(json.dumps(empty_response), content_type="application/json")
    # --- END: Early exit ---

    # --- Get detail level parameter --- START
    detail_level = request.GET.get('detail_level', 'minimal')
    # --- Get detail level parameter --- END

    base_query = Component.objects.filter(geocoded=True, latitude__isnull=False, longitude__isnull=False)

    specific_year_requested = request.GET.get('year')
    requested_technology = request.GET.get('technology')
    exact_technology = request.GET.get('exact_technology')  # New parameter for exact technology filtering
    cm_period = request.GET.get('cm_period', 'future')  # Default to future if not specified

    is_filtered = False
    viewport_filtered = False
    matching_full_tech_names = []

    # --- START: Modified Company Filter ---
    excluded_companies = ["AXLE ENERGY LIMITED", "OCTOPUS ENERGY LIMITED"]
    
    if company_filter: 
        # If a specific company is selected (from either dropdown), filter for it directly
        base_query = base_query.filter(company_name=company_filter)
        is_filtered = True
        print(f"Applied specific company filter: {company_filter}")
    else:
        # If 'All Companies' is selected (no company_filter parameter), exclude Axle/Octopus
        base_query = base_query.exclude(company_name__in=excluded_companies)
        print(f"Applied default company filter: Excluding {excluded_companies}")
        # We don't necessarily set is_filtered=True here, 
        # as this is the default view unless other filters are active.
        
    # --- END: Modified Company Filter ---
    
    # Apply Year Filters (Priority: Exact > CM Period)
    if specific_year_requested:
        # Basic check if it looks like a year, might need refinement
        if specific_year_requested.isdigit() and len(specific_year_requested) == 4:
            base_query = base_query.filter(delivery_year__iexact=specific_year_requested)
            print(f"Applied specific year filter: {specific_year_requested}")
            is_filtered = True
        else:
             print(f"Ignoring invalid specific year format: {specific_year_requested}")
    else:
        # Apply filter based on selected CM period
        if cm_period == 'historical':
            # Historical: 2016-2023
            base_query = base_query.filter(
                Q(delivery_year__gte='2016') & Q(delivery_year__lte='2023')
            )
            print("Applied historical CM period filter: 2016-2023")
        else:
            # Future: 2024 and beyond
            base_query = base_query.filter(Q(delivery_year__gte='2024'))
            print("Applied future CM period filter: >= 2024")
        
        # EXCEPTION: For technologies with mostly older units, don't apply year filter
        if exact_technology in ["OCGT and Reciprocating Engines", 
                                "OCGT and Reciprocating Engines (Fuel Type - Diesel)",
                                "Oil-fired steam generators"]:
            print(f"Special case technology '{exact_technology}': Bypassing year filter")
            # Remove the year filter we just added
            base_query = base_query.all()  # Get a fresh query without year filter
            
            if company_filter:
                base_query = base_query.filter(company_name=company_filter)
                
    record_stage_time("basic_filters")

    # Apply Viewport Filter
    north = request.GET.get('north')
    south = request.GET.get('south')
    east = request.GET.get('east')
    west = request.GET.get('west')
    
    if north and south and east and west:
        try:
            north_f, south_f, east_f, west_f = map(float, [north, south, east, west])
            if -90 <= south_f < north_f <= 90 and -180 <= west_f <= 180 and -180 <= east_f <= 180:
                 print(f"Applying viewport filter: N:{north_f}, S:{south_f}, E:{east_f}, W:{west_f}")
                 if west_f <= east_f:
                     base_query = base_query.filter(
                         latitude__lte=north_f, latitude__gte=south_f,
                         longitude__lte=east_f, longitude__gte=west_f
                     )
                 else: # Viewport crosses the dateline
                     base_query = base_query.filter(
                         latitude__lte=north_f, latitude__gte=south_f
                     ).filter(
                         Q(longitude__gte=west_f) | Q(longitude__lte=east_f)
                     )
                 viewport_filtered = True
                 is_filtered = True
            else:
                print("Invalid bounds received, skipping viewport filter.")
        except ValueError:
            print("Error parsing bounds, skipping viewport filter.")

    record_stage_time("viewport_filter")

    # Apply Technology Filter
    is_filtered = True
    if requested_technology == '':
        print("Requested technology is empty string ('All'), skipping tech filter.")
    elif requested_technology == 'All' and company_filter:
        # Special case: If "All" is specified with a company filter, skip technology filtering
        print("Using 'All' technologies for the selected company, skipping tech filter.")
    else:
        # Check if we have an exact technology filter (bypass normal simplification)
        if exact_technology:
            print(f"Using exact_technology filter: '{exact_technology}'")
            base_query = base_query.filter(technology=exact_technology)
        else:
            # Otherwise try simplified technology name (e.g. "Wind" instead of "Wind (Offshore)")
            # Initialize matching_full_tech_names
            matching_full_tech_names = []
            
            target_simplified_category = get_simplified_technology(requested_technology)
            print(f"Requested tech '{requested_technology}' maps to category: {target_simplified_category}. Finding matching DB names...")

            tech_map_cache_key = f"tech_map_{target_simplified_category}"
            cached_tech_names = cache.get(tech_map_cache_key)

            if cached_tech_names is not None:
                 matching_full_tech_names = cached_tech_names
                 print(f"  Using cached DB tech names for '{target_simplified_category}': {len(matching_full_tech_names)} names")
            else:
                # No need to clear since we just initialized it
                all_db_techs = Component.objects.values_list('technology', flat=True).distinct()
                for db_tech in all_db_techs:
                    if db_tech and get_simplified_technology(db_tech) == target_simplified_category:
                        matching_full_tech_names.append(db_tech)
                # Move cache set outside the loop, after all names are collected
                cache.set(tech_map_cache_key, matching_full_tech_names, 3600) 
                print(f"  Queried and cached DB tech names for '{target_simplified_category}': {len(matching_full_tech_names)} names")

            print(f"  Found {len(matching_full_tech_names)} DB tech names matching category '{target_simplified_category}': {matching_full_tech_names}")

            if matching_full_tech_names:
                print(f"Applying filter: technology IN {matching_full_tech_names}")
                base_query = base_query.filter(technology__in=matching_full_tech_names)
            else:
                 print(f"Warning: No DB tech names found anywhere for category '{target_simplified_category}'")
                 base_query = base_query.none()

    record_stage_time("technology_filter")

    # --- START: Pre-calculate total count ---
    total_matching_components = base_query.count()
    count_time = record_stage_time("count_calculation")
    print(f"Total matching components after filters (before limit): {total_matching_components} (count query took {count_time - stage_times['technology_filter']:.3f}s)")
    # --- END: Pre-calculate total count ---

    # --- Limit and Build GeoJSON ---
    # Increase limit, especially for viewport queries
    requested_limit_str = request.GET.get('limit', '5000') # Default higher
    try:
        requested_limit = int(requested_limit_str)
        # Use a very high limit if specific viewport bounds are provided
        limit = 10000 if viewport_filtered else requested_limit # Significantly increase if viewport filtering
    except ValueError:
        limit = 10000 if viewport_filtered else 5000 # Fallback to high limits

    print(f"Using limit: {limit} (Viewport filtered: {viewport_filtered})")

    # --- Adjust fields fetched based on detail level --- START
    if detail_level == 'minimal':
        fields_to_fetch = ('id', 'latitude', 'longitude', 'technology', 'location')
        print("Fetching minimal fields for map markers.")
    else: # 'full' or any other value defaults to full for now
        fields_to_fetch = ('id', 'latitude', 'longitude', 'location',
                           'company_name', 'description', 'technology', 'delivery_year', 'cmu_id')
        print("Fetching full fields for map markers.")
    
    # Fetch components using values() for better performance
    components_to_render = base_query.values(*fields_to_fetch)[:limit]
    query_time = record_stage_time("db_query")
    print(f"Query executed in {query_time - count_time:.3f}s, fetched {len(components_to_render)} components")

    # --- START: Group components by coordinates ---
    components_by_coord = {}
    for comp in components_to_render:
        lat = comp.get('latitude')
        lng = comp.get('longitude')
        # Skip components without valid coordinates
        if lat is None or lng is None:
            continue 
        coord_key = (lat, lng)
        if coord_key not in components_by_coord:
            components_by_coord[coord_key] = []
        components_by_coord[coord_key].append(comp)
    print(f"Grouped {len(components_to_render)} components into {len(components_by_coord)} unique coordinates.")
    # --- END: Group components by coordinates ---

    # --- Build GeoJSON from grouped components (taking only FIRST per coord) ---
    features = []
    for coord, grouped_components in components_by_coord.items():
        # Take only the first component as representative for this coordinate
        if not grouped_components: continue # Should not happen, but safety check
        representative_comp = grouped_components[0]

        # Get simplified technology (always needed for icon color)
        simplified_tech = get_simplified_technology(representative_comp.get('technology', 'Unknown'))

        # Prepare properties dict using only the representative component
        feature_properties = {
            'id': representative_comp['id'], # ID of the representative marker
            'cmu_id': representative_comp.get('cmu_id', ''),
            'title': representative_comp.get('location', 'Unknown Location'),
            'technology': representative_comp.get('technology', 'Unknown'),
            'display_technology': simplified_tech,
            'company': representative_comp.get('company_name', 'Unknown'),
            'description': representative_comp.get('description', ''),
            'delivery_year': representative_comp.get('delivery_year', ''),
            'detailUrl': f'/component/{representative_comp["id"]}/' 
        }

        features.append({
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [coord[1], coord[0]] # Use coord key (lng, lat)
            },
            'properties': feature_properties
        })
    
    json_build_time = record_stage_time("json_build")
    print(f"Building GeoJSON from grouped data took {json_build_time - query_time:.3f}s") # Adjusted timing reference

    # --- Metadata (using len(features) which is now de-duplicated count) ---
    final_is_filtered = any([
        requested_technology != '',
        company_filter,
        specific_year_requested,
        viewport_filtered
    ])

    response_data = {
        'type': 'FeatureCollection',
        'features': features,
        'metadata': {
            'count': len(features),
            'total': total_matching_components,
            'filtered': final_is_filtered
        }
    }

    total_time = record_stage_time("total")
    print(f"Returning {len(features)} features. Metadata total: {total_matching_components}, filtered: {final_is_filtered}")
    print(f"--- map_data_api processing completed in {total_time:.3f}s ---")
    
    # --- Performance Summary ---
    print("--- Performance Summary ---")
    for i, (stage, time_value) in enumerate(stage_times.items()):
        if i > 0:
            prev_stage = list(stage_times.keys())[i-1]
            stage_duration = time_value - stage_times[prev_stage]
            print(f"  {stage}: {stage_duration:.3f}s ({time_value:.3f}s total)")
        else:
            print(f"  {stage}: {time_value:.3f}s")
    print("------------------------")

    # Create JSON string from response data and cache it
    from django.http import HttpResponse
    json_str = json.dumps(response_data)
    
    # Cache via Django cache (still try this)
    print(f"DEBUG: About to set cache for key '{cache_key}', string length: {len(json_str)}")
    try:
        cache.set(cache_key, json_str, 3600)  # Cache for 1 hour
        print(f"DEBUG: Cache set call completed for '{cache_key}'")
        
        # Verify it was stored correctly
        verify = cache.get(cache_key)
        print(f"DEBUG: Verification - cache now has type: {type(verify)} for key '{cache_key}'")
    except Exception as e:
        print(f"DEBUG ERROR: Failed to cache data: {e}")
    
    # Also use direct file-based caching as a backup strategy
    import os
    cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data_cache')
    os.makedirs(cache_dir, exist_ok=True)
    
    # Create a filename from the cache key
    cache_file = os.path.join(cache_dir, f"{cache_key}.json")
    
    try:
        with open(cache_file, 'w') as f:
            f.write(json_str)
        print(f"DEBUG: Saved cache data to file: {cache_file}")
    except Exception as e:
        print(f"DEBUG ERROR: Failed to write cache file: {e}")
    
    # Return JSON response
    return HttpResponse(json_str, content_type="application/json")

# -------- Map View and API --------- END ---------

# === Stripe Donation Views ===

def donation_page(request):
    """Renders the donation page with the Stripe public key."""
    context = {
        'stripe_public_key': settings.STRIPE_PUBLIC_KEY
    }
    return render(request, 'checker/donation_page.html', context)

@require_http_methods(["POST"])
def create_checkout_session(request):
    """Creates a Stripe Checkout session for a donation."""
    try:
        # Get amount from form
        amount_choice = request.POST.get('amount', '500')
        custom_amount = request.POST.get('custom_amount', '')
        
        # Determine the donation amount in pence
        if amount_choice == 'custom' and custom_amount:
            # Convert pounds to pence (multiply by 100)
            try:
                # Convert to float first, then to int to handle decimal inputs
                donation_amount_pence = int(float(custom_amount) * 100)
            except ValueError:
                # If conversion fails, default to £5
                donation_amount_pence = 500
        else:
            # Use the selected radio button amount (already in pence)
            try:
                donation_amount_pence = int(amount_choice)
            except ValueError:
                # If conversion fails, default to £5
                donation_amount_pence = 500
        
        # Ensure minimum amount (£1 = 100 pence)
        if donation_amount_pence < 100:
            donation_amount_pence = 100

        # Get base URL
        protocol = 'https' if request.is_secure() else 'http'
        host = request.get_host()
        base_url = f"{protocol}://{host}"

        # Format amount for display (£XX.XX)
        amount_display = f"£{donation_amount_pence/100:.2f}"

        checkout_session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            line_items=[
                {
                    'price_data': {
                        'currency': 'gbp',
                        'unit_amount': donation_amount_pence,
                        'product_data': {
                            'name': f'Donation to Capacity Market Search ({amount_display})',
                            'description': 'One-time donation to support site maintenance and development',
                        },
                    },
                    'quantity': 1,
                },
            ],
            mode='payment',
            success_url=base_url + reverse('donation_success') + '?session_id={CHECKOUT_SESSION_ID}',
            cancel_url=base_url + reverse('donation_cancel'),
        )
        # Redirect to Stripe Checkout
        return redirect(checkout_session.url, code=303)
    except Exception as e:
        logger.error(f"Error creating Stripe checkout session: {e}")
        # Redirect to an error page or back home
        return redirect('/')

def donation_success(request):
    """Page displayed after a successful donation."""
    # You could potentially retrieve session details here using
    # session_id = request.GET.get('session_id')
    # session = stripe.checkout.Session.retrieve(session_id)
    # To display customer name, amount etc., but keep it simple for now.
    context = {'message': 'Thank you for your generous donation!'}
    return render(request, 'checker/donation_success.html', context)

def donation_cancel(request):
    """Page displayed if the donation is cancelled."""
    context = {'message': 'Donation cancelled. You can try again anytime.'}
    return render(request, 'checker/donation_cancel.html', context)

# --- END Stripe Donation Views ---

@require_http_methods(["GET"])
def component_map_detail_api(request, component_id):
    """API endpoint that returns detailed info for a specific component"""
    from django.core.cache import cache
    from .utils import normalize
    
    # Create a cache key for this component
    cache_key = f"component_detail_{component_id}"
    
    # Try to get from cache
    cached_data = cache.get(cache_key)
    if cached_data:
        print(f"✅ Cache HIT: Returning cached component detail for ID: {component_id}")
        return JsonResponse(cached_data)
    
    print(f"⚠️ Cache MISS: Fetching component detail for ID: {component_id}")
    
    # Not in cache, fetch from database
    try:
        component = Component.objects.get(id=component_id)
        
        # Normalize company name for the ID
        company_name = component.company_name or 'Unknown'
        company_id = normalize(company_name) if company_name != 'Unknown' else None
        
        data = {
            'success': True,
            'data': {
                'id': component.id,
                'title': component.location or 'Unknown Location',
                'technology': component.technology or 'Unknown',
                'display_technology': get_simplified_technology(component.technology),
                'company': company_name,
                'company_id': company_id,
                'description': component.description or '',
                'delivery_year': component.delivery_year or '',
                'cmu_id': component.cmu_id or '',
                'detailUrl': f'/component/{component.id}/'
            }
        }
        
        # Cache the result for 30 minutes
        cache.set(cache_key, data, 60 * 30)
        print(f"✅ Cached component detail for ID: {component_id}")
        
        return JsonResponse(data)
    except Component.DoesNotExist:
        error_data = {'success': False, 'error': 'Component not found'}
        return JsonResponse(error_data, status=404)
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error fetching component map detail for ID {component_id}: {e}")
        error_data = {'success': False, 'error': 'Server error'}
        return JsonResponse(error_data, status=500)

@require_http_methods(["GET"])
def debug_cache_test(request):
    """Debug view to test if the cache is working properly."""
    from django.core.cache import cache
    from django.http import JsonResponse
    import time, json
    
    # Generate a unique test key
    test_key = f"cache_test_{int(time.time())}"
    test_data = {"timestamp": time.time(), "test": "data"}
    
    # Set data in cache
    print(f"DEBUG: Setting test key '{test_key}' in cache")
    cache.set(test_key, json.dumps(test_data), 60)  # 1 minute timeout
    
    # Immediately retrieve to verify
    cached_data = cache.get(test_key)
    print(f"DEBUG: Retrieved '{cached_data}' for test key '{test_key}'")
    
    # Create a second key with different data
    test_key2 = f"{test_key}_second"
    test_data2 = {"second": True, "time": time.time()}
    cache.set(test_key2, test_data2, 60)  # Store as Python object, not JSON string
    
    # Get all cache keys (if possible)
    cache_info = {
        "test_key": test_key,
        "test_data": test_data,
        "cached_data": cached_data,
        "retrieved_type": str(type(cached_data)),
        "cache_works": cached_data is not None,
        "second_key": test_key2,
        "second_data_direct": test_data2,
    }
    
    # Try to get the second key
    cached_data2 = cache.get(test_key2)
    cache_info["second_retrieved"] = cached_data2
    cache_info["second_type"] = str(type(cached_data2))
    
    return JsonResponse({
        "message": "Cache debug information",
        "cache_info": cache_info,
    })

@csrf_exempt
@require_http_methods(["POST"])
def gpt_search_api(request):
    """
    API endpoint for GPT integration that accepts natural language queries
    and returns structured search results.
    
    Accepts:
    - query: Natural language search query
    - api_key: API key for authentication
    
    Returns JSON response with search results.
    """
    # Check API key for authentication
    api_key = request.headers.get('X-API-Key', '')
    expected_api_key = getattr(settings, 'API_KEY', 'your_custom_api_key_for_auth')
    
    if api_key != expected_api_key:
        return JsonResponse({'error': 'Invalid API key'}, status=401)
    
    try:
        # Parse JSON data from request
        data = json.loads(request.body)
        
        # Extract query from request
        query = data.get('query', '').strip()
        if not query:
            return JsonResponse({'error': 'Query parameter is required'}, status=400)
        
        logger.info(f"GPT API received query: {query}")
        
        # Extract search parameters from the natural language query
        search_params = extract_search_params(query)
        extracted_query = search_params['q']
        
        # Try direct extraction if it's a simple technology search
        if len(extracted_query.split()) == 1 and extracted_query in ['ccgt', 'ocgt', 'chp', 'gas', 'battery', 'dsr']:
            # This is likely a technology-only search - expand it
            expanded_query = f"{extracted_query} technology"
            search_params['q'] = expanded_query
            logger.info(f"Expanded technology query: '{extracted_query}' -> '{expanded_query}'")
            
        # Try direct database search first
        components_raw, total_count = data_access.get_components_from_database(
            search_term=search_params['q'],
            page=1,
            per_page=int(search_params.get('per_page', 50)),
            sort_by='relevance',
            sort_order='desc'
        )
        
        # Log the direct database search attempt
        logger.info(f"GPT API: direct database search returned {len(components_raw)} components out of {total_count} total for query '{search_params['q']}'")
        
        # If direct search found results, return them
        if components_raw and len(components_raw) > 0:
            # Process results for GPT
            return JsonResponse(process_results_for_gpt(
                {
                    'components': components_raw,
                    'total_count': total_count,
                    'query': query,
                    'page': 1,
                    'per_page': search_params.get('per_page', 50),
                    'sort_by': 'relevance',
                    'sort_order': 'desc',
                    'api_time': 0,
                    'error': None
                },
                query
            ))
        
        # If direct search found no results, try with the search_components_service
        logger.info(f"GPT API: direct search found no results, trying search_components_service")
        
        # Create a simulated request object with GET parameters
        mock_request = type('MockRequest', (), {'GET': {}})()
        mock_request.GET = search_params
        
        # Call the search service
        search_results = search_components_service(mock_request, return_data_only=True)
        
        # Process the results for GPT consumption
        response_data = process_results_for_gpt(search_results, query)
        
        return JsonResponse(response_data)
    
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data in request'}, status=400)
    except Exception as e:
        logger.exception(f"Error in GPT search API: {str(e)}")
        return JsonResponse({'error': str(e)}, status=500)

def extract_search_params(query):
    """
    Extract search parameters from a natural language query.
    
    Returns a dictionary of GET parameters, extracting key search terms.
    """
    original_query = query.strip()
    
    # Extract key terms for better searching
    # Common patterns in natural language queries
    extracted_query = original_query.lower()
    
    # Special case for Elephant and Castle
    if "elephant and castle" in extracted_query.lower():
        # Try both "Elephant and Castle" and "Elephant Park" as alternatives
        logger.info(f"Detected 'Elephant and Castle' in query, adding 'Elephant Park' as an alternative")
        return {'q': 'Elephant Park', 'per_page': '50'}
    
    # Check for location-based queries first
    location_patterns = [
        "in nottingham", "in london", "in manchester", "in birmingham", 
        "in glasgow", "in edinburgh", "in cardiff", "in belfast",
        "in leeds", "in liverpool", "in newcastle", "in sheffield",
        "in bristol", "in york", "in cambridge", "in oxford",
        "in the uk", "in the united kingdom", "in england", 
        "in scotland", "in wales", "in northern ireland"
    ]
    
    # Check if this is a location-based query
    is_location_query = False
    location_term = None
    
    for location in location_patterns:
        if location in extracted_query.lower():
            is_location_query = True
            location_term = location.replace("in ", "")
            break
    
    # If we have a generic "in" location query not in our patterns
    if not is_location_query and " in " in extracted_query.lower():
        # Extract the location after "in"
        parts = extracted_query.lower().split(" in ")
        if len(parts) > 1 and parts[1].strip():
            is_location_query = True
            location_term = parts[1].strip()
            
    # Remove common filler words to focus on the key terms
    # But preserve location-based structure if detected
    filler_words = [
        "show me", "find", "search for", "get", "give me", 
        "are there", "list", "what", "where", "how many",
        "about", "with"
    ]
    
    # Don't remove "in" if this is a location query
    if not is_location_query:
        filler_words.extend(["in the", "in", "at", "near", "around"])
    
    for word in filler_words:
        extracted_query = extracted_query.replace(word + " ", " ")
    
    # Extract technology keywords
    tech_keywords = [
        "ccgt", "ocgt", "chp", "gas", "battery", "nuclear", 
        "coal", "hydro", "solar", "wind", "biomass", "dsr"
    ]
    
    tech_found = None
    for tech in tech_keywords:
        if tech in extracted_query.lower():
            tech_found = tech
            break
    
    # Create final search query
    final_query = extracted_query.strip()
    if not final_query:
        final_query = original_query  # Fallback to original if extraction failed
    
    # For location queries, ensure proper structure
    if is_location_query and tech_found:
        # Format as "technology location" to match database patterns
        final_query = f"{tech_found} {location_term}"
        logger.info(f"Formatted location-based query: '{final_query}'")
    
    # Log both queries for debugging
    logger.info(f"Original query: '{original_query}', Extracted search terms: '{final_query}'")
    
    # Return search parameters
    params = {'q': final_query, 'per_page': '50'}
    
    # Add technology filter if found
    if tech_found:
        params['tech'] = tech_found
        
    return params

def process_results_for_gpt(search_results, original_query):
    """
    Process the search results into a format optimized for GPT consumption.
    
    Args:
        search_results: Dictionary from search_components_service
        original_query: The original natural language query
        
    Returns:
        Dictionary with processed results
    """
    # Initialize response structure
    response = {
        'query': original_query,
        'results_count': 0,
        'components': [],
        'summary': '',
        'error': None
    }
    
    # Debug logging
    logger.info(f"Processing search results for GPT: {type(search_results)}")
    
    # Check if search_results is a dictionary
    if not isinstance(search_results, dict):
        logger.warning(f"Search results is not a dictionary: {type(search_results)}")
        response['error'] = 'Invalid search results format'
        return response
    
    # Log dictionary keys for debugging
    logger.info(f"Search results keys: {search_results.keys()}")
    
    # Check if there's an error in the results
    if 'error' in search_results and search_results['error']:
        response['error'] = search_results['error']
        return response
    
    # Extract components from results
    components = search_results.get('components', [])
    logger.info(f"Found {len(components)} components in results['components']")
    
    # Special case for "Elephant and Castle" query with no results
    if "elephant and castle" in original_query.lower() and len(components) == 0:
        logger.info("Special case: 'Elephant and Castle' with no results, trying 'Elephant Park'")
        
        # Create a simulated request for Elephant Park
        mock_request = type('MockRequest', (), {'GET': {}})()
        mock_request.GET = {'q': 'Elephant Park', 'per_page': '50'}
        
        try:
            park_results = search_components_service(mock_request, return_data_only=True)
            if isinstance(park_results, dict) and 'components' in park_results:
                park_components = park_results.get('components', [])
                if park_components and len(park_components) > 0:
                    components = park_components
                    search_results['total_count'] = park_results.get('total_count', len(park_components))
                    response['note'] = "Found results at Elephant Park, which is in the Elephant and Castle area."
        except Exception as e:
            logger.error(f"Error in Elephant Park fallback search: {e}")
    
    # Check if this might be a location-based query
    original_query_lower = original_query.lower()
    location_terms = ["in nottingham", "in london", "in manchester", "in birmingham", 
                     "in leeds", "in sheffield", "in bristol", "in the uk"]
                     
    is_location_query = any(term in original_query_lower for term in location_terms)
    if not is_location_query and " in " in original_query_lower:
        is_location_query = True
        
    location_name = None
    if is_location_query and " in " in original_query_lower:
        parts = original_query_lower.split(" in ")
        if len(parts) > 1:
            location_name = parts[1].strip()
    
    # If components is empty, try some fallback strategies
    if len(components) == 0 and original_query:
        logger.info(f"No results found for '{original_query}', trying fallback strategies")
        
        # First check for technology terms in the query
        tech_terms = {
            'ccgt': 'Combined Cycle Gas Turbine',
            'ocgt': 'Open Cycle Gas Turbine',
            'chp': 'Combined Heat and Power', 
            'gas turbine': 'Gas Turbine',
            'combined cycle': 'Combined Cycle Gas Turbine',
            'open cycle': 'Open Cycle Gas Turbine',
            'battery': 'Battery Storage',
            'storage': 'Battery Storage',
            'dsr': 'Demand Side Response',
            'nuclear': 'Nuclear',
            'biomass': 'Biomass',
            'coal': 'Coal',
            'hydro': 'Hydro',
            'solar': 'Solar',
            'wind': 'Wind'
        }
        
        tech_found = None
        tech_full_name = None
        for term, full_name in tech_terms.items():
            if term in original_query_lower:
                tech_found = term
                tech_full_name = full_name
                break
                
        # If this is a technology + location query with no results, try just the technology
        if tech_found and is_location_query:
            logger.info(f"Tech + location query detected: '{tech_found}' in '{location_name}'. Trying tech-only fallback.")
            
            # Try searching just for the technology
            mock_request = type('MockRequest', (), {'GET': {}})()
            mock_request.GET = {'q': tech_full_name, 'per_page': '100'}
            
            try:
                tech_results = search_components_service(mock_request, return_data_only=True)
                
                # Check if we got results this time and they're in the right format
                if isinstance(tech_results, dict) and 'components' in tech_results:
                    tech_components = tech_results.get('components', [])
                    
                    if tech_components and len(tech_components) > 0:
                        logger.info(f"Fallback tech-only search found {len(tech_components)} components")
                        
                        # Now filter these results for the location, if possible
                        if location_name:
                            location_name = location_name.lower()
                            filtered_components = []
                            location_terms = location_name.split()
                            
                            # Try multiple location matching strategies
                            for comp in tech_components:
                                comp_location = comp.get('location', '').lower()
                                
                                # Strategy 1: Direct substring match
                                if location_name in comp_location:
                                    filtered_components.append(comp)
                                    continue
                                    
                                # Strategy 2: Check if main words from location appear
                                matches = 0
                                important_terms = [term for term in location_terms if len(term) > 3]
                                for term in important_terms:
                                    if term in comp_location:
                                        matches += 1
                                
                                # If more than half of important terms match, include it
                                if important_terms and matches >= len(important_terms) / 2:
                                    filtered_components.append(comp)
                            
                            if filtered_components:
                                logger.info(f"Filtered to {len(filtered_components)} components in location '{location_name}'")
                                components = filtered_components
                                search_results['total_count'] = len(filtered_components)
                            else:
                                # If location filtering yields nothing, just use all tech components
                                # but note it in the summary
                                components = tech_components[:20]  # Limit to top 20
                                search_results['total_count'] = tech_results.get('total_count', len(tech_components))
                                response['note'] = f"No exact matches for {tech_full_name} in {location_name}. Showing other {tech_full_name} components."
                        else:
                            # No location to filter by, use all tech components
                            components = tech_components[:20]  # Limit to top 20
                            search_results['total_count'] = tech_results.get('total_count', len(tech_components))
            except Exception as e:
                logger.error(f"Error in fallback technology search: {e}")
        
        # If still no components, try an even more general search
        if not components and tech_found:
            logger.info(f"Still no results, trying general search for technology: {tech_found}")
            
            # Try an even broader search just for the technology type
            mock_request = type('MockRequest', (), {'GET': {}})()
            mock_request.GET = {'q': tech_found, 'per_page': '20'}
            
            try:
                tech_results = search_components_service(mock_request, return_data_only=True)
                if isinstance(tech_results, dict) and 'components' in tech_results:
                    tech_components = tech_results.get('components', [])
                    if tech_components and len(tech_components) > 0:
                        components = tech_components
                        search_results['total_count'] = tech_results.get('total_count', len(tech_components))
                        response['note'] = f"No exact matches for your query. Showing general results for {tech_full_name}."
            except Exception as e:
                logger.error(f"Error in general technology search: {e}")
    
    # Process components for the response - with enhanced de-duplication and grouping
    # Group components by location and CMU ID to identify the same physical installation
    grouped_components = {}
    for comp in components:
        # Make sure comp is a dictionary before trying to access keys
        if not isinstance(comp, dict):
            logger.warning(f"Found non-dict component in results: {type(comp)}")
            continue
        
        # Create a key based on location and CMU ID
        location = comp.get('location', '')
        cmu_id = comp.get('cmu_id', '')
        description = comp.get('description', '')
        company = comp.get('company_name', '')
        
        # Create a unique key for the physical installation
        # If two components have the same location, company, and CMU ID, they're likely the same physical unit
        key = f"{location}|{cmu_id}|{company}"
        
        if key not in grouped_components:
            grouped_components[key] = {
                'cmu_id': cmu_id,
                'company_name': company,
                'location': location,
                'description': description,
                'technology': comp.get('technology', ''),
                'years': [],
                'url': f"/component/{comp.get('id', '')}" if 'id' in comp else '',
                'capacities': {
                    'derated': set(),
                    'connection': set(),
                    'other_mw': set()
                },
                'latest_year': '',
                'latest_auction': '',
                'market_status': {
                    'current_market': False,  # 2024-2025
                    'next_market': False,     # 2025-2026
                    'future_markets': False,  # 2026+
                    'years_active': []
                }
            }
        
        # Add delivery year info
        delivery_year = comp.get('delivery_year', '')
        auction_name = comp.get('auction_name', '')
        
        if delivery_year:
            # Add to years list for this component
            grouped_components[key]['years'].append({
                'year': delivery_year,
                'auction': auction_name,
                'auction_url': f"/component/{comp.get('id', '')}" if 'id' in comp else ''
            })
            
            # Update market status
            year_int = 0
            try:
                year_int = int(delivery_year)
            except (ValueError, TypeError):
                pass
                
            if year_int > 0:
                grouped_components[key]['market_status']['years_active'].append(year_int)
                
                if year_int == 2024:
                    grouped_components[key]['market_status']['current_market'] = True
                elif year_int == 2025:
                    grouped_components[key]['market_status']['next_market'] = True
                elif year_int > 2025:
                    grouped_components[key]['market_status']['future_markets'] = True
            
            # Track the latest year for sorting
            if not grouped_components[key]['latest_year'] or delivery_year > grouped_components[key]['latest_year']:
                grouped_components[key]['latest_year'] = delivery_year
                grouped_components[key]['latest_auction'] = auction_name
        
        # Collect all capacity information
        derated_capacity = comp.get('derated_capacity_mw', comp.get('derated_capacity', None))
        if derated_capacity is not None and str(derated_capacity).strip():
            try:
                derated_value = float(derated_capacity)
                grouped_components[key]['capacities']['derated'].add(derated_value)
            except (ValueError, TypeError):
                grouped_components[key]['capacities']['derated'].add(str(derated_capacity))
        
        connection_capacity = comp.get('connection_capacity', None)
        if connection_capacity is not None and str(connection_capacity).strip():
            try:
                connection_value = float(connection_capacity)
                grouped_components[key]['capacities']['connection'].add(connection_value)
            except (ValueError, TypeError):
                grouped_components[key]['capacities']['connection'].add(str(connection_capacity))
        
        # Check additional_data for other MW values
        additional_data = comp.get('additional_data', {})
        if isinstance(additional_data, dict):
            for k, v in additional_data.items():
                if isinstance(k, str) and 'capacity' in k.lower() and 'mw' in k.lower():
                    if v is not None and str(v).strip():
                        try:
                            value = float(v)
                            grouped_components[key]['capacities']['other_mw'].add(f"{k}: {value}")
                        except (ValueError, TypeError):
                            grouped_components[key]['capacities']['other_mw'].add(f"{k}: {v}")
        
        # Look for any other fields that might contain MW values
        for field_name, field_value in comp.items():
            if isinstance(field_name, str) and isinstance(field_value, str) and 'mw' in field_name.lower():
                if field_value.strip() and field_name not in ['derated_capacity_mw', 'connection_capacity']:
                    grouped_components[key]['capacities']['other_mw'].add(f"{field_name}: {field_value}")
    
    # Convert the grouped components to a list and format them
    processed_components = []
    for key, comp_data in grouped_components.items():
        # Sort years from newest to oldest
        comp_data['years'].sort(key=lambda x: x.get('year', ''), reverse=True)
        
        # Format capacities into strings
        derated_capacity_str = ""
        if comp_data['capacities']['derated']:
            values = list(comp_data['capacities']['derated'])
            if len(values) == 1:
                derated_capacity_str = f"{values[0]} MW" if isinstance(values[0], (int, float)) else f"{values[0]}"
            else:
                derated_capacity_str = f"Varies: {', '.join(str(v) for v in values)} MW"
        
        connection_capacity_str = ""
        if comp_data['capacities']['connection']:
            values = list(comp_data['capacities']['connection'])
            if len(values) == 1:
                connection_capacity_str = f"{values[0]} MW" if isinstance(values[0], (int, float)) else f"{values[0]}"
            else:
                connection_capacity_str = f"Varies: {', '.join(str(v) for v in values)} MW"
        
        other_capacity_str = ""
        if comp_data['capacities']['other_mw']:
            other_capacity_str = ", ".join(str(v) for v in comp_data['capacities']['other_mw'])
        
        # Generate market status string
        market_status = []
        if comp_data['market_status']['current_market']:
            market_status.append("Currently in market (2024-2025)")
        if comp_data['market_status']['next_market']:
            market_status.append("In next market period (2025-2026)")
        if comp_data['market_status']['future_markets']:
            market_status.append("In future markets (2026+)")
        
        if not market_status and comp_data['market_status']['years_active']:
            years = sorted(comp_data['market_status']['years_active'], reverse=True)
            if years and years[0] < 2024:
                market_status.append(f"Previously in market (last active: {years[0]})")
        
        market_status_str = "; ".join(market_status) if market_status else "Market status unknown"
        
        # Format years for display
        years_str = []
        for year_data in comp_data['years'][:3]:  # Limit to 3 most recent years
            years_str.append(f"{year_data['year']} ({year_data['auction']})")
        
        years_display = ", ".join(years_str)
        if len(comp_data['years']) > 3:
            years_display += f" and {len(comp_data['years']) - 3} more years"
        
        # Create the processed component
        processed_comp = {
            'cmu_id': comp_data['cmu_id'],
            'company_name': comp_data['company_name'],
            'location': comp_data['location'],
            'description': comp_data['description'],
            'technology': comp_data['technology'],
            'years': years_display,
            'latest_year': comp_data['latest_year'],
            'derated_capacity': derated_capacity_str,
            'connection_capacity': connection_capacity_str,
            'other_capacity_info': other_capacity_str,
            'market_status': market_status_str,
            'url': comp_data['url']
        }
        processed_components.append(processed_comp)
    
    # Sort components by latest year (newest first)
    processed_components.sort(key=lambda x: x.get('latest_year', ''), reverse=True)
    
    # Update response with processed data
    response['components'] = processed_components
    response['results_count'] = len(processed_components)
    
    # Generate a summary
    if response['results_count'] > 0:
        tech_types = set(c.get('technology', '') for c in processed_components if c.get('technology'))
        tech_summary = ', '.join(sorted(tech_types)) if tech_types else 'various technologies'
        
        # Count how many are in current market
        current_market_count = sum(1 for c in processed_components if "Currently in market" in c.get('market_status', ''))
        
        # If this was a location query, include the location in the summary
        if is_location_query and location_name:
            response['summary'] = f"Found {response['results_count']} distinct components "
            response['summary'] += f"of {tech_summary} in or near {location_name.title()}."
            if current_market_count > 0:
                response['summary'] += f" {current_market_count} are active in the current market period."
        else:
            response['summary'] = f"Found {response['results_count']} distinct components "
            response['summary'] += f"of {tech_summary} in the UK Capacity Market."
            if current_market_count > 0:
                response['summary'] += f" {current_market_count} are active in the current market period."
    else:
        if is_location_query and location_name:
            response['summary'] = f"No matching components found in {location_name.title()}."
        else:
            response['summary'] = "No matching components found in the UK Capacity Market."
    
    return response

def help_view(request, section=None):
    """
    View for rendering help content.
    If section is specified, only that section is rendered.
    Otherwise, the full help content is rendered.
    """
    context = {
        'section': section
    }
    return render(request, 'checker/help_content.html', context)
