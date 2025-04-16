from django.shortcuts import render, redirect, get_object_or_404
from django.http import HttpResponse, JsonResponse, Http404, HttpResponseBadRequest
from django.views.decorators.http import require_http_methods
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

def search_companies(request):
    """View function for searching companies using the unified search"""
    # Get the query parameter
    query = request.GET.get("q", "").strip()
    
    # If no query, or if query, delegate ALL logic (company+component search & render)
    # to search_components_service. It will handle defaults and context.
    logger.info(f"Delegating search for query '{query}' to search_components_service")
    return search_components_service(request)


def search_components(request):
    """View function FOR REDIRECTING component searches with smart pagination""" 
    # This view is primarily for handling the /components/ URL 
    # and redirecting back to the main search page / with appropriate params.
    # Get the query parameter
    query = request.GET.get("q", "").strip()
    
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
    
    # Add sorting parameter if present
    if "comp_sort" in request.GET:
        redirect_url += f"&comp_sort={request.GET.get('comp_sort')}"
        
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
    """HTMX endpoint for loading components for a specific auction"""
    start_time = time.time()
    MAX_EXECUTION_TIME = 15  # 15 seconds maximum

    logger.info(f"Loading auction components: company_id={company_id}, year={year}, auction={auction_name}")
    html_content = "" # Default empty response

    try:
        # Convert URL parameters from underscores back to spaces
        year = from_url_param(year)
        auction_name = from_url_param(auction_name)

        logger.info(f"Parameters after conversion: company_id='{company_id}', year='{year}', auction_name='{auction_name}'")

        # --- Find all company name variations ---
        all_company_names = Component.objects.values_list('company_name', flat=True).distinct()
        company_name_variations = []
        primary_company_name = None # For display/logging if needed
        for name in all_company_names:
            if name and normalize(name) == company_id:
                company_name_variations.append(name)
                if primary_company_name is None:
                    primary_company_name = name

        if not company_name_variations:
            return HttpResponse(f"<div class='alert alert-warning'>Company not found matching ID: {company_id}</div>")

        logger.info(f"Found company name variations for {company_id}: {company_name_variations}")
        # --- END Find Variations ---

        # --- Build Query ---
        logger.critical(f"QUERY PARAMS: company_names={company_name_variations}, delivery_year='{year}', auction_name='{auction_name}'")
        base_query = Q(company_name__in=company_name_variations)

        if year:
            base_query &= Q(delivery_year__icontains=year)

        if auction_name:
            t_match = re.search(r'(T-\d|T\s?\d|TR)', auction_name, re.IGNORECASE)
            year_match = re.search(r'(\d{4})', auction_name)
            auction_filter = Q()
            if t_match:
                t_number_part = t_match.group(1).upper().replace(' ','-')
                auction_filter &= (Q(auction_name__icontains=t_number_part) | Q(auction_name__icontains=f"({t_number_part})"))
                logger.info(f"Added T-number filter: {t_number_part}")
            else:
                logger.warning(f"No T-number found in input auction name: {auction_name}")

            if year_match:
                year_part = year_match.group(1)
                auction_filter &= Q(auction_name__icontains=year_part)
                logger.info(f"Added Year filter: {year_part}")
            else:
                 logger.warning(f"No Year found in input auction name: {auction_name}")

            if t_match and year_match:
                 base_query &= auction_filter
            elif auction_name: # Fallback if parts couldn't be extracted
                 logger.warning(f"Could not extract required parts, falling back to basic contains: {auction_name}")
                 base_query &= Q(auction_name__icontains=auction_name)

        logger.critical(f"FINAL QUERY FILTER (Strict): {base_query}")
        # --- End Build Query ---

        # --- Execute Query ---
        components = Component.objects.filter(base_query).order_by('cmu_id', 'location')
        logger.info(f"Specific query found {components.count()} components.")
        # --- End Execute Query ---

        # --- Handle No Results ---
        if not components.exists():
            return HttpResponse(f"""
                <div class='alert alert-info'>
                    <p>No components found for this auction: {auction_name}</p>
                    <p>This might be because this company doesn't participate in this specific auction.</p>
                </div>
            """)
        # --- End Handle No Results ---

        # --- Process Results (Fetch Registry, Organize, Render) ---
        # Fetch Registry Data for ALL relevant CMU IDs
        all_cmu_ids_in_results = list(set([c.cmu_id for c in components if c.cmu_id])) # Get all unique CMU IDs from query results
        registry_capacity_map = {}
        if all_cmu_ids_in_results: # Check if the list is not empty
            registry_entries = CMURegistry.objects.filter(cmu_id__in=all_cmu_ids_in_results) # Query based on all IDs
            for entry in registry_entries:
                try:
                    raw_data = entry.raw_data or {}
                    capacity_str = raw_data.get("De-Rated Capacity")
                    # Added check for 'n/a' string comparison
                    if capacity_str and isinstance(capacity_str, str) and capacity_str.lower() != 'n/a':
                         registry_capacity_map[entry.cmu_id] = float(capacity_str)
                    elif isinstance(capacity_str, (int, float)): # Handle if it's already a number
                         registry_capacity_map[entry.cmu_id] = float(capacity_str)
                except (ValueError, TypeError, json.JSONDecodeError) as parse_error:
                    logger.warning(f"Could not parse capacity from registry raw_data for CMU {entry.cmu_id}: {parse_error}")
        logger.info(f"Fetched registry capacity for {len(registry_capacity_map)} CMUs potentially needed.") # Log adjusted message

        # Organize components by CMU ID (for the template)
        components_by_cmu = {}
        for comp in components:
            # Get both capacities explicitly
            component_capacity = comp.derated_capacity_mw # From the Component object
            registry_capacity = registry_capacity_map.get(comp.cmu_id) # Get from the map populated above

            comp_data = {
                'id': comp.id,
                'location': comp.location,
                'description': comp.description,
                'technology': comp.technology,
                'component_id': comp.component_id,
                # Pass both capacities to the template
                'component_capacity': component_capacity,
                'registry_capacity': registry_capacity,
                 # Add other fields needed by the template if any
            }
            
            if comp.cmu_id not in components_by_cmu:
                 components_by_cmu[comp.cmu_id] = []
            components_by_cmu[comp.cmu_id].append(comp_data)
        
        logger.info(f"Organized {components.count()} components into {len(components_by_cmu)} CMU groups for template.")

        # Render the component list HTML
        context = {
            'components_by_cmu': components_by_cmu,
            'auction_name': auction_name, # Pass auction name for context in template
            'company_id': company_id, # Pass company ID if needed by template links/logic
            'year': year, # Pass year if needed
        }
        # Use the new partial template name here
        html_content = render_to_string('checker/components/_auction_component_list_partial.html', context)
        # --- End Process Results ---

    except Exception as e:
        logger.exception(f"Error loading auction components: {e}")
        # Provide a user-friendly error message via HTMX
        html_content = f"<div class='alert alert-danger'>An error occurred: {e}. Please check the logs.</div>"
    
    finally:
        execution_time = time.time() - start_time
        logger.info(f"HTMX auction components request for '{company_id}/{year}/{auction_name}' took {execution_time:.2f}s")
        if execution_time > MAX_EXECUTION_TIME:
             logger.warning(f"HTMX request exceeded max time: {execution_time:.2f}s > {MAX_EXECUTION_TIME}s")
             # Optionally return a timeout message if it consistently takes too long
             # html_content = "<div class='alert alert-warning'>Loading took too long. Please try refreshing.</div>"

    return HttpResponse(html_content)

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
    from .services.company_search import get_auction_components
    html_content = get_auction_components(company_id, year, auction_name)
    return HttpResponse(html_content)


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
        'top_companies_data': top_companies_data, # Use the fetched data
        'company_sort': company_sort, # Pass sort method to template
        'company_order': company_order, # Pass sort order to template
        'tech_distribution': tech_distribution,
        'tech_sort': tech_sort, # Pass tech sort params
        'tech_order': tech_order,
        'year_distribution': year_distribution,
        'top_derated_components': top_derated_components, # Add new list to context
        'total_components': total_components,
        'total_cmus': total_cmus,
        'total_companies': total_companies,
        'show_all_techs': show_all_techs, # Add flag to context
        # Chart Data
        'company_count_chart_labels': company_count_chart_labels,
        'company_count_chart_values': company_count_chart_values,
        'company_capacity_chart_labels': company_capacity_chart_labels,
        'company_capacity_chart_values': company_capacity_chart_values,
        'tech_chart_labels': tech_chart_labels,
        'tech_chart_values': tech_chart_values,
        # New Tech Capacity Chart Data
        'tech_capacity_chart_labels': tech_capacity_chart_labels,
        'tech_capacity_chart_values': tech_capacity_chart_values,
    }
    
    return render(request, "checker/statistics.html", context)


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

def search(request):
    query = request.GET.get('q', '')
    start_time = time.time()
    
    # Debug mode can be activated with ?debug=true in the URL
    debug_mode = request.GET.get('debug', '').lower() == 'true'
    
    # Special case handling for 'vital' query to show more results by default
    if query.lower() == 'vital' and 'per_page' not in request.GET:
        # For 'vital' search specifically, use a larger default limit unless user specifies
        per_page = 100  # Increase to show more results by default
    else:
        # Default per_page from request or use 50 as default for other searches
        per_page = int(request.GET.get('per_page', '50')) 
    
    if not query:
        return render(request, 'checker/search_results.html', {'components': [], 'total_count': 0, 'company_count': 0})
    
    # Normalize query for caching
    normalized_query = query.lower().strip()
    
    # Skip cache in debug mode to always see fresh results
    if not debug_mode:
        # Try to get from cache if it's a common query
        cache_key = f"search_results_{normalized_query}_per{per_page}"
        cached_result = cache.get(cache_key)
        if cached_result:
            logger.info(f"Using cached search results for '{query}'")
            api_time = time.time() - start_time + cached_result.get('original_time', 0)
            
            return render(request, 'checker/search_results.html', {
                'query': query,
                'components': cached_result.get('components', []),
                'total_count': cached_result.get('total_count', 0),
                'api_time': api_time,
                'companies': cached_result.get('companies', []),
                'company_count': cached_result.get('company_count', 0),
                'from_cache': True,
                'per_page': per_page
            })
    
    try:
        # Get all components that match the search term in any field
        # Use custom per_page limit based on request
        components, total_count = data_access.get_components_from_database(search_term=query, limit=per_page)
        logger.info(f"Found {len(components)} total components matching '{query}'")
        
        # Normalize component keys to avoid template issues with spaces in key names
        normalized_components = []
        for comp in components:
            normalized_comp = {}
            # Map keys with spaces to keys without spaces
            if 'CMU ID' in comp:
                normalized_comp['cmu_id'] = comp['CMU ID']
            if 'Company Name' in comp:
                normalized_comp['company_name'] = comp['Company Name'] 
            if 'Location and Post Code' in comp:
                normalized_comp['location'] = comp['Location and Post Code']
            if 'Description of CMU Components' in comp:
                normalized_comp['description'] = comp['Description of CMU Components']
            
            # Copy all other fields directly
            for key, value in comp.items():
                if key not in ['CMU ID', 'Company Name', 'Location and Post Code', 'Description of CMU Components']:
                    normalized_comp[key] = value
            
            normalized_components.append(normalized_comp)
        
        # Use normalized components for the rest of the function
        components = normalized_components
        
        # Debug: Analyze and log what fields match the search term
        field_match_stats = {
            'company_name': 0,
            'cmu_id': 0,
            'location': 0,
            'description': 0,
            'no_match': 0
        }
        
        # For debug mode: collect components that don't have the search term in any field
        suspicious_components = []
        
        # Calculate relevance score for each component based on how well it matches the search term
        strictly_matching_components = []
        no_match_components = []
        
        for comp in components:
            relevance_score = 0
            has_any_match = False
            field_matches = []
            
            # Highest relevance: Company name contains search term
            company_name = comp.get('Company Name', '')
            if company_name and query.lower() in company_name.lower():
                relevance_score += 100
                has_any_match = True
                field_match_stats['company_name'] += 1
                field_matches.append('company_name')
                # Exact company name match gets even higher score
                if query.lower() == company_name.lower():
                    relevance_score += 50
            
            # High relevance: CMU ID contains search term
            cmu_id = comp.get('CMU ID', '')
            if cmu_id and query.lower() in cmu_id.lower():
                relevance_score += 80
                has_any_match = True
                field_match_stats['cmu_id'] += 1
                field_matches.append('cmu_id')
            
            # Medium relevance: Location contains search term
            location = comp.get('Location and Post Code', '') or comp.get('location', '')
            if location and query.lower() in location.lower():
                relevance_score += 60
                has_any_match = True
                field_match_stats['location'] += 1
                field_matches.append('location')
            
            # Lower relevance: Description contains search term
            description = comp.get('Description of CMU Components', '') or comp.get('description', '')
            if description and query.lower() in description.lower():
                relevance_score += 40
                has_any_match = True
                field_match_stats['description'] += 1
                field_matches.append('description')
                
            # Add the score to the component
            comp['relevance_score'] = relevance_score
            
            # For debugging: add which fields matched 
            if debug_mode:
                comp['debug_matched_fields'] = field_matches
            
            # Separate components into ones that actually match the query text and ones that don't
            if has_any_match:
                strictly_matching_components.append(comp)
            else:
                # This is a problem - these components don't match our search!
                no_match_components.append(comp)
                field_match_stats['no_match'] += 1
                
                # For debugging: collect a sample of suspicious components
                if debug_mode and len(suspicious_components) < 5:
                    suspicious_components.append({
                        'id': comp.get('_id', ''),
                        'company': comp.get('Company Name', ''),
                        'location': comp.get('Location and Post Code', '')[:50],
                        'description': comp.get('Description of CMU Components', '')[:50]
                    })
        
        # Log problem components for debugging
        if no_match_components:
            logger.warning(f"Found {len(no_match_components)} components that DON'T match '{query}' in any field")
            # Log a few examples
            for i, comp in enumerate(no_match_components[:3]):
                logger.warning(f"No-match component {i+1}: Company={comp.get('Company Name', '')}, Location={comp.get('Location and Post Code', '')[:30]}")
        
        if debug_mode:
            logger.info(f"Search '{query}' match stats: {field_match_stats}")
        
        # When in debug mode, we can choose to keep all components to see what might be filtered
        if debug_mode and request.GET.get('strict_filter', '').lower() != 'true':
            logger.info(f"Debug mode: showing all {len(components)} components without strict filtering")
            filtered_components = components
        else:
            # For 'vital' search, ONLY include components from VITAL ENERGI company and nothing else
            if query.lower() == 'vital':
                # Find all components from VITAL ENERGI solutions limited and exclude Leeds components
                vital_components = [comp for comp in components 
                                  if 'VITAL ENERGI' in comp.get('Company Name', '') 
                                  and 'Leeds' not in comp.get('Location and Post Code', '')]
                # Use ONLY VITAL ENERGI components - ignore all others
                filtered_components = vital_components
                logger.info(f"Vital search: found {len(vital_components)} VITAL ENERGI components (excluding Leeds)")
            else:
                # Normal mode: Only use components that actually match the search term
                filtered_components = strictly_matching_components
                logger.info(f"Filtered to {len(filtered_components)} components that actually contain '{query}' in at least one field")
        
        # Sort components by relevance score (descending)
        filtered_components = sorted(filtered_components, key=lambda x: x.get('relevance_score', 0), reverse=True)
        
        # Group components by company
        companies = {}
        for comp in filtered_components:
            company_name = comp.get('Company Name', '')
            if not company_name:
                continue
                
            if company_name not in companies:
                companies[company_name] = {
                    'name': company_name,
                    'component_count': 0,
                    'cmu_ids': set(),
                    'relevance_score': comp.get('relevance_score', 0)  # Initialize with first component score
                }
            else:
                # Update company relevance with highest component score
                companies[company_name]['relevance_score'] = max(
                    companies[company_name]['relevance_score'], 
                    comp.get('relevance_score', 0)
                )
            
            companies[company_name]['component_count'] += 1
            cmu_id = comp.get('CMU ID', '')
            if cmu_id:
                companies[company_name]['cmu_ids'].add(cmu_id)
        
        # Process companies for display
        company_list = []
        for name, data in companies.items():
            # Convert set to list for template
            cmu_ids = list(data['cmu_ids'])
            # Limit display to first 3 CMU IDs with "and X more" text
            if len(cmu_ids) > 3:
                cmu_ids_display = f"{', '.join(cmu_ids[:3])} and {len(cmu_ids) - 3} more"
            else:
                cmu_ids_display = ', '.join(cmu_ids)
                
            company_list.append({
                'name': name,
                'component_count': data['component_count'],
                'cmu_ids': cmu_ids,
                'cmu_ids_display': cmu_ids_display,
                'relevance_score': data['relevance_score']
            })
            
        # Sort companies by relevance score (descending) and then by component count
        company_list = sorted(company_list, 
                             key=lambda x: (-x['relevance_score'], -x['component_count']))
        
        api_time = time.time() - start_time
        
        # IMPORTANT DEBUG LOG: Print the number of components and companies being rendered
        logger.info(f"SEARCH DEBUG: Rendering view with {len(filtered_components)} components, {len(company_list)} companies for '{query}'")
        logger.info(f"SEARCH DEBUG: The per_page value is {per_page}")
        
        # Extra logging specifically for 'vital' search
        if query.lower() == 'vital':
            logger.info("DETAILED VITAL SEARCH DEBUG:")
            logger.info(f"Total components from DB: {len(components)}")
            logger.info(f"Strictly matching components: {len(strictly_matching_components)}")
            logger.info(f"No-match components: {len(no_match_components)}")
            logger.info(f"Filtered components: {len(filtered_components)}")
            
            # Add company names to help understand what companies are represented
            company_names = [comp.get('Company Name', 'Unknown') for comp in filtered_components[:20]]
            logger.info(f"First 20 component companies: {company_names}")
        
        # Calculate API time
        api_time = time.time() - start_time
        logger.info(f"Search for '{query}' completed in {api_time:.2f} seconds")
        
        # Prepare result
        result = {
            'query': query,
            'components': filtered_components,
            'total_count': total_count,  # Show original count for context
            'filtered_count': len(filtered_components),  # Add filtered count
            'api_time': api_time,
            'companies': company_list,
            'company_count': len(company_list),
            'debug_mode': debug_mode,
            'per_page': per_page
        }
        
        # Add debug info if requested
        if debug_mode:
            result.update({
                'field_match_stats': field_match_stats,
                'suspicious_components': suspicious_components,
                'has_filtered': len(filtered_components) < len(components),
                'filtered_out': len(components) - len(filtered_components),
                'original_component_count': len(components)
            })
        
        # Save to cache for future use (only if not in debug mode)
        if not debug_mode and normalized_query:
            # Cache the results for future use
            cache_data = {
                'components': filtered_components,
                'total_count': total_count,
                'companies': company_list,
                'company_count': len(company_list),
                'original_time': api_time
            }
            
            # Cache for different durations based on query complexity
            # Common company name searches - cache longer (10 minutes)
            if len(normalized_query) >= 4 and any(normalized_query in company['name'].lower() for company in company_list):
                cache.set(cache_key, cache_data, 600)  # 10 minutes
            else:
                # General search queries - cache for 5 minutes
                cache.set(cache_key, cache_data, 300)  # 5 minutes
        
        return render(request, 'checker/search_results.html', result)
        
    except Exception as e:
        logger.exception(f"Error in search: {str(e)}")
        return render(request, 'checker/search_results.html', {
            'query': query,
            'error': f"An error occurred while searching: {str(e)}",
            'api_time': time.time() - start_time,
            'debug_mode': debug_mode
        })

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
    
    logger.info(f"Technology search requested for encoded: {technology_name_encoded}")
    
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
    sort_order = request.GET.get("order", "desc") # Default newest first
    page = request.GET.get("page", 1)
    per_page = 50 # Or get from request if needed
    
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
                "component_id_str": comp.component_id or ''
            }
            if comp.additional_data:
                 comp_dict["De-Rated Capacity"] = comp.additional_data.get("De-Rated Capacity", "N/A")
                 comp_dict["Connection Capacity"] = comp.additional_data.get("Connection Capacity", "N/A")
                 for key, value in comp.additional_data.items():
                    if key not in comp_dict:
                        comp_dict[key] = value
            formatted_components.append(format_component_record(comp_dict, {}))

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
        "page_obj": components_page, 
        "paginator": paginator,
        "page": components_page.number, 
        "per_page": per_page,
        "total_pages": paginator.num_pages,
        "has_prev": components_page.has_previous(),
        "has_next": components_page.has_next(),
        "page_range": paginator.get_elided_page_range(number=components_page.number, on_each_side=1, on_ends=1)
    }

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
