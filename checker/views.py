from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from django.views.decorators.http import require_http_methods
import urllib.parse
from django.conf import settings
import os
import json
import glob
from django.db.models import Count
# Remove Component import from here
# Remove unused checker import
# from capacity_checker import checker 

# Import service functions first
from .services.company_search import search_companies_service, get_company_years, get_cmu_details, company_detail # Import company_detail only once
from .services.component_search import search_components_service
from .services.component_detail import get_component_details
from .utils import safe_url_param, from_url_param
from .services.data_access import get_component_data_from_json, get_json_path, fetch_components_for_cmu_id

# Now import the models
from .models import Component

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
    from django.http import HttpResponse
    from django.db.models import Q
    import logging
    import traceback
    from .utils import normalize, from_url_param
    import time
    import re
    
    # Set a timeout for the entire operation
    start_time = time.time()
    MAX_EXECUTION_TIME = 15  # 15 seconds maximum (to avoid 30-second timeout)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Loading auction components: company_id={company_id}, year={year}, auction={auction_name}")
    
    try:
        # Convert URL parameters from underscores back to spaces
        year = from_url_param(year)
        auction_name = from_url_param(auction_name)
        
        logger.info(f"Parameters after conversion: company_id='{company_id}', year='{year}', auction_name='{auction_name}'")

        # --- FIX: Find all company name variations --- 
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
        # --- END FIX ---

        # Extract auction type for display purposes only
        auction_type = None
        if "T-1" in auction_name:
            auction_type = "T-1"
        elif "T-4" in auction_name:
            auction_type = "T-4"
        elif "T-3" in auction_name:
            auction_type = "T-3"
        else:
            auction_type = "other"
        
        logger.info(f"Auction details: type={auction_type}, full_name={auction_name}")
        
        # Build query using company name variations
        try:
            logger.critical(f"QUERY PARAMS: company_names={company_name_variations}, delivery_year='{year}', auction_name='{auction_name}'")
            
            # --- FIX: Use company_name__in --- 
            base_query = Q(company_name__in=company_name_variations)
            # --- END FIX ---

            # Filter by delivery_year (icontains should be sufficient)
            if year:
                base_query &= Q(delivery_year__icontains=year)

            # --- Stricter Auction Name Matching ---
            if auction_name:
                # Updated Regex to find T-number (allows space)
                t_match = re.search(r'(T-\d|T\s?\d|TR)', auction_name, re.IGNORECASE) 
                # Find the primary year (e.g., 2025 from "2025 26...")
                year_match = re.search(r'(\d{4})', auction_name) 
                
                auction_filter = Q() # Start with an empty filter for auction parts
                
                # 1. Require matching T-number if found in input
                if t_match:
                    t_number_part = t_match.group(1).upper().replace(' ','-') # Normalize to T-X
                    # Require auction_name to contain T-4 or (T-4)
                    auction_filter &= (Q(auction_name__icontains=t_number_part) | Q(auction_name__icontains=f"({t_number_part})"))
                    logger.info(f"Added T-number filter: {t_number_part}")
                else:
                    logger.warning(f"No T-number found in input auction name: {auction_name}")
                    # If no T-number, the query will likely fail, but we proceed

                # 2. Require matching year part if found in input
                if year_match:
                    year_part = year_match.group(1)
                    # Require auction_name to contain the year part (e.g., 2025)
                    auction_filter &= Q(auction_name__icontains=year_part)
                    logger.info(f"Added Year filter: {year_part}")
                else:
                     logger.warning(f"No Year found in input auction name: {auction_name}")
                     # If no year, the query will likely fail, but we proceed

                # 3. Only add the auction_filter if we extracted BOTH parts
                if t_match and year_match:
                     base_query &= auction_filter
                elif auction_name: # Fallback to simple contains if parts couldn't be extracted
                     logger.warning(f"Could not extract required parts, falling back to basic contains: {auction_name}")
                     base_query &= Q(auction_name__icontains=auction_name)
            # --- End Stricter Matching ---


            # === Log the final Q object ===
            logger.critical(f"FINAL QUERY FILTER (Strict): {base_query}")
            # === End Logging ===

            # Get components with the stricter filter
            components = Component.objects.filter(base_query).order_by('cmu_id', 'location')
            
            logger.info(f"Specific query found {components.count()} components.")

        except Exception as query_error:
            logger.error(f"Error in database query: {str(query_error)}")
            logger.error(traceback.format_exc())
            return HttpResponse(f"""
                <div class='alert alert-danger'>
                    <p><strong>Error querying components: {str(query_error)}</strong></p>
                    <p>Please try again or choose a different auction.</p>
                </div>
            """)
        
        # If we found no components, show specific message
        if not components.exists(): # Use exists() for efficiency
            return HttpResponse(f"""
                <div class='alert alert-info'>
                    <p>No components found for this auction: {auction_name}</p>
                    <p>This might be because this company doesn't participate in this specific auction.</p>
                </div>
            """)
        
        # Organize by CMU ID
        try:
            components_by_cmu = {}
            for comp in components:
                cmu_id = comp.cmu_id
                if not cmu_id:
                    continue
                    
                if cmu_id not in components_by_cmu:
                    components_by_cmu[cmu_id] = []
                
                components_by_cmu[cmu_id].append(comp)
            
            # Generate HTML
            html = f"<div class='component-results mb-3'>"
            html += f"<div class='alert alert-info'>Found {len(components_by_cmu)} CMU IDs for auction: {auction_name}</div>"
            
            # Group by CMU ID
            html += "<div class='row'>"
            
            for cmu_id in sorted(components_by_cmu.keys()):
                cmu_components = components_by_cmu.get(cmu_id, [])
                
                # Extract locations
                locations = {}
                for comp in cmu_components:
                    loc = comp.location or "Unknown Location"
                    if loc not in locations:
                        locations[loc] = []
                    locations[loc].append(comp)
                
                # Format component records for the template - full width for each card
                cmu_html = f"""
                <div class="col-12 mb-3">
                    <div class="card cmu-card">
                        <div class="card-header bg-light">
                            <div class="d-flex justify-content-between align-items-center">
                                <a href="/components/?q={cmu_id}" title="Click to view all with this CMU ID" class="badge bg-info text-dark text-decoration-none">
                                    CMU ID: <strong>{cmu_id}</strong>
                                </a>
                                <span class="small text-muted">Components: {len(cmu_components)}</span>
                            </div>
                        </div>
                        <div class="card-body">
                            {f"<p><strong>Components:</strong> {len(cmu_components)}</p>" if len(cmu_components) > 1 else ""}
                """
                
                # Check timeout
                if time.time() - start_time > MAX_EXECUTION_TIME:
                    return HttpResponse("""
                        <div class='alert alert-warning'>
                            <p><strong>The operation is taking too long.</strong></p>
                            <p>We're working on improving performance. Please try again later or choose a different auction.</p>
                        </div>
                    """)
                
                # Add locations list
                if locations:
                    cmu_html += "<ul class='list-unstyled'>"
                    
                    for location, location_components in sorted(locations.items()):
                        # Format location as plain text, not a link
                        location_html = location # Plain text location
                        
                        # Conditionally create the component count text for the location
                        location_count_text = f' <span class="text-muted">({len(location_components)} components)</span>' if len(location_components) > 1 else ''
                        
                        cmu_html += f"""
                            <li class="mb-2">
                                <strong>{location_html}</strong>{location_count_text}
                                <ul class="ms-3">
                        """
                        
                        # Add description of components with badges
                        for component in location_components:
                            desc = component.description or 'No description'
                            tech = component.technology or ''
                            # Use component.id which is the database primary key
                            db_id = component.id 
                            auction = component.auction_name or ''
                            delivery_year = component.delivery_year or ''
                            
                            # --- Get the actual Component ID from additional_data ---
                            actual_component_id = None
                            if isinstance(component.additional_data, dict):
                                actual_component_id = component.additional_data.get("Component ID")
                            # --- End Get actual Component ID ---
                            
                            # Create badges
                            badges = []
                            
                            # REMOVED ID badge (using DB ID)
                            # if db_id:
                            #     badges.append(f'<span class="badge bg-secondary me-1">ID: {db_id}</span>')
                            
                            # REMOVED Delivery year badge
                            # if delivery_year:
                            #     badges.append(f'<span class="badge bg-secondary me-1">Year: {delivery_year}</span>')
                            
                            # Keep only necessary badges (can be empty if none were added)
                            badges_html = " ".join(badges)

                            # Check for potential duplicates in the same location
                            duplicate_warning = ""
                            duplicate_count = sum(1 for c in location_components if 
                                c.location == component.location and 
                                c.description == component.description and
                                c.id != component.id)
                            
                            if duplicate_count > 0:
                                duplicate_warning = f"""
                                    <div class="alert alert-warning py-1 mt-1">
                                        <small>⚠️ Found {duplicate_count} potential duplicate(s) with same location and description</small>
                                    </div>
                                """
                            
                            # Make description the link using database id
                            detail_url = f"/component/{db_id}/" 
                            
                            # --- FIX: Display actual component ID if available --- 
                            component_id_display = ""
                            if actual_component_id:
                                component_id_display = f", <strong>Component ID:</strong> {actual_component_id}"
                            elif component.component_id: # Fallback to model field
                                component_id_display = f", <strong>Comp ID (source _id?):</strong> {component.component_id}"
                            # --- END FIX ---
                                
                            # --- Prepare conditional strings (Cleanly) --- 
                            auction_str = f", <strong>Auction:</strong> {auction}" if auction else ""
                            cmu_id_str = f", <strong>CMU ID:</strong> {component.cmu_id}" if component.cmu_id else ""
                            delivery_year_str = f", <strong>Year:</strong> {delivery_year}" if delivery_year else "" # ADDED Delivery Year string
                            # --- End prepare --- 
                            
                            # --- Construct Details Line Separately --- 
                            details_line_content = f"<strong>DB ID:</strong> {db_id}{component_id_display}{auction_str}{cmu_id_str}{delivery_year_str}"
                            # --- End Construct Details --- 
                            
                            # --- Prepare Technology Badge --- 
                            tech_badge_html = f'<span class="badge bg-success mt-1">{tech}</span>' if tech else ""
                            # --- End Prepare Tech Badge ---
                            
                            cmu_html += f"""
                                <li>
                                    <div class="mb-1">{badges_html}</div>
                                    <i><a href="{detail_url}">{desc}</a></i>
                                    <div>{tech_badge_html}</div>
                                    <div class="small text-muted">
                                        {details_line_content} 
                                    </div>
                                    {duplicate_warning}
                                </li>
                            """
                        
                        cmu_html += """
                                </ul>
                            </li>
                        """
                    
                    cmu_html += "</ul>"
                else:
                    cmu_html += "<p>No location information available</p>"
                
                cmu_html += """
                        </div>
                    </div>
                </div>
                """
                
                html += cmu_html
            
            html += "</div></div>"
            
            return HttpResponse(html)
        except Exception as e:
            logger.error(f"Error processing components: {str(e)}")
            logger.error(traceback.format_exc())
            error_html = f"""
                <div class='alert alert-danger'>
                    <p><strong>Error processing components: {str(e)}</strong></p>
                    <p>Please try again or choose a different auction.</p>
                </div>
            """
            return HttpResponse(error_html)
            
    except Exception as e:
        logger.error(f"Error loading auction components: {str(e)}")
        logger.error(traceback.format_exc())
        error_html = f"""
            <div class='alert alert-danger'>
                <p><strong>We're having trouble loading these components.</strong></p>
                <p>Please try again or choose a different auction.</p>
                <button class="btn btn-primary mt-2" onclick="location.reload()">Try Again</button>
                <p class="small text-muted mt-3">Error: {str(e)[:200]}...</p>
            </div>
        """
        
        if request.GET.get("debug"):
            error_html += f"<pre>{traceback.format_exc()}</pre>"
            
        return HttpResponse(error_html)

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
        # Find all CMU IDs for this company name
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
    from django.db.models import Count, Q # Ensure Q is imported if needed later
    import logging # Add logging
    logger = logging.getLogger(__name__)
    
    # Increase limits for existing lists
    COMPANY_LIMIT = 50 # Increased from 20
    TECH_LIMIT = 25    # Increased from 10
    DERATED_LIMIT = 20 # Limit for the new list
    
    # Get top companies by component count, excluding empty company names
    top_companies = Component.objects.exclude(company_name__isnull=True) \
                             .exclude(company_name='') \
                             .values('company_name') \
                             .annotate(count=Count('id')) \
                             .order_by('-count')[:COMPANY_LIMIT] # Use COMPANY_LIMIT
    
    # Get technology distribution - Reverted to Top N by count
    tech_distribution = Component.objects.exclude(technology__isnull=True) \
                                 .exclude(technology='') \
                                 .values('technology') \
                                 .annotate(count=Count('id')) \
                                 .order_by('-count')[:TECH_LIMIT] # Order by count desc, use TECH_LIMIT
    
    # Get delivery year distribution - include all years
    year_distribution = Component.objects.exclude(delivery_year__isnull=True) \
                                 .exclude(delivery_year='') \
                                 .values('delivery_year') \
                                 .annotate(count=Count('id')) \
                                 .order_by('delivery_year')  # Order by year ascending
    
    # --- New: Get Top Components by De-Rated Capacity (Optimized) --- 
    top_derated_components = []
    try:
        # Fetch only necessary fields for candidates
        candidate_components = Component.objects.exclude(additional_data__isnull=True).only(
            'id', 'location', 'company_name', 'additional_data'
        ).iterator() # Use iterator to avoid loading all into memory at once
        
        top_components_heap = []
        components_processed = 0
        
        for comp in candidate_components:
            components_processed += 1
            if comp.additional_data:
                capacity_str = comp.additional_data.get("De-Rated Capacity") 
                if capacity_str is not None:
                    try:
                        capacity_float = float(capacity_str)
                        component_data = {
                            'id': comp.id,
                            'location': comp.location or "N/A",
                            'company_name': comp.company_name or "N/A",
                            'derated_capacity': capacity_float
                        }
                        
                        # Use heapq to maintain the top N items efficiently
                        if len(top_components_heap) < DERATED_LIMIT:
                            # Push tuple (capacity, data) - heapq sorts by the first element
                            heapq.heappush(top_components_heap, (capacity_float, component_data))
                        elif capacity_float > top_components_heap[0][0]: # Compare with the smallest capacity in the heap
                            # Replace the smallest element with the new larger element
                            heapq.heapreplace(top_components_heap, (capacity_float, component_data))
                            
                    except (ValueError, TypeError):
                        pass # Skip components with non-numeric capacity
                        
        # The heap now contains the top N components, but sorted ascending by capacity.
        # Extract the component data and sort descending for display.
        top_derated_components = sorted([item[1] for item in top_components_heap], 
                                       key=lambda x: x['derated_capacity'], 
                                       reverse=True)
        
        logger.info(f"Processed {components_processed} components for de-rated capacity, found top {len(top_derated_components)}")
        
    except Exception as e:
        logger.error(f"Error processing de-rated capacity ranking: {e}")
    # --- End New Section --- 

    # Get total counts
    total_components = Component.objects.count()
    total_cmus = Component.objects.values('cmu_id').distinct().count()
    total_companies = Component.objects.exclude(company_name__isnull=True) \
                              .exclude(company_name='') \
                              .values('company_name').distinct().count()
    
    # Calculate percentages for visual representation and add normalized company IDs
    for company in top_companies:
        if total_components > 0:
             company['percentage'] = (company['count'] / total_components) * 100
        else:
             company['percentage'] = 0
        company['company_id'] = normalize(company['company_name'])
        
    # Re-enable percentage calculation for tech distribution 
    for tech in tech_distribution:
        if total_components > 0:
            tech['percentage'] = (tech['count'] / total_components) * 100
        else:
             tech['percentage'] = 0
        
    for year in year_distribution:
        if total_components > 0:
            year['percentage'] = (year['count'] / total_components) * 100
        else:
             year['percentage'] = 0
    
    context = {
        'top_companies': top_companies,
        'tech_distribution': tech_distribution,
        'year_distribution': year_distribution,
        'top_derated_components': top_derated_components, # Add new list to context
        'total_components': total_components,
        'total_cmus': total_cmus,
        'total_companies': total_companies,
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
    if sort_field == "date":
        db_sort_field = '-delivery_year' if sort_order == 'desc' else 'delivery_year'
    else:
        # For other fields, we'll sort in Python after fetching data
        db_sort_field = '-delivery_year'  # Default sort for initial query
    
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
        
        # Get all components for this technology
        components_list = list(component_queryset)
        
        # Apply sorting for capacity fields if needed
        if sort_field == "derated_capacity":
            # Sort by de-rated capacity - handle components without this data
            def get_derated_capacity(comp):
                if comp.additional_data and "De-Rated Capacity" in comp.additional_data:
                    try:
                        return float(comp.additional_data["De-Rated Capacity"])
                    except (ValueError, TypeError):
                        return 0  # Default for invalid values
                return 0  # Default for missing values
            
            components_list.sort(
                key=get_derated_capacity,
                reverse=(sort_order == "desc")
            )
        elif sort_field == "mw":
            # Sort by MW (Connection Capacity)
            def get_connection_capacity(comp):
                if comp.additional_data and "Connection Capacity" in comp.additional_data:
                    try:
                        return float(comp.additional_data["Connection Capacity"])
                    except (ValueError, TypeError):
                        return 0
                return 0
            
            components_list.sort(
                key=get_connection_capacity,
                reverse=(sort_order == "desc")
            )
            
        # Apply pagination after sorting
        paginator = Paginator(components_list, per_page)
        try:
            components_page = paginator.page(page)
        except PageNotAnInteger:
            components_page = paginator.page(1)
        except EmptyPage:
            components_page = paginator.page(paginator.num_pages)
            
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
    # --- Add sort parameter handling --- 
    sort_order = request.GET.get("sort", "desc") # Default to descending (largest first)
    if sort_order not in ["asc", "desc"]:
        sort_order = "desc" # Fallback to default if invalid value
    # --- End sort parameter handling ---
    per_page = 50
    start_time = time.time()
    
    # --- Logic adapted from statistics_view --- 
    all_processed_components = []
    error_message = None
    try:
        candidate_components = Component.objects.exclude(additional_data__isnull=True).only(
            'id', 'location', 'company_name', 'additional_data'
        ) 
        
        for comp in candidate_components:
            if comp.additional_data:
                capacity_str = comp.additional_data.get("De-Rated Capacity") 
                if capacity_str is not None:
                    try:
                        capacity_float = float(capacity_str)
                        all_processed_components.append({
                            'id': comp.id,
                            'location': comp.location or "N/A",
                            'company_name': comp.company_name or "N/A",
                            'derated_capacity': capacity_float
                        })
                    except (ValueError, TypeError): 
                        pass # Skip non-numeric
                        
        # Sort by capacity based on sort_order
        reverse_sort = (sort_order == "desc")
        all_processed_components.sort(key=lambda x: x['derated_capacity'], reverse=reverse_sort)
        logger.info(f"Processed and sorted {len(all_processed_components)} components for de-rated capacity ({sort_order}).")
        
    except Exception as e:
        logger.error(f"Error processing de-rated capacity list: {e}")
        error_message = f"Error processing component list: {e}"
        all_processed_components = [] # Ensure list is empty on error
    # --- End adapted logic --- 
    
    # Apply pagination to the full sorted list
    paginator = Paginator(all_processed_components, per_page)
    try:
        components_page = paginator.page(page)
    except PageNotAnInteger:
        components_page = paginator.page(1)
    except EmptyPage:
        components_page = paginator.page(paginator.num_pages)
    
    api_time = time.time() - start_time
    
    context = {
        "page_obj": components_page,
        "paginator": paginator,
        "total_count": len(all_processed_components),
        "api_time": api_time,
        "error": error_message,
        "sort_order": sort_order, # Pass sort order to template
        # Add other necessary context variables if the template requires them
        "page": components_page.number, 
        "per_page": per_page,
        "total_pages": paginator.num_pages,
        "has_prev": components_page.has_previous(),
        "has_next": components_page.has_next(),
        "page_range": paginator.get_elided_page_range(number=components_page.number, on_each_side=1, on_ends=1)
    }

    return render(request, "checker/derated_capacity_list.html", context) # Use a new template
