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
logger = logging.getLogger(__name__)

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
                                <span>CMU ID: <strong>{cmu_id}</strong></span>
                                <a href="/components/?q={cmu_id}" class="btn btn-sm btn-info">View Components</a>
                            </div>
                            <div class="small text-muted mt-1">Found {len(cmu_components)} components</div>
                        </div>
                        <div class="card-body">
                            <p><strong>Components:</strong> {len(cmu_components)}</p>
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
                        location_html = location
                        
                        cmu_html += f"""
                            <li class="mb-2">
                                <strong>{location_html}</strong> <span class="text-muted">({len(location_components)} components)</span>
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
                            
                            # ID badge (using DB ID)
                            if db_id:
                                badges.append(f'<span class="badge bg-secondary me-1">ID: {db_id}</span>')
                            
                            # Auction type badge
                            auction_badge_class = "bg-secondary"
                            if "T-1" in auction:
                                auction_badge_class = "bg-warning"
                            elif "T-4" in auction:
                                auction_badge_class = "bg-info"
                            elif "T-3" in auction:
                                auction_badge_class = "bg-success"
                            
                            if auction:
                                badges.append(f'<span class="badge {auction_badge_class} me-1">{auction}</span>')
                            
                            # Delivery year badge
                            if delivery_year:
                                badges.append(f'<span class="badge bg-secondary me-1">Year: {delivery_year}</span>')
                            
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
                                
                            cmu_html += f"""
                                <li>
                                    <div class="mb-1">{badges_html}</div>
                                    <i><a href="{detail_url}">{desc}</a></i>{f" - {tech}" if tech else ""}
                                    <div class="small text-muted">
                                        <strong>DB ID:</strong> {db_id}
                                        {component_id_display} 
                                        {f", <strong>Location:</strong> {component.location}" if component.location else ""}
                                        {f", <strong>CMU ID:</strong> {component.cmu_id}" if component.cmu_id else ""}
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
    
    # Get top companies by component count, excluding empty company names
    top_companies = Component.objects.exclude(company_name__isnull=True) \
                             .exclude(company_name='') \
                             .values('company_name') \
                             .annotate(count=Count('id')) \
                             .order_by('-count')[:20]  # Top 20 companies
    
    # Get technology distribution
    tech_distribution = Component.objects.exclude(technology__isnull=True) \
                                 .exclude(technology='') \
                                 .values('technology') \
                                 .annotate(count=Count('id')) \
                                 .order_by('-count')[:10]  # Top 10 technologies
    
    # Get delivery year distribution - include all years
    year_distribution = Component.objects.exclude(delivery_year__isnull=True) \
                                 .exclude(delivery_year='') \
                                 .values('delivery_year') \
                                 .annotate(count=Count('id')) \
                                 .order_by('delivery_year')  # Order by year ascending
    
    # Get total counts
    total_components = Component.objects.count()
    total_cmus = Component.objects.values('cmu_id').distinct().count()
    total_companies = Component.objects.exclude(company_name__isnull=True) \
                              .exclude(company_name='') \
                              .values('company_name').distinct().count()
    
    # Calculate percentages for visual representation and add normalized company IDs
    for company in top_companies:
        company['percentage'] = (company['count'] / total_components) * 100
        # Add normalized company ID for URL
        company['company_id'] = normalize(company['company_name'])
        
    for tech in tech_distribution:
        tech['percentage'] = (tech['count'] / total_components) * 100
        
    for year in year_distribution:
        year['percentage'] = (year['count'] / total_components) * 100
    
    context = {
        'top_companies': top_companies,
        'tech_distribution': tech_distribution,
        'year_distribution': year_distribution,
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
