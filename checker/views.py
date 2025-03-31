from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from django.views.decorators.http import require_http_methods
import urllib.parse
from django.conf import settings
import os
import json
import glob
from django.db.models import Count
from .models import Component

# Import service functions
from .services.company_search import search_companies_service, get_company_years, get_cmu_details

from .services.component_search import search_components_service
from .services.component_detail import get_component_details
from .services.company_search import company_detail
from .utils import safe_url_param, from_url_param
from .services.data_access import get_component_data_from_json, get_json_path, fetch_components_for_cmu_id


def search_companies(request):
    """View function for searching companies using the unified search"""
    # Get the query parameter
    query = request.GET.get("q", "").strip()
    comp_sort = request.GET.get("comp_sort", "desc")  # Sort for component results
    
    # Add pagination parameters but don't change data fetching yet
    page = int(request.GET.get("page", 1))  # Current page, default to 1
    per_page = int(request.GET.get("per_page", 100))  # Items per page, default to 100
    
    # Setup logging
    import logging
    logger = logging.getLogger(__name__)
    
    # If no query, just return the regular company search page
    if not query:
        return search_companies_service(request)

    # Get component results if there's a query - use the EXACT SAME approach as before
    component_results = {}
    total_component_count = 0
    components = []
    
    if query:
        # Get components using the standard service - no changes to this part
        component_results = search_components_service(request, return_data_only=True)
        
        # If we have results, prepare for pagination UI
        if component_results and query in component_results:
            components = component_results[query]
            total_component_count = len(components)
            
            # We're not actually paginating the data yet - just preparing counts for the UI
            # This ensures we don't break the existing functionality

    # Get company results - no changes here
    company_results = search_companies_service(request, return_data_only=True)

    # Create company links for the unified search - no changes
    company_links = []
    if company_results and query in company_results:
        for company_html in company_results[query]:
            company_links.append(company_html)

    # Prepare pagination metadata
    total_component_count = 0
    displayed_component_count = 0
    if component_results and query in component_results:
        displayed_component_count = len(component_results[query])
        
        # Get actual total count from metadata if available
        total_component_count = getattr(component_results, 'total_count', displayed_component_count)        

    # Prepare pagination metadata for UI
    total_pages = (total_component_count + per_page - 1) // per_page if total_component_count > 0 else 1
    has_prev = page > 1
    has_next = page < total_pages
    
    # Pass both results to the template
    extra_context = {
        'unified_search': True,
        'company_links': company_links,
        'component_results': component_results,
        'component_count': total_component_count,
        'displayed_component_count': total_component_count, # For now, show all components
        'comp_sort': comp_sort,
        'query': query,
        # Pagination context (for UI only at first)
        'page': page,
        'per_page': per_page,
        'total_pages': total_pages,
        'has_prev': has_prev,
        'has_next': has_next,
        'page_range': range(max(1, page-2), min(total_pages+1, page+3))
    }

    return search_companies_service(request, extra_context=extra_context)


def search_components(request):
    """View function for searching components with smart pagination"""
    # Get the query parameter
    query = request.GET.get("q", "").strip()
    
    # If no query, just redirect to home
    if not query:
        return redirect("/")
        
    # Smart pagination - adjust page size based on query complexity
    page = int(request.GET.get("page", 1))
    
    # Base page size - standard is 100 items per page
    base_page_size = 100
    
    # If query is very short or contains spaces, it's likely to match many records
    # or be a complex query - reduce page size to prevent timeouts
    if len(query) < 5 or ' ' in query:  
        per_page = 50  # More restrictive for potentially expensive queries
    else:
        # Much more aggressive page size limits
        if len(query) < 3:  
            per_page = 10  # Extremely restrictive for very short queries
        elif len(query) < 5 or ' ' in query:  
            per_page = 20  # Very restrictive for potentially expensive queries
        else:
            per_page = 50  # Still limited even for better queries
        
    # Add parameters to the redirect URL
    redirect_url = f"/?q={urllib.parse.quote(query)}&page={page}&per_page={per_page}"
    
    # Add sorting parameter if present
    if "comp_sort" in request.GET:
        redirect_url += f"&comp_sort={request.GET.get('comp_sort')}"
        
    # Log the pagination settings to help with debugging
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"Component search with query '{query}': page={page}, per_page={per_page}")
    
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
def component_detail(request, component_id):
    """View function for component details page"""
    return get_component_details(request, component_id)


@require_http_methods(["GET"])
def htmx_auction_components(request, company_id, year, auction_name):
    """HTMX endpoint for loading components for a specific auction"""
    from django.http import HttpResponse
    from django.db.models import Q
    import logging
    import traceback
    from .models import Component
    from .utils import normalize, from_url_param
    import time
    import re
    
    # Set a timeout for the entire operation
    start_time = time.time()
    MAX_EXECUTION_TIME = 15  # 15 seconds maximum (to avoid 30-second timeout)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Loading auction components: company_id={company_id}, year={year}, auction={auction_name}")
    
    # Convert URL parameters from underscores back to spaces
    year = from_url_param(year)
    auction_name = from_url_param(auction_name)
    
    # Improved auction type detection with more flexible patterns
    auction_type = None
    if "T-1" in auction_name or "(T-1)" in auction_name or "T1" in auction_name or "(T1)" in auction_name:
        auction_type = "T-1"
    elif "T-4" in auction_name or "(T-4)" in auction_name or "T4" in auction_name or "(T4)" in auction_name:
        auction_type = "T-4"
    elif "T-3" in auction_name or "(T-3)" in auction_name or "T3" in auction_name or "(T3)" in auction_name:
        auction_type = "T-3"
    else:
        # For other auction types, use the first word of the auction name
        auction_type = auction_name.split(" ")[0] if auction_name else "unknown"
    
    # Extract year pattern from auction name
    year_pattern = None
    year_match = re.search(r'(\d{4})[-/]?(\d{2})', auction_name)
    if year_match:
        year_pattern = year_match.group(0)
    
    logger.info(f"Auction details: type={auction_type}, year_pattern={year_pattern}, full_name={auction_name}")
    
    try:
        # Get company name
        company_name = None
        companies = Component.objects.values('company_name').distinct()
        for company in companies:
            if company['company_name'] and normalize(company['company_name']) == company_id:
                company_name = company['company_name']
                break
        
        if not company_name:
            # Try a looser match if exact match fails
            top_companies = Component.objects.values_list('company_name', flat=True).distinct()[:100]
            for curr_name in top_companies:
                if curr_name and (company_id in normalize(curr_name) or normalize(curr_name) in company_id):
                    company_name = curr_name
                    break
        
        if not company_name:
            return HttpResponse(f"<div class='alert alert-warning'>Company not found: {company_id}</div>")
        
        logger.info(f"Found company: {company_name}")
        
        # ===== CRUCIAL SECTION: MODIFIED APPROACH =====
        # Instead of doing complex filtering with database queries,
        # we'll get ALL components for this company and year first,
        # then apply strict filtering entirely in Python
        
        # Base query - just get company and year
        base_query = Q(company_name=company_name)
        if year:
            base_query &= Q(delivery_year__icontains=year)
        
        # Get ALL components for this company and year
        all_components = Component.objects.filter(base_query).order_by('cmu_id', 'location')
        logger.info(f"Found {len(all_components)} total components for company={company_name}, year={year}")
        
        # Now do strict Python-based filtering
        filtered_components = []
        
        for comp in all_components:
            comp_auction = comp.auction_name or ""
            
            # ===== SUPER STRICT AUCTION TYPE MATCHING =====
            # For T-1 auction section, only include components with T-1 in auction name
            # AND explicitly exclude any component with T-4
            if auction_type == "T-1":
                if (("T-1" in comp_auction or "(T-1)" in comp_auction or "T1" in comp_auction) and 
                    not ("T-4" in comp_auction or "(T-4)" in comp_auction or "T4" in comp_auction)):
                    filtered_components.append(comp)
                    
            # For T-4 auction section, only include components with T-4 in auction name
            # AND explicitly exclude any component with T-1
            elif auction_type == "T-4":
                if (("T-4" in comp_auction or "(T-4)" in comp_auction or "T4" in comp_auction) and 
                    not ("T-1" in comp_auction or "(T-1)" in comp_auction or "T1" in comp_auction)):
                    filtered_components.append(comp)
                    
            # For other auction types, use more relaxed filtering
            else:
                if auction_type.lower() in comp_auction.lower():
                    filtered_components.append(comp)
        
        logger.info(f"After strict filtering, found {len(filtered_components)} components for auction type: {auction_type}")
        
        # If we found no components after filtering, show specific message
        if not filtered_components:
            return HttpResponse(f"""
                <div class='alert alert-info'>
                    <p>No components found for this auction type: {auction_type}</p>
                    <p>This might be because this company doesn't participate in this specific auction.</p>
                    <p class="small text-muted">Debug: auction_name="{auction_name}", detected type="{auction_type}"</p>
                </div>
            """)
        
        # Organize by CMU ID
        components_by_cmu = {}
        for comp in filtered_components:
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
            
            # Add locations list
            if locations:
                cmu_html += "<ul class='list-unstyled'>"
                
                for location, location_components in sorted(locations.items()):
                    # Create component ID for linking
                    component_id = f"{cmu_id}_{normalize(location)}"
                    
                    # Format location as a blue link
                    location_html = f'<a href="/component/{component_id}/" style="color: blue; text-decoration: underline;">{location}</a>'
                    
                    cmu_html += f"""
                        <li class="mb-2">
                            <strong>{location_html}</strong> <span class="text-muted">({len(location_components)} components)</span>
                            <ul class="ms-3">
                    """
                    
                    # Add description of components with badges
                    for component in location_components:
                        desc = component.description or 'No description'
                        tech = component.technology or ''
                        component_id = component.component_id or ''
                        auction = component.auction_name or ''
                        delivery_year = component.delivery_year or ''
                        
                        # Create badges
                        badges = []
                        
                        # Component ID badge - important to display this
                        if component_id:
                            badges.append(f'<span class="badge bg-secondary me-1">ID: {component_id}</span>')
                        
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
                        
                        cmu_html += f"""
                            <li>
                                <div class="mb-1">{badges_html}</div>
                                <i>{desc}</i>{f" - {tech}" if tech else ""}
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
        logger.error(f"Error loading auction components: {str(e)}")
        logger.error(traceback.format_exc())
        error_html = f"""
            <div class='alert alert-danger'>
                <p><strong>We're having trouble loading these components.</strong></p>
                <p>Please try again or choose a different auction.</p>
                <button class="btn btn-primary mt-2" onclick="location.reload()">Try Again</button>
            </div>
        """
        
        if request.GET.get("debug"):
            error_html += f"<pre>{traceback.format_exc()}</pre>"
            
        return HttpResponse(error_html)


@require_http_methods(["GET"])
def htmx_cmu_details(request, cmu_id):
    """HTMX endpoint for lazy loading CMU details"""
    cmu_html = get_cmu_details(cmu_id)
    return HttpResponse(cmu_html)


def debug_mapping(request):
    """Debug view to examine the CMU ID to company name mapping"""
    from django.http import JsonResponse
    from .services.company_search import get_cmu_dataframe
    import traceback
    
    debug_info = {
        "status": "success",
        "message": "CMU to company mapping debug",
    }
    
    try:
        # Get CMU dataframe
        cmu_df, df_api_time = get_cmu_dataframe()
        
        if cmu_df is None:
            debug_info["status"] = "error"
            debug_info["message"] = "Error loading CMU data"
            return JsonResponse(debug_info, status=500)
            
        # Create mapping
        cmu_to_company = {}
        for _, row in cmu_df.iterrows():
            cmu_id = row.get("CMU ID", "")
            company = row.get("Full Name", "")
            if cmu_id and company:
                cmu_to_company[cmu_id] = company
                
        # Return a sample of the mapping
        debug_info["mapping_size"] = len(cmu_to_company)
        debug_info["mapping_sample"] = dict(list(cmu_to_company.items())[:10])
        
        return JsonResponse(debug_info)
    except Exception as e:
        debug_info["status"] = "error"
        debug_info["message"] = f"Error building mapping: {str(e)}"
        debug_info["traceback"] = traceback.format_exc()
        return JsonResponse(debug_info, status=500)
        

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
    
    # Get top companies by component count
    top_companies = Component.objects.values('company_name') \
                             .annotate(count=Count('id')) \
                             .order_by('-count')[:20]  # Top 20 companies
    
    # Get technology distribution
    tech_distribution = Component.objects.values('technology') \
                                 .annotate(count=Count('id')) \
                                 .order_by('-count')[:10]  # Top 10 technologies
    
    # Get delivery year distribution
    year_distribution = Component.objects.values('delivery_year') \
                                 .annotate(count=Count('id')) \
                                 .order_by('-count')[:10]
    
    # Get total counts
    total_components = Component.objects.count()
    total_cmus = Component.objects.values('cmu_id').distinct().count()
    total_companies = Component.objects.values('company_name').distinct().count()
    
    # Calculate percentages for visual representation
    for company in top_companies:
        company['percentage'] = (company['count'] / total_components) * 100
        
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
