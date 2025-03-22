from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from django.views.decorators.http import require_http_methods
import urllib.parse
from django.conf import settings

# Import service functions
from .services.company_search import search_companies_service, get_company_years, get_cmu_details, \
    get_auction_components

from .services.component_search import search_components_service
from .services.component_detail import get_component_details
from .services.company_search import company_detail
from .utils import safe_url_param, from_url_param


def search_companies(request):
    """View function for searching companies using the unified search"""
    # Get the query parameter
    query = request.GET.get("q", "").strip()
    comp_sort = request.GET.get("comp_sort", "desc")  # Sort for component results

    # Setup logging first so it's available
    import logging
    logger = logging.getLogger(__name__)
    
    # Add debug logging
    logger.info(f"DEBUG: search_companies called with query='{query}', comp_sort='{comp_sort}'")

    # If no query, just return the regular company search page
    if not query:
        logger.info("DEBUG: No query, returning regular search page")
        return search_companies_service(request)

    # Get component results if there's a query
    components = []
    component_results = {}
    if query:
        logger.info(f"DEBUG: Getting component results for query='{query}'")
        component_results = search_components_service(request, return_data_only=True)
        logger.info(f"DEBUG: Component results keys: {list(component_results.keys()) if component_results else 'None'}")
        if component_results and query in component_results:
            components = component_results[query]
            logger.info(f"DEBUG: Found {len(components)} components for query")

    # Get company results
    logger.info(f"DEBUG: Getting company results for query='{query}'")
    company_results = search_companies_service(request, return_data_only=True)
    logger.info(f"DEBUG: Company results keys: {list(company_results.keys()) if company_results else 'None'}")

    # Create company links for the unified search
    company_links = []
    if company_results and query in company_results:
        logger.info(f"DEBUG: Found company results for query, building links")
        for company_html in company_results[query]:
            company_links.append(company_html)
        logger.info(f"DEBUG: Created {len(company_links)} company links")

    # Pass both results to the template
    extra_context = {
        'unified_search': True,
        'company_links': company_links,
        'component_results': component_results,
        'component_count': len(components) if components else 0,
        'comp_sort': comp_sort
    }
    
    logger.info(f"DEBUG: Final extra_context: unified_search=True, company_links={len(company_links)}, component_count={len(components) if components else 0}")

    return search_companies_service(request, extra_context=extra_context)


def search_components(request):
    """Redirect to the unified search"""
    query = request.GET.get("q", "")
    if query:
        return redirect(f"/?q={urllib.parse.quote(query)}")
    return redirect("/")


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
    # Add debugging
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"htmx_auction_components called with company_id={company_id}, year={year}, auction_name={auction_name}")
    
    # Convert parameters from URL format (underscores) back to spaces
    year = from_url_param(year)
    auction_name = from_url_param(auction_name)
    
    logger.info(f"After conversion: year={year}, auction_name={auction_name}")

    # Get the HTML for the auction components
    html = get_auction_components(company_id, year, auction_name)
    logger.info(f"get_auction_components returned {len(html)} characters of HTML")
    
    return HttpResponse(html)


@require_http_methods(["GET"])
def htmx_cmu_details(request, cmu_id):
    """HTMX endpoint for lazy loading CMU details"""
    cmu_html = get_cmu_details(cmu_id)
    return HttpResponse(cmu_html)


def debug_mapping_cache(request):
    """Debug endpoint to view the CMU to company mapping cache."""
    from django.core.cache import cache

    cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})

    output = f"<h1>CMU to Company Mapping Cache</h1>"
    output += f"<p>Total entries: {len(cmu_to_company_mapping)}</p>"

    output += "<h2>Sample Entries</h2>"
    output += "<table border='1'><tr><th>CMU ID</th><th>Company Name</th></tr>"

    for i, (cmu_id, company) in enumerate(cmu_to_company_mapping.items()):
        output += f"<tr><td>{cmu_id}</td><td>{company}</td></tr>"
        if i > 20:  # Show only first 20 entries
            break

    output += "</table>"

    # Add a form to add a new mapping manually
    output += """
        <h2>Add Mapping</h2>
        <form method="post">
            <label>CMU ID: <input type="text" name="cmu_id"></label><br>
            <label>Company Name: <input type="text" name="company"></label><br>
            <input type="submit" value="Add Mapping">
        </form>
        """

    # Handle form submission
    if request.method == "POST":
        cmu_id = request.POST.get("cmu_id", "").strip()
        company = request.POST.get("company", "").strip()

        if cmu_id and company:
            # Update the mapping
            cmu_to_company_mapping[cmu_id] = company
            cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 3600)

            # Also update any components for this CMU ID
            from .services.data_access import get_component_data_from_json, save_component_data_to_json
            components = get_component_data_from_json(cmu_id)
            if components:
                for component in components:
                    component["Company Name"] = company
                save_component_data_to_json(cmu_id, components)

            output += f"<p style='color:green'>Added mapping: {cmu_id} -> {company}</p>"
        else:
            output += "<p style='color:red'>Both CMU ID and Company Name are required</p>"

    return HttpResponse(output) # Fixed indentation issue


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


def debug_component_retrieval(request, cmu_id):
    """
    Debug endpoint to directly examine component retrieval for a specific CMU ID.
    This helps identify where components might be getting lost in the data flow.
    """
    import json
    from django.http import JsonResponse
    from .services.data_access import get_component_data_from_json, fetch_components_for_cmu_id, get_json_path
    import os
    
    debug_info = {
        "cmu_id": cmu_id,
        "json_path": get_json_path(cmu_id),
        "json_path_exists": os.path.exists(get_json_path(cmu_id)),
        "json_results": None,
        "json_components_count": 0,
        "api_results": None,
        "api_components_count": 0,
        "combined_results": None,
        "error": None
    }
    
    try:
        # Try to get components from JSON storage
        json_components = get_component_data_from_json(cmu_id)
        if json_components:
            debug_info["json_results"] = "Found components in JSON"
            debug_info["json_components_count"] = len(json_components)
            debug_info["json_components"] = [{
                "Location": comp.get("Location and Post Code", "N/A"),
                "Description": comp.get("Description of CMU Components", "N/A"),
                "Technology": comp.get("Generating Technology Class", "N/A"),
                "Auction": comp.get("Auction Name", "N/A"),
                "Delivery Year": comp.get("Delivery Year", "N/A")
            } for comp in json_components[:5]]  # Limit to first 5 for brevity
        else:
            debug_info["json_results"] = "No components found in JSON"
        
        # Try to get components from API
        api_components, api_time = fetch_components_for_cmu_id(cmu_id)
        if api_components:
            debug_info["api_results"] = f"Found components from API in {api_time:.2f}s"
            debug_info["api_components_count"] = len(api_components)
            debug_info["api_components"] = [{
                "Location": comp.get("Location and Post Code", "N/A"),
                "Description": comp.get("Description of CMU Components", "N/A"),
                "Technology": comp.get("Generating Technology Class", "N/A"),
                "Auction": comp.get("Auction Name", "N/A"),
                "Delivery Year": comp.get("Delivery Year", "N/A")
            } for comp in api_components[:5]]  # Limit to first 5 for brevity
        else:
            debug_info["api_results"] = "No components found from API"
        
        # Check if the combined results would have components
        combined = json_components or api_components or []
        debug_info["combined_results"] = f"Total unique components found: {len(combined)}"
        
        # Add auction name analysis for each component
        auction_analysis = {}
        for comp in combined:
            auction_name = comp.get("Auction Name", "Unknown")
            if auction_name not in auction_analysis:
                auction_analysis[auction_name] = 0
            auction_analysis[auction_name] += 1
        
        debug_info["auction_analysis"] = auction_analysis
        
        return JsonResponse(debug_info)
        
    except Exception as e:
        import traceback
        debug_info["error"] = str(e)
        debug_info["traceback"] = traceback.format_exc()
        return JsonResponse(debug_info, status=500)
