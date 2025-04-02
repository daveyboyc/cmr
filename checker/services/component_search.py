import urllib.parse
import time
import logging
from django.shortcuts import render
from django.core.cache import cache
import traceback
from django.urls import reverse

from ..utils import normalize, get_cache_key
from .data_access import (
    fetch_components_for_cmu_id, 
    get_components_from_database
)
from .company_search import _perform_company_search, get_cmu_dataframe, _build_search_results

logger = logging.getLogger(__name__)


def search_components_service(request, return_data_only=False):
    """Service function for searching companies AND components in a unified interface."""
    # Get query and pagination parameters
    query = request.GET.get("q", "").strip()
    page = int(request.GET.get("page", 1))
    per_page = int(request.GET.get("per_page", 50))  # Default to 50 items per page
    sort_order = request.GET.get("comp_sort", "desc")

    # Initialize context variables
    company_links = []
    component_results_dict = {}
    total_component_count = 0
    displayed_component_count = 0
    total_pages = 1
    has_prev = False
    has_next = False
    page_range = range(1, 2)
    error_message = None
    api_time = 0
    note = None # For messages like "Using cached results"

    # Debug info
    debug_info = {
        "query": query,
        "page": page,
        "per_page": per_page,
        "sort_order": sort_order,
        "company_search_attempted": False,
        "component_search_attempted": False,
        "final_total_components": 0,
        "final_displayed_components": 0,
        "data_source": "unknown"
    }

    # If no query, just render the empty search page
    if not query:
        logger.info("Rendering empty search page (no query)")
        return render(request, "checker/search.html", {
            "query": query,
            "company_links": [],
            "component_results": {},
            "component_count": 0,
            "displayed_component_count": 0,
            "page": 1,
            "total_pages": 1,
            "has_prev": False,
            "has_next": False,
            "page_range": range(1, 2),
            "sort_order": sort_order,
            "debug_info": debug_info
        })

    # --- START SEARCH LOGIC (if query exists) --- 
    start_time = time.time()
    
    # Check cache first
    cache_key = get_cache_key("search_results", query.lower())
    cached_results = cache.get(cache_key)
    if cached_results:
        logger.info(f"Using cached results for query '{query}'")
        note = "Using cached results"
        component_results_dict = cached_results
        total_component_count = sum(len(matches) for matches in cached_results.values())
        displayed_component_count = min(per_page, total_component_count)
        api_time = 0
    else:
        # STEP 1: Search for matching companies (using dataframe approach)
        debug_info["company_search_attempted"] = True
        try:
            cmu_df, df_api_time = get_cmu_dataframe()
            api_time += df_api_time
            if cmu_df is not None:
                norm_query = normalize(query)
                matching_records = _perform_company_search(cmu_df, norm_query)
                # Limit companies shown for performance, use _build_search_results for formatting
                unique_companies = list(matching_records["Full Name"].unique())[:20] 
                company_results_built = _build_search_results(cmu_df, unique_companies, sort_order, query, cmu_limit=3)
                if query in company_results_built:
                    company_links = company_results_built[query]
                logger.info(f"Found {len(company_links)} matching company links for '{query}'")
            else:
                error_message = "Error loading company data (CMU dataframe)."
                logger.error(error_message)
        except Exception as e:
            error_message = f"Error searching companies: {str(e)}"
            logger.exception(error_message)
            debug_info["company_error"] = error_message

        # STEP 2: Search for matching components (using database fetch)
        debug_info["component_search_attempted"] = True
        components_list = []
        try:
            # First try location-based search since it's faster
            components_list = get_components_from_database(location=query, page=page, per_page=per_page)
            
            if not components_list:
                # If no location matches, try company name search
                components_list = get_components_from_database(company_name=query, page=page, per_page=per_page)
            
            if not components_list:
                # Finally, try CMU ID search
                components_list = get_components_from_database(cmu_id=query, page=page, per_page=per_page)
            
            # Get total count for pagination
            total_component_count = len(components_list)
            displayed_component_count = len(components_list)
            debug_info["data_source"] = "database"
            
            logger.info(f"Fetched {len(components_list)} components for '{query}' page {page}")
            
            # Cache the results
            if components_list:
                cache.set(cache_key, {query: components_list}, 3600)  # Cache for 1 hour
            
        except Exception as e:
            error_msg = f"Error fetching component data: {str(e)}"
            logger.exception(error_msg)
            if not error_message: error_message = error_msg
            debug_info["component_error"] = error_msg
            components_list = []
            total_component_count = 0

        # STEP 3: Format component results for display
        formatted_components = []
        if components_list:
            for component in components_list:
                formatted_record = format_component_record(component, {})
                formatted_components.append(formatted_record)
            
            if formatted_components:
                component_results_dict[query] = formatted_components
                logger.info(f"Formatted {len(formatted_components)} component records for display.")

    # STEP 4: Calculate final pagination variables
    if total_component_count > 0 and per_page > 0:
        total_pages = (total_component_count + per_page - 1) // per_page
    else:
        total_pages = 1
        
    has_prev = page > 1
    has_next = page < total_pages
    # Ensure page range is sensible
    page_range_start = max(1, page - 2)
    page_range_end = min(total_pages + 1, page + 3)
    # Prevent range end from being less than start if total_pages is small
    if page_range_end <= page_range_start:
         page_range_end = page_range_start + 1 
    page_range = range(page_range_start, page_range_end)

    # Update debug info with final counts
    debug_info["final_total_components"] = total_component_count
    debug_info["final_displayed_components"] = displayed_component_count

    # STEP 5: Prepare context and render template
    context = {
        "query": query,
        "note": note,
        "company_links": company_links,
        "component_results": component_results_dict, 
        "component_count": total_component_count,
        "displayed_component_count": displayed_component_count,
        "error": error_message,
        "api_time": api_time,
        "sort_order": sort_order,
        "debug_info": debug_info,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "has_prev": has_prev,
        "has_next": has_next,
        "page_range": page_range,
        "unified_search": True
    }

    return render(request, "checker/search.html", context)


def format_component_record(record, cmu_to_company_mapping):
    """Format a component record for display with proper company badge"""
    # Get base fields with null checks
    loc = record.get("Location and Post Code", "N/A")
    if loc is None:
        loc = "N/A"
        
    desc = record.get("Description of CMU Components", "N/A")
    if desc is None:
        desc = "N/A"
        
    tech = record.get("Generating Technology Class", "N/A")
    if tech is None:
        tech = "N/A"
        
    typ = record.get("Type", "N/A")
    if typ is None:
        typ = "N/A"
        
    delivery_year = record.get("Delivery Year", "N/A")
    if delivery_year is None:
        delivery_year = "N/A"
        
    auction = record.get("Auction Name", "N/A")
    if auction is None:
        auction = "N/A"
        
    cmu_id = record.get("CMU ID", "N/A")
    if cmu_id is None:
        cmu_id = "N/A"

    # Get company name with multiple fallbacks
    company_name = ""
    if "Company Name" in record and record["Company Name"]:
        company_name = record["Company Name"]
    if not company_name:
        company_name = cmu_to_company_mapping.get(cmu_id, "")
        if not company_name:
            for mapping_id, mapping_name in cmu_to_company_mapping.items():
                if mapping_id.lower() == cmu_id.lower():
                    company_name = mapping_name
                    break
    if not company_name:
        try:
            # DEPRECATED - Should rely on database fetch in calling function
            # json_components = get_component_data_from_json(cmu_id)
            # if json_components:
            #     for comp in json_components:
            #         if "Company Name" in comp and comp["Company Name"]:
            #             company_name = comp["Company Name"]
            #             cmu_to_company_mapping[cmu_id] = company_name
            #             cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 3600)
            #             break
            pass # Keep pass here as fallback logic removed
        except Exception as e:
            logger.error(f"Error getting company name: {e}")

    # Create company badge
    company_info = ""
    if company_name:
        company_id_normalized = normalize(company_name)
        company_link = f'<a href="/company/{company_id_normalized}/" class="badge bg-success" style="font-size: 1rem; text-decoration: none;">{company_name}</a>'
        company_info = f'<div class="mt-2 mb-2">{company_link}</div>'
    else:
        company_info = f'<div class="mt-2 mb-2"><span class="badge bg-warning">No Company Found</span></div>'

    # Get the database ID (pk) which is now stored in '_id'
    db_id = record.get("_id", None)
    # Get the string component ID (which might be the source _id)
    component_id_str = record.get("component_id_str", "") 
    # Get the actual component ID extracted from additional_data
    actual_component_id = record.get("actual_component_id", "")
    
    # --- FIX: Generate correct link using db_id --- 
    loc_link = loc # Default to plain text if no ID
    if db_id is not None:
        try:
            # Use Django's reverse function if possible, otherwise fallback to hardcoded URL
            detail_url = reverse('component_detail', kwargs={'pk': db_id})
            loc_link = f'<a href="{detail_url}" style="color: blue; text-decoration: underline;">{loc}</a>'
        except Exception as url_error:
            logger.warning(f"Could not reverse URL for component_detail pk={db_id}: {url_error}. Falling back to hardcoded URL.")
            # Fallback to hardcoded URL pattern if reverse fails
            loc_link = f'<a href="/component/{db_id}/" style="color: blue; text-decoration: underline;">{loc}</a>'
    # --- END FIX ---

    # Extract auction type for badge
    auction_type = ""
    auction_badge_class = "bg-secondary"
    if auction and auction != "N/A":
        if "T-1" in auction or "T1" in auction:
            auction_type = "T-1"
            auction_badge_class = "bg-warning"
        elif "T-4" in auction or "T4" in auction:
            auction_type = "T-4"
            auction_badge_class = "bg-info"
        elif "T-3" in auction or "T3" in auction:
            auction_type = "T-3"
            auction_badge_class = "bg-success"
        elif "TR" in auction:
            auction_type = "TR"
            auction_badge_class = "bg-danger"
        else:
            auction_type = auction.split()[0] if " " in auction else auction

    # Format badges with proper ordering and spacing
    badges = []
    
    # Auction badge
    if auction_type:
        badges.append(f'<span class="badge {auction_badge_class} me-1">{auction_type}</span>')
    
    # Delivery year badge
    if delivery_year != "N/A":
        badges.append(f'<span class="badge bg-secondary me-1">Year: {delivery_year}</span>')
    
    # Technology badge
    if tech != "N/A":
        # Safe check for string length - tech is guaranteed to be a string not None at this point
        tech_short = tech[:20] + "..." if len(tech) > 20 else tech
        badges.append(f'<span class="badge bg-primary me-1">{tech_short}</span>')
    
    # Type badge (if different from auction type)
    if typ != "N/A" and typ != auction_type:
        badges.append(f'<span class="badge bg-dark me-1">{typ}</span>')
    
    # Add DB ID badge
    if db_id is not None:
        badges.append(f'<span class="badge bg-secondary me-1 small">DB ID: {str(db_id)}</span>')
    
    # Add actual Component ID badge if it exists
    if actual_component_id:
        badges.append(f'<span class="badge bg-dark me-1 small">Component ID: {actual_component_id}</span>')
    # Fallback: if actual not found, show the one from the model field (if it exists and differs from CMU ID)
    elif component_id_str and component_id_str != cmu_id:
        badges.append(f'<span class="badge bg-dark me-1 small">Comp ID (from source _id?): {component_id_str}</span>')
    
    badges_html = " ".join(badges)
    badges_div = f'<div class="mb-2">{badges_html}</div>' if badges_html else ""

    return f"""
    <div class="component-record">
        <strong>{loc_link}</strong>
        <div class="mt-1 mb-1"><i>{desc}</i></div>
        {badges_div}
        <div class="small text-muted">Full auction: <b>{auction}</b> | CMU ID: {cmu_id}</div>
        {company_info}
    </div>
    """


def format_components_for_display(component, query=None):
    """
    Format a component dictionary into an HTML string for display.
    This mimics the formatting done in search_components_service.
    
    Args:
        component: Dictionary containing component data
        query: Optional search query for highlighting
        
    Returns:
        HTML string formatted for display
    """
    try:
        # Get key attributes (adjust these based on your data structure)
        component_id = component.get('_id', '')
        location = component.get('Location and Post Code', 'No location')
        description = component.get('Description of CMU Components', 'No description')
        
        # Format as HTML with blue links for the location
        html = f'<div class="component-item">'
        
        # If we have a component ID, make the location a link
        if component_id:
            html += f'<a href="/component/{component_id}" class="component-link">{location}</a>'
        else:
            html += f'<span>{location}</span>'
            
        # Add description if available
        if description:
            html += f'<div class="component-description">{description}</div>'
            
        html += '</div>'
        
        return html
    except Exception as e:
        # Fallback for any formatting errors
        import json
        return json.dumps(component)


# Add this function to your services/company_search.py file
def company_detail(request, company_id):
    """
    View function for company detail page.
    Displays all data related to a specific company.
    """
    try:
        # Get CMU dataframe
        cmu_df, df_api_time = get_cmu_dataframe()

        if cmu_df is None:
            return render(request, "checker/company_detail.html", {
                "error": "Error loading CMU data",
                "company_name": None
            })

        # Find company name from company_id
        company_name = None
        for _, row in cmu_df.iterrows():
            if normalize(row.get("Full Name", "")) == company_id:
                company_name = row.get("Full Name")
                break

        if not company_name:
            return render(request, "checker/company_detail.html", {
                "error": f"Company not found: {company_id}",
                "company_name": None
            })

        # Get all records for this company
        company_records = cmu_df[cmu_df["Full Name"] == company_name]

        # Prepare year and auction data
        year_auction_data = []
        grouped = company_records.groupby("Delivery Year")

        for year, group in grouped:
            if year.startswith("Years:"):
                year = year.replace("Years:", "").strip()

            auctions = {}
            auctions_display = []
            if "Auction Name" in group.columns:
                for _, row in group.iterrows():
                    auction_name = row.get("Auction Name", "")
                    if auction_name and auction_name not in auctions:
                        auctions[auction_name] = []

                        # Extract auction type for badge
                        auction_type = ""
                        badge_class = "bg-secondary"
                        if "T-1" in auction_name:
                            auction_type = "T-1"
                            badge_class = "bg-warning"
                        elif "T-4" in auction_name:
                            auction_type = "T-4"
                            badge_class = "bg-info"
                        else:
                            auction_type = auction_name

                        # Create unique auction ID
                        auction_id = f"auction-{normalize(year)}-{normalize(auction_name)}-{company_id}"

                        auctions_display.append((auction_name, auction_id, badge_class, auction_type))
                        auctions[auction_name].append(row.get("CMU ID"))

            if not auctions:
                continue

            year_id = f"year-{normalize(year)}-{company_id}"
            year_auction_data.append({
                'year': year,
                'auctions': auctions,
                'auctions_display': auctions_display,
                'year_id': year_id
            })

        # Sort by year (most recent first by default)
        year_auction_data.sort(key=lambda x: try_parse_year(x['year']), reverse=True)

        return render(request, "checker/company_detail.html", {
            "company_name": company_name,
            "company_id": company_id,
            "year_auction_data": year_auction_data,
            "api_time": df_api_time
        })

    except Exception as e:
        logger.error(f"Error in company_detail: {str(e)}")
        logger.error(traceback.format_exc())
        return render(request, "checker/company_detail.html", {
            "error": f"Error loading company details: {str(e)}",
            "company_name": None,
            "traceback": traceback.format_exc() if request.GET.get("debug") else None
        })