import urllib.parse
import time
import logging
from django.shortcuts import render
from django.core.cache import cache
import traceback

from ..utils import normalize
from .data_access import fetch_components_for_cmu_id, get_component_data_from_json
from .company_search import _perform_company_search, get_cmu_dataframe

logger = logging.getLogger(__name__)


def search_components_service(request, return_data_only=False):
    """Service function for searching components and companies in a unified interface"""
    # Add pagination parameters
    page = int(request.GET.get("page", 1))
    per_page = int(request.GET.get("per_page", 500))  # Default to 500 items per page
    sort_order = request.GET.get("comp_sort", "desc")  # Sort for component results

    results = {}
    company_links = []
    error_message = None
    api_time = 0
    query = request.GET.get("q", "").strip()

    # Add debug flag to force company results to appear even if no search query
    debug_company = request.GET.get("debug_company", "false") == "true"

    # Debug info to collect
    debug_info = {
        "query": query,
        "company_search_attempted": False,
        "cmu_df_loaded": False,
        "cmu_df_rows": 0,
        "matching_records": 0,
        "unique_companies": [],
        "error": None
    }

    if request.method == "GET":
        if "search_results" in request.session and not query:
            # Only use session results if no new query is provided
            results = request.session.pop("search_results")
            company_links = request.session.pop("company_links", [])
            record_count = request.session.pop("record_count", None)
            api_time = request.session.pop("api_time", 0)
            last_query = request.session.pop("last_query", "")

            if debug_company:
                # Force some company links for testing
                company_links.append(
                    '<a href="/?q=Test%20Company%201" style="color: blue; text-decoration: underline;">Test Company 1</a>')
                company_links.append(
                    '<a href="/?q=Test%20Company%202" style="color: blue; text-decoration: underline;">Test Company 2</a>')
                debug_info["forced_test_links"] = True
                
            if return_data_only:
                return results

            return render(request, "checker/search_components.html", {
                "results": results,
                "company_links": company_links,
                "record_count": record_count,
                "component_count": record_count,
                "displayed_component_count": record_count,
                "error": error_message,
                "api_time": api_time,
                "query": last_query,
                "sort_order": sort_order,
                "debug_info": debug_info,
                # Pagination context
                "page": page,
                "total_pages": 1 if record_count == 0 else (record_count + per_page - 1) // per_page,
                "has_prev": False,
                "has_next": False,
                "page_range": range(1, 2),
            })
        elif query or debug_company:
            start_time = time.time()

            # STEP 1: Search for matching companies
            try:
                debug_info["company_search_attempted"] = True
                # Use the company search functionality to find matching companies
                cmu_df, df_api_time = get_cmu_dataframe()
                api_time += df_api_time

                if cmu_df is not None:
                    debug_info["cmu_df_loaded"] = True
                    debug_info["cmu_df_rows"] = len(cmu_df)

                    if query:
                        norm_query = normalize(query)
                        matching_records = _perform_company_search(cmu_df, norm_query)
                        unique_companies = list(matching_records["Full Name"].unique())

                        debug_info["matching_records"] = len(matching_records)
                        debug_info["unique_companies"] = unique_companies

                        # Filter out companies with no components
                        companies_with_components = []
                        for company in unique_companies:
                            # Get all CMU IDs for this company
                            company_records = cmu_df[cmu_df["Full Name"] == company]
                            cmu_ids = company_records["CMU ID"].unique().tolist()
                            
                            # Check if any of these CMU IDs have components
                            has_components = False
                            for cmu_id in cmu_ids:
                                components = get_component_data_from_json(cmu_id)
                                if components and len(components) > 0:
                                    has_components = True
                                    break
                            
                            if has_components:
                                companies_with_components.append(company)
                        
                        # Log the filtering results
                        logger.info(f"DEBUG: Found {len(unique_companies)} companies, {len(companies_with_components)} have components")
                        debug_info["companies_filtered_out"] = len(unique_companies) - len(companies_with_components)
                        
                        # Use only companies that have components
                        unique_companies = companies_with_components

                        # Create blue links for each company - TEMPORARILY point to company search page
                        company_links = [
                            f'<a href="/?q={urllib.parse.quote(company)}" style="color: blue; text-decoration: underline;">{company}</a>'
                            for company in unique_companies
                        ]

                    # Alternative approach: show a random sample of companies for testing
                    if debug_company and not company_links:
                        sample_companies = cmu_df["Full Name"].sample(min(5, len(cmu_df))).tolist()
                        debug_info["sample_companies"] = sample_companies
                        company_links = [
                            f'<a href="/?q={urllib.parse.quote(company)}" style="color: blue; text-decoration: underline;">{company}</a>'
                            for company in sample_companies
                        ]
                else:
                    debug_info["error"] = "CMU dataframe is None"
                    company_links = []
            except Exception as e:
                error_msg = f"Error searching companies: {str(e)}"
                logger.error(error_msg)
                debug_info["error"] = error_msg
                company_links = []

            # Always force a test company link to appear (for debugging)
            if debug_company:
                company_links.append(
                    '<a href="/?q=TestCompany" style="color: blue; text-decoration: underline;">Test Company (Forced)</a>')
                debug_info["forced_test_link"] = True

            # STEP 2: Get mapping of CMU IDs to company names from cache
            cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
            debug_info["cmu_mapping_entries"] = len(cmu_to_company_mapping)

            # STEP 3: Search for matching components
            components = []
            total_component_count = 0
            components_api_time = 0

            try:
                if query:  # Only search for components if there's a query
                    # First try to get components directly from JSON
                    logger.info(f"DEBUG: Searching for components with query: {query}")
                    components = get_component_data_from_json(query) or []
                    logger.info(f"DEBUG: Found {len(components)} components in JSON")
                    debug_info["json_components_found"] = len(components)

                    # If not found, try to fetch components by CMU ID
                    if not components:
                        logger.info(f"DEBUG: No components found in JSON, trying API with CMU ID: {query}")
                        api_components, components_metadata = fetch_components_for_cmu_id(query, page=page, per_page=per_page)
                        components = api_components or []
                        
                        # Get accurate total count from metadata
                        if isinstance(components_metadata, dict):
                            total_component_count = components_metadata.get("total_count", len(components))
                            components_api_time = components_metadata.get("processing_time", 0)
                            api_time += components_api_time
                        else:
                            total_component_count = len(components)
                            
                        logger.info(f"DEBUG: Found {len(components)} components via API (total: {total_component_count})")
                        debug_info["api_components_found"] = len(components)
                    else:
                        # If we found components in JSON, use that count
                        total_component_count = len(components)
            except Exception as e:
                import traceback
                logger.error(f"DEBUG ERROR in component search: {str(e)}")
                logger.error(traceback.format_exc())
                error_msg = f"Error fetching component data: {str(e)}"
                logger.error(error_msg)
                error_message = error_msg
                debug_info["component_error"] = error_msg
                components = []
                total_component_count = 0

            # Set the displayed count based on how many we're showing on this page
            displayed_component_count = len(components)

            # Calculate pagination values if not already provided by the API
            if total_component_count > 0:
                total_pages = (total_component_count + per_page - 1) // per_page
                has_prev = page > 1
                has_next = page < total_pages
                page_range = range(max(1, page - 2), min(total_pages + 1, page + 3))
            else:
                total_pages = 1
                has_prev = False
                has_next = False
                page_range = range(1, 2)

            # Add log to verify correct counts
            logger.info(f"DEBUG: Displaying {displayed_component_count} of {total_component_count} total components (Page {page} of {total_pages})")

            record_count = total_component_count
            logger.info(f"DEBUG: Final component count: {record_count}")

            # Format component results for display
            sentences = []
            
            # Sort components based on comp_sort parameter
            if components:
                try:
                    logger.info(f"DEBUG: Sorting components by Delivery Year, reverse={sort_order == 'desc'}")
                    
                    # Define a safe sorting key function that handles None values
                    def safe_delivery_year_key(component):
                        return component.get("Delivery Year", "") or ""
                    
                    components.sort(
                        key=safe_delivery_year_key,
                        reverse=(sort_order == "desc")
                    )
                except Exception as sort_error:
                    logger.error(f"Error sorting components: {str(sort_error)}")
                    debug_info["sort_error"] = str(sort_error)

            # Format each component record
            results = {}
            for component in components:
                formatted_record = format_component_record(component, cmu_to_company_mapping)
                sentences.append(formatted_record)
                
            if sentences:
                results[query] = sentences
                logger.info(f"DEBUG: Added {len(sentences)} formatted component records to results[{query}]")
            
            elapsed_time = time.time() - start_time
            api_time += elapsed_time

            request.session["search_results"] = results
            request.session["company_links"] = company_links
            request.session["record_count"] = record_count
            request.session["api_time"] = api_time
            request.session["last_query"] = query
            
            if return_data_only:
                return results

            return render(request, "checker/search_components.html", {
                "results": results,
                "company_links": company_links,
                "record_count": record_count,
                "component_count": total_component_count,
                "displayed_component_count": displayed_component_count,
                "error": error_message,
                "api_time": api_time,
                "query": query,
                "sort_order": sort_order,
                "debug_info": debug_info,
                # Pagination context
                "page": page,
                "total_pages": total_pages,
                "has_prev": has_prev,
                "has_next": has_next,
                "page_range": page_range,
            })
        else:
            # Clear session but keep query parameter in case it's needed
            if "search_results" in request.session:
                request.session.pop("search_results", None)
            if "company_links" in request.session:
                request.session.pop("company_links", None)
            if "api_time" in request.session:
                request.session.pop("api_time", None)
                
            if return_data_only:
                return {}

            return render(request, "checker/search_components.html", {
                "results": {},
                "company_links": [],
                "api_time": api_time,
                "query": query,
            })
    
    if return_data_only:
        return {}

    return render(request, "checker/search_components.html", {
        "results": {},
        "company_links": [],
        "api_time": api_time,
        "query": query,
    })


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
            json_components = get_component_data_from_json(cmu_id)
            if json_components:
                for comp in json_components:
                    if "Company Name" in comp and comp["Company Name"]:
                        company_name = comp["Company Name"]
                        cmu_to_company_mapping[cmu_id] = company_name
                        cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 3600)
                        break
        except Exception as e:
            logger.error(f"Error getting company name from JSON: {e}")

    # Create company badge
    company_info = ""
    if company_name:
        company_id = normalize(company_name)
        company_link = f'<a href="/company/{company_id}/" class="badge bg-success" style="font-size: 1rem; text-decoration: none;">{company_name}</a>'
        company_info = f'<div class="mt-2 mb-2">{company_link}</div>'
    else:
        company_info = f'<div class="mt-2 mb-2"><span class="badge bg-warning">No Company Found</span></div>'

    # Create blue link for location pointing to component detail page
    # FIX: Properly handle locations that contain slashes or special characters
    normalized_loc = normalize(loc).replace('/', '_')  # Replace slashes with underscores
    component_id = f"{cmu_id}_{normalized_loc}"
    encoded_component_id = urllib.parse.quote(component_id)
    loc_link = f'<a href="/component/{encoded_component_id}/" style="color: blue; text-decoration: underline;">{loc}</a>'

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
    
    component_id = record.get("_id", "")
    if component_id:
        badges.append(f'<span class="badge bg-dark me-1 small">ID: {component_id[:12]}</span>')
    
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