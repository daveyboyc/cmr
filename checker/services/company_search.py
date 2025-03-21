import pandas as pd
import urllib.parse
import logging
import time
import traceback
from django.shortcuts import render
from django.core.cache import cache
from django.http import HttpResponse
from django.template.loader import render_to_string

from ..utils import normalize, get_cache_key, format_location_list, safe_url_param, from_url_param
from .data_access import (
    get_cmu_dataframe,
    fetch_components_for_cmu_id,
    get_component_data_from_json
)

logger = logging.getLogger(__name__)


def search_companies_service(request, extra_context=None, return_data_only=False):
    """Service function for searching companies"""
    results = {}
    error_message = None
    api_time = 0
    query = request.GET.get("q", "").strip()
    sort_order = request.GET.get("sort", "desc")

    if request.method == "GET":
        if "search_results" in request.session and not query:
            results = request.session.pop("search_results")
            record_count = request.session.pop("record_count", None)
            api_time = request.session.pop("api_time", 0)
            last_query = request.session.pop("last_query", "")

            if return_data_only:
                return results
                
            context = {
                "results": results,
                "record_count": record_count,
                "error": error_message,
                "api_time": api_time,
                "query": last_query,
                "sort_order": sort_order,
            }

            if extra_context:
                context.update(extra_context)

            return render(request, "checker/search.html", context)

        elif query:
            norm_query = normalize(query)
            cmu_df, df_api_time = get_cmu_dataframe()
            api_time += df_api_time

            if cmu_df is None:
                if return_data_only:
                    return {}
                    
                context = {
                    "error": "Error fetching CMU data",
                    "api_time": api_time,
                    "query": query,
                    "sort_order": sort_order,
                }

                if extra_context:
                    context.update(extra_context)

                return render(request, "checker/search.html", context)

            record_count = len(cmu_df)
            matching_records = _perform_company_search(cmu_df, norm_query)
            unique_companies = list(matching_records["Full Name"].unique())
            results = _build_search_results(cmu_df, unique_companies, sort_order, query)

            request.session["search_results"] = results
            request.session["record_count"] = record_count
            request.session["api_time"] = api_time
            request.session["last_query"] = query
            
            if return_data_only:
                return results

            context = {
                "results": results,
                "record_count": record_count,
                "error": error_message,
                "api_time": api_time,
                "query": query,
                "sort_order": sort_order,
            }

            if extra_context:
                context.update(extra_context)

            return render(request, "checker/search.html", context)
        else:
            if "search_results" in request.session:
                request.session.pop("search_results", None)
            if "api_time" in request.session:
                request.session.pop("api_time", None)
                
            if return_data_only:
                return {}

            context = {
                "results": {},
                "api_time": api_time,
                "query": query,
                "sort_order": sort_order,
            }

            if extra_context:
                context.update(extra_context)

            return render(request, "checker/search.html", context)
    
    if return_data_only:
        return {}
        
    context = {
        "results": {},
        "api_time": api_time,
        "query": query,
        "sort_order": sort_order,
    }

    if extra_context:
        context.update(extra_context)

    return render(request, "checker/search.html", context)


def _perform_company_search(cmu_df, norm_query):
    """
    Perform search for companies based on normalized query.
    Returns a DataFrame of matching records.
    """
    cmu_id_matches = cmu_df[cmu_df["Normalized CMU ID"].str.contains(norm_query, regex=False, na=False)]
    company_matches = cmu_df[cmu_df["Normalized Full Name"].str.contains(norm_query, regex=False, na=False)]
    matching_records = pd.concat([cmu_id_matches, company_matches]).drop_duplicates(subset=['Full Name'])
    return matching_records

def _build_search_results(cmu_df, unique_companies, sort_order, query):
    """
    Build HTML for search results.
    """
    results = {}
    sentences = []

    for full_name in unique_companies:
        logger.info(f"DEBUG: Processing company: {full_name}")
        records = cmu_df[cmu_df["Full Name"] == full_name]
        company_id = normalize(full_name)
        year_auction_data = _prepare_year_auction_data(records, company_id)

        # Add debugging about year_auction_data
        logger.info(f"DEBUG: Found {len(year_auction_data)} years for company: {full_name}")
        for year_info in year_auction_data:
            logger.info(f"DEBUG: Year {year_info['year']} has {len(year_info['auctions'])} auctions")

        ascending = sort_order == "asc"
        year_auction_data.sort(key=lambda x: try_parse_year(x['year']), reverse=not ascending)
        
        # Format company link correctly - this was creating an HTML card but we need a proper link
        company_url = f"/company/{company_id}/"
        company_html = f'<a href="{company_url}" style="color: blue; text-decoration: underline; margin-right: 10px;">{full_name}</a>'
        logger.info(f"DEBUG: Created company link: {company_url}")
        sentences.append(company_html)

    if sentences:
        results[query] = sentences
        logger.info(f"DEBUG: Added {len(sentences)} company results to query '{query}'")
    else:
        results[query] = [f"No matching record found for '{query}'."]
        logger.info(f"DEBUG: No results found for query '{query}'")

    return results

def _prepare_year_auction_data(records, company_id):
    """
    Prepare data structure for years and auctions.
    """
    year_auction_data = []
    grouped = records.groupby("Delivery Year")

    for year, group in grouped:
        if year.startswith("Years:"):
            year = year.replace("Years:", "").strip()

        auctions = {}
        if "Auction Name" in group.columns:
            for _, row in group.iterrows():
                auction_name = row.get("Auction Name", "")
                if auction_name:
                    if auction_name not in auctions:
                        auctions[auction_name] = []
                    auctions[auction_name].append(row.get("CMU ID"))

        if not auctions:
            continue

        year_id = f"year-{year.replace(' ', '')}-{company_id}"
        year_auction_data.append({
            'year': year,
            'auctions': auctions,
            'year_id': year_id
        })

    return year_auction_data

def _build_company_card_html(company_name, company_id, year_auction_data):
    """
    Build HTML for a company card using template.
    """
    return render_to_string('checker/components/company_card.html', {
        'company_name': company_name,
        'company_id': company_id,
        'year_auction_data': year_auction_data
    })

def get_auction_components(company_id, year, auction_name):
    """
    Get components for a specific auction.
    This function is used by the HTMX endpoint to load auction components lazily.
    """
    logger.info(f"get_auction_components called with company_id={company_id}, year={year}, auction_name={auction_name}")
    
    # Try to get from cache first
    cache_key = f"auction_components_{company_id}_{year}_{auction_name}"
    cached_html = cache.get(cache_key)
    if cached_html:
        logger.info(f"Returning cached auction components for {cache_key}")
        return cached_html
    
    start_time = time.time()
    try:
        # Extract T-1 or T-4 from auction name if possible
        t_number = ""
        auction_upper = auction_name.upper() if auction_name else ""
        if "T-1" in auction_upper or "T1" in auction_upper or "T 1" in auction_upper:
            t_number = "T-1"
        elif "T-4" in auction_upper or "T4" in auction_upper or "T 4" in auction_upper:
            t_number = "T-4"
        
        logger.info(f"Extracted t_number={t_number} from auction name '{auction_name}'")
        
        # Extract year range pattern from auction name if possible
        import re
        year_range_pattern = ""
        if auction_name:
            matches = re.findall(r'\d{4}/\d{2}', auction_name)
            if matches:
                year_range_pattern = matches[0]
            else:
                # Try to extract just a 4-digit year
                matches = re.findall(r'\d{4}', auction_name)
                if matches:
                    year_range_pattern = matches[0]
                    
        logger.info(f"Extracted year_range_pattern={year_range_pattern} from auction name '{auction_name}'")

        # Get all CMU IDs for this company
        cmu_df, _ = get_cmu_dataframe()
        if cmu_df is None:
            logger.error("Failed to get CMU dataframe")
            return "<div class='alert alert-danger'>Error loading CMU data</div>"

        company_name = None
        for _, row in cmu_df.iterrows():
            if normalize(row.get("Full Name", "")) == company_id:
                company_name = row.get("Full Name")
                break

        if not company_name:
            logger.error(f"Company not found: {company_id}")
            return f"<div class='alert alert-danger'>Company not found: {company_id}</div>"

        logger.info(f"Found company name: {company_name}")

        # Get all records for this company
        logger.info(f"Getting records for company: {company_name}")
        company_records = cmu_df[cmu_df["Full Name"] == company_name]
        all_cmu_ids = company_records["CMU ID"].unique().tolist()
        logger.info(f"Found {len(all_cmu_ids)} CMU IDs for company")

        # Collect matching components
        matching_components = []
        logger.info(f"Fetching components for {len(all_cmu_ids)} CMU IDs")
        
        # Only process the first 10 CMU IDs to improve performance
        # This is a tradeoff between completeness and performance
        cmu_ids_to_process = all_cmu_ids[:min(10, len(all_cmu_ids))]
        
        # Show message if we're limiting results
        limited_results = len(all_cmu_ids) > len(cmu_ids_to_process)
        
        for i, cmu_id in enumerate(cmu_ids_to_process):
            try:
                # First try to get components from JSON cache
                components = get_component_data_from_json(cmu_id)
                
                # If not found in JSON, fetch from API
                if not components:
                    components, _ = fetch_components_for_cmu_id(cmu_id)
                
                # Skip if no components found
                if not components:
                    continue
                    
                # Process components
                for comp in components:
                    comp_auction = comp.get("Auction Name", "")
                    comp_auction_upper = comp_auction.upper() if comp_auction else ""

                    # Check for t-number match with more flexible matching
                    t_number_match = not t_number or (
                        (t_number == "T-1" and ("T-1" in comp_auction_upper or "T1" in comp_auction_upper or "T 1" in comp_auction_upper)) or
                        (t_number == "T-4" and ("T-4" in comp_auction_upper or "T4" in comp_auction_upper or "T 4" in comp_auction_upper))
                    )
                    
                    # Check for year match
                    year_match = not year_range_pattern or year_range_pattern in comp_auction
                    
                    logger.info(f"Component auction: '{comp_auction}', t_number_match: {t_number_match}, year_match: {year_match}")

                    # Check if auction name matches both the year range pattern and T-number
                    if year_match and t_number_match:
                        comp = comp.copy()
                        comp["CMU ID"] = cmu_id
                        matching_components.append(comp)
            except Exception as e:
                logger.error(f"Error processing CMU ID {cmu_id}: {str(e)}")
                continue

        logger.info(f"Found {len(matching_components)} matching components")

        if not matching_components:
            logger.info("No components found, showing message")
            return f"""
            <div class='alert alert-info'>
                <h5>No components found for this auction</h5>
                <p>We couldn't find any components matching the criteria:</p>
                <ul>
                    <li>Company: {company_name}</li>
                    <li>Year: {year}</li>
                    <li>Auction: {auction_name}</li>
                </ul>
                <p>Try clicking on a different auction or year.</p>
            </div>
            """

        # Group matching components by auction name
        auctions = {}
        for comp in matching_components:
            auction = comp.get("Auction Name", "No Auction")
            if auction not in auctions:
                auctions[auction] = []
            auctions[auction].append(comp)

        logger.info(f"Grouped components into {len(auctions)} auctions")
        
        # If no auctions found, return a helpful message
        if not auctions:
            logger.info("No auctions found after grouping")
            return f"""
            <div class='alert alert-info'>
                <h5>No components found for this auction</h5>
                <p>We couldn't find any components matching the criteria:</p>
                <ul>
                    <li>Company: {company_name}</li>
                    <li>Year: {year}</li>
                    <li>Auction: {auction_name}</li>
                </ul>
                <p>Try clicking on a different auction or year.</p>
            </div>
            """

        # Format the results
        html = f"""
        <h4>Components for {company_name} - {year_range_pattern or year}xx {t_number}</h4>
        """
        
        # Add a note if we limited the results
        if limited_results:
            html += f"""
            <div class="alert alert-info mb-3">
                <small><strong>Note:</strong> Showing partial results for performance reasons. 
                We found {len(matching_components)} components from {len(cmu_ids_to_process)} of {len(all_cmu_ids)} total CMU IDs.</small>
            </div>
            """

        # Define a safe key function for sorting that handles None values
        def safe_sort_key(item):
            auction_name, _ = item
            return auction_name or ""

        # Use the safe sort function
        for auction_name, components in sorted(auctions.items(), key=safe_sort_key):
            html += f"""
            <div class="auction-group mb-4">
                <h5 class="border-bottom pb-2">{auction_name} ({len(components)} components)</h5>
                <div class="results-list">
            """

            # Sort components by location with a safe key function
            def safe_location_key(component):
                return component.get("Location and Post Code", "") or ""
                
            components.sort(key=safe_location_key)

            for component in components:
                loc = component.get("Location and Post Code", "N/A")
                desc = component.get("Description of CMU Components", "N/A")
                tech = component.get("Generating Technology Class", "N/A")
                auction = component.get("Auction Name", "N/A")
                cmu_id = component.get("CMU ID", "N/A")

                # Create a unique ID for this component
                component_id = f"{cmu_id}_{normalize(loc)}"

                # Create CMU ID badge
                cmu_badge = f'<a href="/components/?q={cmu_id}" class="badge bg-primary" style="font-size: 0.9rem; text-decoration: none;">CMU ID: {cmu_id}</a>'

                # Create the component entry with location as a link
                html += f"""
                <div class="result-item mb-3">
                    <strong><a href="/component/{component_id}/" class="text-primary">{loc}</a></strong>
                    <div class="mt-1 mb-1"><i>{desc}</i></div>
                    <div>Technology: {tech} | <b>{auction}</b> | {cmu_badge}</div>
                </div>
                """

            html += """
                </div>
            </div>
            """

        elapsed_time = time.time() - start_time
        logger.info(f"Successfully completed in {elapsed_time:.2f} seconds")
        
        # Only cache if it took a significant time to generate (more than 0.5 seconds)
        if elapsed_time > 0.5:
            cache.set(cache_key, html, 86400)  # Cache for 24 hours
            
        return html

    except Exception as e:
        logger.error(f"Unexpected error in get_auction_components: {str(e)}")
        logger.error(traceback.format_exc())
        elapsed_time = time.time() - start_time
        logger.info(f"Failed after {elapsed_time:.2f} seconds")
        error_html = f"""
            <div class='alert alert-danger'>
                <h5>Error loading auction components</h5>
                <p>An unexpected error occurred: {str(e)}</p>
                <p>Please try again or contact support if the problem persists.</p>
            </div>
        """
        return error_html

def try_parse_year(year_str):
    """Try to parse a year string to an integer for sorting."""
    try:
        import re
        matches = re.findall(r'\d+', year_str)
        if matches:
            return int(matches[0])
        return 0
    except:
        return 0

def get_company_years(company_id, year, auction_name=None):
    """
    Get year details for a company.
    This function is used by the HTMX endpoint to load year details lazily.
    """
    cmu_df, _ = get_cmu_dataframe()

    if cmu_df is None:
        return "<div class='alert alert-danger'>Error loading CMU data</div>"

    company_name = None
    for _, row in cmu_df.iterrows():
        if normalize(row.get("Full Name", "")) == company_id:
            company_name = row.get("Full Name")
            break

    if not company_name:
        return f"<div class='alert alert-warning'>Company not found: {company_id}</div>"

    company_records = cmu_df[cmu_df["Full Name"] == company_name]
    year_records = company_records[company_records["Delivery Year"] == year]

    if year_records.empty:
        return f"<div class='alert alert-info'>No CMUs found for {company_name} in {year}</div>"

    if auction_name and "Auction Name" in year_records.columns:
        year_records = year_records[year_records["Auction Name"] == auction_name]
        if year_records.empty:
            return f"<div class='alert alert-info'>No CMUs found for {company_name} in {year} with auction {auction_name}</div>"

    cmu_ids = year_records["CMU ID"].unique().tolist()
    debug_info = f"Found {len(cmu_ids)} CMU IDs for {company_name} in {year}"
    if auction_name:
        debug_info += f" (Auction: {auction_name})"
    debug_info += f": {', '.join(cmu_ids)}"
    print(debug_info)

    html = f"<div class='small text-muted mb-2'>{debug_info}</div><div class='row'>"

    for cmu_id in cmu_ids:
        components, _ = fetch_components_for_cmu_id(cmu_id)
        component_debug = f"Found {len(components)} components for CMU ID {cmu_id}"
        print(component_debug)

        filtered_components = _filter_components_by_year_auction(components, year, auction_name)
        if not filtered_components:
            component_debug += f" (filtered to 0 for {year}"
            if auction_name:
                component_debug += f", {auction_name}"
            component_debug += ")"
            continue

        html += _build_cmu_card_html(cmu_id, filtered_components, component_debug)

    html += "</div>"
    return html

def _filter_components_by_year_auction(components, year, auction_name=None):
    """
    Filter components by year and auction name.
    """
    filtered_components = []
    for comp in components:
        comp_delivery_year = comp.get("Delivery Year", "")
        comp_auction = comp.get("Auction Name", "")

        if comp_delivery_year != year:
            continue
        if auction_name and comp_auction != auction_name:
            continue
        filtered_components.append(comp)

    return filtered_components

def _build_cmu_card_html(cmu_id, components, component_debug):
    """
    Build HTML for a CMU card using template.
    """
    # Get auction info for this CMU ID
    auction_year_info = {}
    for comp in components:
        auction = comp.get("Auction Name", "")
        if auction:
            # Extract auction year and type
            parts = auction.split()
            auction_type = parts[0] if len(parts) >= 1 else ""
            auction_year = parts[1] if len(parts) >= 2 else ""
            key = f"{auction_type} {auction_year}"
            auction_year_info[key] = True

    # Create a comma-separated string of auction info
    auction_info = ", ".join(auction_year_info.keys())

    # Get unique locations
    locations = set()
    for comp in components:
        location = comp.get("Location and Post Code", "")
        if location:
            locations.add(location)
    
    # Get company name if available
    company_name = None
    if components and "Company Name" in components[0]:
        company_name = components[0]["Company Name"]
        
    # Get location HTML
    location_html = format_location_list(locations, components)
    
    # Render template
    return render_to_string('checker/components/cmu_card.html', {
        'cmu_id': cmu_id,
        'components': components,
        'component_debug': component_debug,
        'auction_info': auction_info,
        'location_html': location_html,
        'company_name': company_name
    })

def get_cmu_details(cmu_id):
    """
    HTMX endpoint to get CMU details.
    """
    components, _ = fetch_components_for_cmu_id(cmu_id)

    if not components:
        return f"<div>No components found for CMU ID {cmu_id}</div>"

    locations = set()
    for component in components:
        location = component.get("Location and Post Code", "")
        if location:
            locations.add(location)

    html = f"""
    <div id="cmu-content-{cmu_id}">
        <p><strong>Components:</strong> {len(components)}</p>
        {format_location_list(locations, components)}
    </div>
    """
    return html


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
                    cmu_id = row.get("CMU ID", "")
                    
                    # Only include auctions with components
                    components = get_component_data_from_json(cmu_id)
                    has_components = components and len(components) > 0
                    
                    if auction_name and auction_name not in auctions and has_components:
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
                        auctions[auction_name].append(cmu_id)

            if not auctions:
                continue

            year_id = f"year-{normalize(year)}-{company_id}"
            year_auction_data.append({
                'year': year,
                'auctions': auctions,
                'auctions_display': auctions_display,
                'year_id': year_id
            })

        # Get sort order from request, default to desc (newest first)
        sort_order = request.GET.get("sort", "desc")

        # Sort by year based on sort order
        year_auction_data.sort(
            key=lambda x: try_parse_year(x['year']),
            reverse=(sort_order == "desc")
        )

        return render(request, "checker/company_detail.html", {
            "company_name": company_name,
            "company_id": company_id,
            "year_auction_data": year_auction_data,
            "api_time": df_api_time,
            "sort_order": sort_order
        })

    except Exception as e:
        logger.error(f"Error in company_detail: {str(e)}")
        logger.error(traceback.format_exc())
        return render(request, "checker/company_detail.html", {
            "error": f"Error loading company details: {str(e)}",
            "company_name": None,
            "traceback": traceback.format_exc() if request.GET.get("debug") else None
        })