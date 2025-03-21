import pandas as pd
import urllib.parse
import logging
import time
import traceback
from django.shortcuts import render
from django.core.cache import cache
from django.http import HttpResponse
from django.template.loader import render_to_string

from ..utils import normalize, get_cache_key, format_location_list
from .data_access import (
    get_cmu_dataframe,
    fetch_components_for_cmu_id,
    get_component_data_from_json
)

logger = logging.getLogger(__name__)


def search_companies_service(request):
    """Service function for searching companies"""
    results = {}
    error_message = None
    api_time = 0
    query = request.GET.get("q", "").strip()  # Get query early to use it throughout
    sort_order = request.GET.get("sort", "desc")  # Get sort order, default to desc (newest first)

    if request.method == "GET":
        if "search_results" in request.session and not query:
            # Only use session results if no new query is provided
            results = request.session.pop("search_results")
            record_count = request.session.pop("record_count", None)
            api_time = request.session.pop("api_time", 0)
            last_query = request.session.pop("last_query", "")
            return render(request, "checker/search.html", {
                "results": results,
                "record_count": record_count,
                "error": error_message,
                "api_time": api_time,
                "query": last_query,  # Pass the last query back to the template
                "sort_order": sort_order,  # Pass sort order to template
            })
        elif query:
            norm_query = normalize(query)

            # Get CMU DataFrame
            cmu_df, df_api_time = get_cmu_dataframe()
            api_time += df_api_time

            if cmu_df is None:
                error_message = "Error fetching CMU data"
                return render(request, "checker/search.html", {
                    "error": error_message,
                    "api_time": api_time,
                    "query": query,  # Keep the query in the search box
                    "sort_order": sort_order,
                })

            record_count = len(cmu_df)

            # Perform search using optimized query approach
            matching_records = _perform_company_search(cmu_df, norm_query)

            # Get unique company names
            unique_companies = list(matching_records["Full Name"].unique())

            # Get results HTML
            results = _build_search_results(cmu_df, unique_companies, sort_order, query)

            # Store in session but also render directly (no redirect)
            request.session["search_results"] = results
            request.session["record_count"] = record_count
            request.session["api_time"] = api_time
            request.session["last_query"] = query  # Store the query for later

            # Render directly instead of redirecting
            return render(request, "checker/search.html", {
                "results": results,
                "record_count": record_count,
                "error": error_message,
                "api_time": api_time,
                "query": query,  # Keep the query in the search box
                "sort_order": sort_order,  # Pass sort order to template
            })
        else:
            # Clear session but keep query parameter in case it's needed
            if "search_results" in request.session:
                request.session.pop("search_results", None)
            if "api_time" in request.session:
                request.session.pop("api_time", None)

            return render(request, "checker/search.html", {
                "results": {},
                "api_time": api_time,
                "query": query,  # Keep any query in the search box
                "sort_order": sort_order,
            })

    return render(request, "checker/search.html", {
        "results": {},
        "api_time": api_time,
        "query": query,  # Keep any query in the search box
        "sort_order": sort_order,
    })


def _perform_company_search(cmu_df, norm_query):
    """
    Perform search for companies based on normalized query.
    Returns a DataFrame of matching records.
    """
    # Try direct CMU ID match first (case-insensitive)
    cmu_id_matches = cmu_df[cmu_df["Normalized CMU ID"].str.contains(norm_query, regex=False, na=False)]

    # Then look for company name matches
    company_matches = cmu_df[cmu_df["Normalized Full Name"].str.contains(norm_query, regex=False, na=False)]

    # Combine the matches
    matching_records = pd.concat([cmu_id_matches, company_matches])

    # Drop duplicates based on Full Name - only keep one record per company
    matching_records = matching_records.drop_duplicates(subset=['Full Name'])

    return matching_records


def _build_search_results(cmu_df, unique_companies, sort_order, query):
    """
    Build HTML for search results.

    Args:
        cmu_df: DataFrame containing CMU data
        unique_companies: List of unique company names to build cards for
        sort_order: Sort order for years (asc or desc)
        query: Search query used

    Returns:
        Dictionary with query as key and list of company HTML as values
    """
    results = {}
    sentences = []

    for full_name in unique_companies:
        print(f"Processing company: {full_name}")
        records = cmu_df[cmu_df["Full Name"] == full_name]

        # Create a unique ID for the company
        company_id = full_name.replace(' ', '').lower()

        # Group records by delivery year and prepare data
        year_auction_data = _prepare_year_auction_data(records, company_id)

        # Sort years based on sort_order
        ascending = sort_order == "asc"
        year_auction_data.sort(key=lambda x: try_parse_year(x['year']), reverse=not ascending)

        # Build company card HTML
        company_html = _build_company_card_html(full_name, company_id, year_auction_data)
        sentences.append(company_html)

    if sentences:
        results[query] = sentences
    else:
        results[query] = [f"No matching record found for '{query}'."]

    return results


def _prepare_year_auction_data(records, company_id):
    """
    Prepare data structure for years and auctions.

    Args:
        records: DataFrame containing records for a specific company
        company_id: Unique ID for the company

    Returns:
        List of dictionaries with year and auction information
    """
    year_auction_data = []
    grouped = records.groupby("Delivery Year")

    for year, group in grouped:
        if year.startswith("Years:"):
            year = year.replace("Years:", "").strip()

        # Get auction info for this year
        auctions = {}
        if "Auction Name" in group.columns:
            for _, row in group.iterrows():
                auction_name = row.get("Auction Name", "")
                if auction_name:
                    if auction_name not in auctions:
                        auctions[auction_name] = []
                    auctions[auction_name].append(row.get("CMU ID"))

        # Skip if no auctions found
        if not auctions:
            continue

        # Create a unique ID for this year's collapsible section
        year_id = f"year-{year.replace(' ', '')}-{company_id}"

        year_auction_data.append({
            'year': year,
            'auctions': auctions,
            'year_id': year_id
        })

    return year_auction_data


def _build_company_card_html(company_name, company_id, year_auction_data):
    """
    Build HTML for a company card.

    Args:
        company_name: Name of the company
        company_id: Unique ID for the company
        year_auction_data: List of dictionaries with year and auction information

    Returns:
        HTML string for the company card
    """
    # Start company card
    company_html = f"""
    <div class="company-card">
        <h4 class="mb-3">{company_name}</h4>
        <div class="years-container">
    """

    # Add years
    for year_info in year_auction_data:
        year = year_info['year']
        auctions = year_info['auctions']
        year_id = year_info['year_id']

        # Create expandable year section
        company_html += _build_year_section_html(year, year_id, auctions, company_id)

    # Close company card
    company_html += """
        </div>
    </div>
    """

    return company_html


def _build_year_section_html(year, year_id, auctions, company_id):
    """
    Build HTML for a year section.

    Args:
        year: Year string
        year_id: Unique ID for the year section
        auctions: Dictionary of auctions mapped to CMU IDs
        company_id: Unique ID for the company

    Returns:
        HTML string for the year section
    """
    year_html = f"""
    <div class="card year-card">
        <div class="card-header" id="heading-{year_id}">
            <h5 class="mb-0">
                <button class="year-button" 
                        data-bs-toggle="collapse" 
                        data-bs-target="#collapse-{year_id}" 
                        aria-expanded="false" 
                        aria-controls="collapse-{year_id}">
                    <strong>Delivery Year: {year}</strong>
                </button>
            </h5>
        </div>
        <div id="collapse-{year_id}" class="collapse" aria-labelledby="heading-{year_id}">
            <div class="card-body">
                <div class="accordion" id="auction-accordion-{year_id}">
    """

    # Add auctions
    for idx, (auction_name, cmu_ids) in enumerate(auctions.items()):
        auction_id = f"auction-{year_id}-{idx}"
        year_html += _build_auction_section_html(auction_name, auction_id, year_id, company_id, year)

    # Close year section
    year_html += """
                </div>
            </div>
        </div>
    </div>
    """

    return year_html


def _build_auction_section_html(auction_name, auction_id, year_id, company_id, year):
    """
    Build HTML for an auction section with correctly matched spinner ID.
    """
    # Extract auction type (T-1, T-4, etc.)
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

    # IMPORTANT FIX: The spinner ID must be "spinner-auction-{auction_id}" to match what the JavaScript expects
    auction_html = f"""
    <div class="accordion-item">
        <h2 class="accordion-header" id="heading-{auction_id}">
            <button class="accordion-button collapsed" type="button" 
                    data-bs-toggle="collapse" 
                    data-bs-target="#collapse-{auction_id}" 
                    aria-expanded="false" 
                    aria-controls="collapse-{auction_id}">
                <span class="badge {badge_class} me-2">{auction_type}</span>
                Auction: {auction_name}
            </button>
        </h2>
        <div id="collapse-{auction_id}" class="accordion-collapse collapse" 
             aria-labelledby="heading-{auction_id}" 
             data-bs-parent="#auction-accordion-{year_id}">
            <div class="accordion-body">
                <!-- FIXED SPINNER ID: Changed from spinner-{auction_id} to spinner-auction-{auction_id} -->
                <div class="spinner text-primary" role="status" id="spinner-auction-{auction_id}">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <div id="auction-content-{auction_id}" class="auction-content"
                     hx-get="/api/auction-components/{company_id}/{year.replace(' ', '_')}/{auction_name.replace(' ', '_')}/"
                     hx-trigger="revealed"
                     hx-swap="innerHTML">
                    <!-- Content will be loaded via HTMX -->
                    <div class="text-muted mb-3">Loading components for {auction_name}...</div>
                </div>
            </div>
        </div>
    </div>
    """

    return auction_html


def get_auction_components(company_id, year, auction_name):
    """
    Find components by matching specific auction name patterns.
    With guaranteed spinner hiding.
    """
    start_time = time.time()
    logger.info(
        f"Starting get_auction_components for company_id={company_id}, year={year}, auction_name={auction_name}")

    try:
        # Get CMU dataframe to find CMU IDs for this company
        logger.info("Fetching CMU dataframe")
        cmu_df, _ = get_cmu_dataframe()

        if cmu_df is None:
            logger.error("Error loading CMU data")
            return """
            <div class='alert alert-danger'>Error loading CMU data</div>
            <style>.spinner { display: none !important; }</style>
            """

        # Convert company_id back to company name
        logger.info(f"Converting company_id {company_id} to company name")
        company_name = None
        for _, row in cmu_df.iterrows():
            if normalize(row.get("Full Name", "")) == company_id:
                company_name = row.get("Full Name")
                break

        if not company_name:
            logger.warning(f"Company not found: {company_id}")
            return f"""
            <div class='alert alert-warning'>Company not found: {company_id}</div>
            <style>.spinner {{ display: none !important; }}</style>
            """

        logger.info(f"Found company name: {company_name}")

        # Get all records for this company
        logger.info(f"Getting records for company: {company_name}")
        company_records = cmu_df[cmu_df["Full Name"] == company_name]
        all_cmu_ids = company_records["CMU ID"].unique().tolist()
        logger.info(f"Found {len(all_cmu_ids)} CMU IDs for company")

        # Determine the expected year range pattern and T-number
        year_range_pattern = f"{year}-"  # E.g. "2028-" for 2028-29

        t_number = ""
        if "(T-1)" in auction_name:
            t_number = "(T-1)"
        elif "(T-3)" in auction_name:
            t_number = "(T-3)"
        elif "(T-4)" in auction_name:
            t_number = "(T-4)"
        elif "T-1" in auction_name:
            t_number = "T-1"
        elif "T-3" in auction_name:
            t_number = "T-3"
        elif "T-4" in auction_name:
            t_number = "T-4"

        logger.info(f"Searching for pattern: {year_range_pattern} and T-number: {t_number}")

        # Collect matching components
        matching_components = []
        logger.info(f"Fetching components for {len(all_cmu_ids)} CMU IDs")

        for i, cmu_id in enumerate(all_cmu_ids):
            try:
                # Fetch components for this CMU
                components, _ = fetch_components_for_cmu_id(cmu_id)

                for comp in components:
                    comp_auction = comp.get("Auction Name", "")

                    # Check if auction name matches both the year range pattern and T-number
                    if year_range_pattern in comp_auction and t_number in comp_auction:
                        comp = comp.copy()
                        comp["CMU ID"] = cmu_id
                        matching_components.append(comp)
            except Exception as e:
                logger.error(f"Error processing CMU ID {cmu_id}: {str(e)}")
                continue

        logger.info(f"Found {len(matching_components)} matching components with strict criteria")

        if not matching_components:
            logger.info("No strict matches, trying broader search")
            # Try a broader search with just the year
            for i, cmu_id in enumerate(all_cmu_ids):
                try:
                    components, _ = fetch_components_for_cmu_id(cmu_id)
                    for comp in components:
                        comp_auction = comp.get("Auction Name", "")
                        if year_range_pattern in comp_auction:
                            comp = comp.copy()
                            comp["CMU ID"] = cmu_id
                            matching_components.append(comp)
                except Exception as e:
                    logger.error(f"Error in broader search for CMU ID {cmu_id}: {str(e)}")
                    continue

            logger.info(f"Found {len(matching_components)} matching components with broader criteria")

            # If still no matches, show sample of available auctions
            if not matching_components:
                logger.info("Still no matches, showing sample of available auctions")
                # Collect sample of auction names
                sample_auctions = set()
                for cmu_id in all_cmu_ids[:10]:  # Check first 10 CMUs only
                    try:
                        components, _ = fetch_components_for_cmu_id(cmu_id)
                        for comp in components:
                            sample_auctions.add(comp.get("Auction Name", ""))
                    except Exception as e:
                        logger.error(f"Error collecting sample auctions for CMU ID {cmu_id}: {str(e)}")
                        continue

                # Create a sorted list of sample auctions
                sorted_auctions = sorted(list(sample_auctions))
                logger.info(f"Collected {len(sorted_auctions)} sample auctions")

                html_response = f"""
                <style>.spinner {{ display: none !important; }}</style>
                <div class='alert alert-warning'>
                    <h5>No components found matching '{year_range_pattern}' and '{t_number}'</h5>
                    <p>Sample of available auction names in your database:</p>
                    <ul>
                        {"".join(f"<li>{auction}</li>" for auction in sorted_auctions[:15])}
                        {f"<li>...and {len(sorted_auctions) - 15} more</li>" if len(sorted_auctions) > 15 else ""}
                    </ul>
                </div>
                """
                elapsed_time = time.time() - start_time
                logger.info(f"Completed in {elapsed_time:.2f} seconds with no matches response")
                return html_response

        # Group matching components by auction name
        auctions = {}
        for comp in matching_components:
            auction = comp.get("Auction Name", "No Auction")
            if auction not in auctions:
                auctions[auction] = []
            auctions[auction].append(comp)

        logger.info(f"Grouped components into {len(auctions)} auctions")

        # Format the results with inline style to hide spinner
        html = f"""
        <style>.spinner {{ display: none !important; }}</style>
        <h4>Components for {company_name} - {year_range_pattern}xx {t_number}</h4>
        """

        for auction_name, components in sorted(auctions.items()):
            html += f"""
            <div class="auction-group mb-4">
                <h5 class="border-bottom pb-2">{auction_name} ({len(components)} components)</h5>
                <div class="results-list">
            """

            # Sort components by location
            components.sort(key=lambda c: c.get("Location and Post Code", ""))

            for component in components:
                loc = component.get("Location and Post Code", "N/A")
                desc = component.get("Description of CMU Components", "N/A")
                tech = component.get("Generating Technology Class", "N/A")
                auction = component.get("Auction Name", "N/A")
                cmu_id = component.get("CMU ID", "N/A")

                # Create CMU ID badge
                cmu_badge = f'<a href="/components/?q={cmu_id}" class="badge bg-primary" style="font-size: 0.9rem; text-decoration: none;">CMU ID: {cmu_id}</a>'

                # Create the component entry with auction name instead of delivery year
                html += f"""
                <div class="result-item mb-3">
                    <strong>{loc}</strong>
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
        return html

    except Exception as e:
        logger.error(f"Unexpected error in get_auction_components: {str(e)}")
        logger.error(traceback.format_exc())
        elapsed_time = time.time() - start_time
        logger.info(f"Failed after {elapsed_time:.2f} seconds")
        return f"""
            <style>.spinner {{ display: none !important; }}</style>
            <div class='alert alert-danger'>
                <h5>Error loading auction components</h5>
                <p>An unexpected error occurred: {str(e)}</p>
                <p>Please try again or contact support if the problem persists.</p>
            </div>
        """


def try_parse_year(year_str):
    """Try to parse a year string to an integer for sorting."""
    try:
        # Extract the first number from the string
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

    Args:
        company_id: The ID of the company (normalized company name)
        year: The delivery year to get details for
        auction_name: Optional auction name to filter by
    """
    # Get CMU dataframe to find CMU IDs for this company and year
    cmu_df, _ = get_cmu_dataframe()

    if cmu_df is None:
        return "<div class='alert alert-danger'>Error loading CMU data</div>"

    # Convert company_id back to company name
    company_name = None
    for _, row in cmu_df.iterrows():
        if normalize(row.get("Full Name", "")) == company_id:
            company_name = row.get("Full Name")
            break

    if not company_name:
        return f"<div class='alert alert-warning'>Company not found: {company_id}</div>"

    # Get records for this company and year
    company_records = cmu_df[cmu_df["Full Name"] == company_name]
    year_records = company_records[company_records["Delivery Year"] == year]

    if year_records.empty:
        return f"<div class='alert alert-info'>No CMUs found for {company_name} in {year}</div>"

    # Filter by auction name if provided
    if auction_name and "Auction Name" in year_records.columns:
        year_records = year_records[year_records["Auction Name"] == auction_name]
        if year_records.empty:
            return f"<div class='alert alert-info'>No CMUs found for {company_name} in {year} with auction {auction_name}</div>"

    # Get CMU IDs for this year and auction
    cmu_ids = year_records["CMU ID"].unique().tolist()

    # Debug info
    debug_info = f"Found {len(cmu_ids)} CMU IDs for {company_name} in {year}"
    if auction_name:
        debug_info += f" (Auction: {auction_name})"
    debug_info += f": {', '.join(cmu_ids)}"
    print(debug_info)

    # Generate HTML for each CMU
    html = f"<div class='small text-muted mb-2'>{debug_info}</div><div class='row'>"

    for cmu_id in cmu_ids:
        # Fetch components for this CMU
        components, _ = fetch_components_for_cmu_id(cmu_id)

        component_debug = f"Found {len(components)} components for CMU ID {cmu_id}"
        print(component_debug)

        # Filter components by year and auction if needed
        filtered_components = _filter_components_by_year_auction(components, year, auction_name)

        if not filtered_components:
            component_debug += f" (filtered to 0 for {year}"
            if auction_name:
                component_debug += f", {auction_name}"
            component_debug += ")"
            continue  # Skip this CMU if no matching components

        # Generate CMU card HTML
        html += _build_cmu_card_html(cmu_id, filtered_components, component_debug)

    html += "</div>"
    return html


def _filter_components_by_year_auction(components, year, auction_name=None):
    """
    Filter components by year and auction name.

    Args:
        components: List of component dictionaries
        year: Year to filter by
        auction_name: Optional auction name to filter by

    Returns:
        List of filtered components
    """
    filtered_components = []
    for comp in components:
        comp_delivery_year = comp.get("Delivery Year", "")
        comp_auction = comp.get("Auction Name", "")

        # Match delivery year
        if comp_delivery_year != year:
            continue

        # If auction name specified, match that too
        if auction_name and comp_auction != auction_name:
            continue

        filtered_components.append(comp)

    return filtered_components


def _build_cmu_card_html(cmu_id, components, component_debug):
    """
    Build HTML for a CMU card.

    Args:
        cmu_id: CMU ID
        components: List of component dictionaries
        component_debug: Debug info string

    Returns:
        HTML string for the CMU card
    """
    # Get auction info for this CMU ID
    auction_year_info = {}
    for comp in components:
        auction = comp.get("Auction Name", "")
        if auction:
            # Try to extract auction year and type (e.g., "T-4 2024/25")
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

    # Build CMU card HTML
    html = f"""
    <div class="col-md-6 mb-3">
        <div class="card cmu-card">
            <div class="card-header bg-light">
                <div class="d-flex justify-content-between align-items-center">
                    <span>CMU ID: <strong>{cmu_id}</strong></span>
                    <a href="/components/?q={cmu_id}" class="btn btn-sm btn-info">View Components</a>
                </div>
                <div class="small text-muted mt-1">{component_debug} (filtered to {len(components)})</div>
                {f'<div class="small mt-1">Auctions: {auction_info}</div>' if auction_info else ''}
            </div>
            <div class="card-body">
                <p><strong>Components:</strong> {len(components)}</p>
                {format_location_list(locations, components)}
            </div>
        </div>
    </div>
    """

    return html


def get_cmu_details(cmu_id):
    """
    HTMX endpoint to get CMU details.
    This function will be used by the HTMX endpoint to load CMU details lazily.
    """
    components, _ = fetch_components_for_cmu_id(cmu_id)

    if not components:
        return f"<div>No components found for CMU ID {cmu_id}</div>"

    # Get unique locations
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