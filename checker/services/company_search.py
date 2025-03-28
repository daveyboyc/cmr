import pandas as pd
import urllib.parse
import logging
import time
import traceback
import json
from django.shortcuts import render, redirect
from django.core.cache import cache
from django.http import HttpResponse
from django.template.loader import render_to_string
import requests

from ..utils import normalize, get_cache_key, format_location_list, safe_url_param, from_url_param
from .data_access import (
    get_cmu_dataframe,
    fetch_components_for_cmu_id,
    get_component_data_from_json,
    save_component_data_to_json
)

logger = logging.getLogger(__name__)


def search_companies_service(request, extra_context=None, return_data_only=False):
    """Service function for searching companies"""
    results = {}
    error_message = None
    api_time = 0
    query = request.GET.get("q", "").strip()
    sort_order = request.GET.get("sort", "desc")  # Get sort order, default to desc (newest first)
    
    # Add these lines to handle potentially expensive queries
    logger = logging.getLogger(__name__)
    
    # Add safety limit for search terms
    if len(query) > 100:
        query = query[:100]
        error_message = "Search query too long, truncated to 100 characters"
        logger.warning(f"Search query truncated: '{query}'")
    
    # Add safer handling for the query with space check
    if query and ' ' in query:
        logger.info(f"Search query contains spaces - potential company name: '{query}'")
        
        # Add a check for problematic queries that might timeout
        if query.lower() in ['grid beyond', 'gridbeyond']:
            logger.warning(f"Known problematic query detected: '{query}' - using optimized path")
            
            # For known problematic queries, limit the search to avoid timeouts
            # This is a temporary fix until we can optimize the database better
            results[query] = [
                f'<a href="/company/gridbeyond/" style="color: blue; text-decoration: underline;">Grid Beyond</a>'
            ]
            
            if return_data_only:
                return results
            
            context = {
                "results": results,
                "record_count": 1,
                "api_time": 0.1,
                "query": query,
                "sort_order": sort_order,
                "error": "Using optimized search results for this query."
            }
            
            if extra_context:
                context.update(extra_context)
                
            return render(request, "checker/search.html", context)
    
    if request.method == "GET":
        # Shortcut for component results
        if query.startswith("CM_"):
            component_id = query
            logger.info(f"Direct component ID query: {component_id}")
            components = get_component_data_from_json(component_id)
            
            if components:
                logger.info(f"Found {len(components)} components for direct CMU ID: {component_id}")
                # Use component name if available
                company_name = None
                if components and len(components) > 0 and "Company Name" in components[0]:
                    company_name = components[0]["Company Name"]
                    
                if company_name:
                    # Instead of redirecting, perform a company search
                    logger.info(f"Searching for company: {company_name}")
                    norm_query = normalize(company_name)
                    cmu_df, df_api_time = get_cmu_dataframe()
                    api_time += df_api_time
                    
                    if cmu_df is not None:
                        matching_records = _perform_company_search(cmu_df, norm_query)
                        unique_companies = list(matching_records["Full Name"].unique())
                        
                        # Make sure the company is included
                        if company_name not in unique_companies:
                            unique_companies.append(company_name)
                            
                        results = _build_search_results(cmu_df, unique_companies, sort_order, company_name)
                        
                        if return_data_only:
                            return results
                            
                        context = {
                            "results": results,
                            "record_count": len(cmu_df),
                            "error": error_message,
                            "api_time": api_time,
                            "query": company_name,
                            "sort_order": sort_order,
                        }
                        
                        if extra_context:
                            context.update(extra_context)
                            
                        return render(request, "checker/search.html", context)
        
        elif "debug" in request.GET:
            # Show all companies (for debugging)
            cmu_df, df_api_time = get_cmu_dataframe()
            api_time += df_api_time
            
            if cmu_df is not None:
                sample_companies = list(cmu_df["Full Name"].sample(5).unique())
                results = _build_search_results(cmu_df, sample_companies, sort_order, "Debug Sample")
                
                if return_data_only:
                    return results
                
                context = {
                    "results": results,
                    "record_count": len(cmu_df),
                    "debug": True,
                    "sample_companies": sample_companies,
                    "sort_order": sort_order
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
            
    
                
            results = _build_search_results(cmu_df, unique_companies, sort_order, query, add_debug_info=True)

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

def _build_search_results(cmu_df, unique_companies, sort_order, query, add_debug_info=False):
    """
    Build search results for companies.
    Returns a dictionary with query as key and list of formatted company links as values.
    """
    logger = logging.getLogger(__name__)
    results = {query: []}
    company_count = len(unique_companies)
    
    # Debug info for component retrieval
    debug_info = {
        "company_count": company_count,
        "companies_with_years": 0,
        "companies_with_components": 0,
        "total_cmu_ids": 0,
        "total_components": 0
    }
    
    # Process each matching company
    for company in unique_companies:
        # Get all CMU IDs for this company
        company_records = cmu_df[cmu_df["Full Name"] == company]
        cmu_ids = company_records["CMU ID"].unique().tolist()
        
        debug_info["total_cmu_ids"] += len(cmu_ids)
        
        # Organize years from the records
        year_data = _organize_year_data(company_records, sort_order)
        
        if year_data:
            debug_info["companies_with_years"] += 1
            
            # Check if any CMU has components
            has_components = False
            company_component_count = 0
            
            for cmu_id in cmu_ids:
                components = get_component_data_from_json(cmu_id)
                if components:
                    has_components = True
                    company_component_count += len(components)
                    
                    # If this is the specific company we're debugging, log more details
                    if add_debug_info and company == "LIMEJUMP LTD":
                        logger.info(f"Found {len(components)} components for LIMEJUMP LTD's CMU ID: {cmu_id}")
                        logger.info(f"First component: {components[0].get('Location and Post Code', 'N/A')}")
            
            debug_info["total_components"] += company_component_count
            
            if has_components:
                debug_info["companies_with_components"] += 1
            
            # Generate a simple blue link for the company instead of a card
            company_id = normalize(company)
            company_html = f'<a href="/company/{company_id}/" style="color: blue; text-decoration: underline;">{company}</a>'
            
            # Add additional information about CMU IDs and components count
            cmu_ids_str = ", ".join(cmu_ids[:3])
            if len(cmu_ids) > 3:
                cmu_ids_str += f" and {len(cmu_ids) - 3} more"
                
            company_html = f"""
            <div>
                <strong>{company_html}</strong>
                <div class="mt-1 mb-1"><span class="text-muted">CMU IDs: {cmu_ids_str}</span></div>
                <div>{company_component_count} components across {len(cmu_ids)} CMU IDs</div>
            </div>
            """
            
            results[query].append(company_html)
    
    if add_debug_info:
        logger.info(f"Search results debug: {debug_info}")
        
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

def get_auction_components(company_id, year, auction_name=None):
    """
    Get components for a specific year and auction for a company.
    """
    logger = logging.getLogger(__name__)
    
    debug_info = []
    cmu_ids = []
    total_components_found = 0

    # Get the CMU IDs from the dataframe
    cmu_df, _ = get_cmu_dataframe()
    if cmu_df is not None:
        # Get company name from ID
        company_name = None
        matching_rows = cmu_df[cmu_df["Normalized Full Name"] == company_id]
        if not matching_rows.empty:
            company_name = matching_rows.iloc[0]["Full Name"]
            debug_info.append(f"Found company name: {company_name}")
            
            # Get all CMU IDs for this company
            company_records = cmu_df[cmu_df["Full Name"] == company_name]
            df_cmu_ids = company_records["CMU ID"].unique().tolist()
            
            # Add to the CMU IDs list
            for cmu_id in df_cmu_ids:
                if cmu_id not in cmu_ids:
                    cmu_ids.append(cmu_id)
                    
            debug_info.append(f"Found {len(df_cmu_ids)} CMU IDs from dataframe")
    
    # Fetch components for all CMU IDs using the method that works in search
    for cmu_id in cmu_ids:
        # First try to get components using JSON method (which works in search)
        components = get_component_data_from_json(cmu_id)
        
        # If no components found, try the fetch method as fallback
        if not components:
            components, _ = fetch_components_for_cmu_id(cmu_id)
        
        # Count total components found
        if components:
            total_components_found += len(components)
            logger.info(f"Found {len(components)} components for CMU ID {cmu_id}")
    
    # Now add the total count to debug info
    debug_info.append(f"Total components before filtering: {total_components_found}")
    debug_info = ", ".join(debug_info)

    # Generate HTML for each CMU
    html = f"<div class='small text-muted mb-2'>{debug_info}</div><div class='row'>"
    
    total_filtered_components = 0
    cmu_with_components = 0

    for cmu_id in cmu_ids:
        # Get components the same way as before
        components = get_component_data_from_json(cmu_id)
        if not components:
            components, _ = fetch_components_for_cmu_id(cmu_id)
        
        if not components:
            continue
            
        component_debug = f"Found {len(components)} components for CMU ID {cmu_id}"

        # Filter components by year and auction if needed
        filtered_components = _filter_components_by_year_auction(components, year, auction_name)
        
        if filtered_components:
            total_filtered_components += len(filtered_components)
            cmu_with_components += 1
            logger.info(f"After filtering: {len(filtered_components)} components match year={year}, auction={auction_name}")

        if not filtered_components:
            component_debug += f" (filtered to 0 for {year}"
            if auction_name:
                component_debug += f", {auction_name}"
            component_debug += ")"
            continue  # Skip this CMU if no matching components

        # Generate CMU card HTML
        print(f"Building CMU card for {cmu_id} with {len(components)} components")
        html += _build_cmu_card_html(cmu_id, filtered_components, component_debug)

    # Add summary stats to the HTML
    html += f"</div><div class='alert alert-info mt-3'>Found {total_filtered_components} components across {cmu_with_components} CMU IDs that match year={year}, auction={auction_name}</div>"
    
    return html

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
    Returns the filtered components list.
    """
    logger = logging.getLogger(__name__)
    filtered_components = []
    
    # Extract the first year from a year range like "2028-29"
    year_to_match = str(year).split('-')[0].strip() if year else ""
    
    # Normalize auction name for more flexible matching
    norm_auction_name = None
    if auction_name:
        # Convert to lowercase and remove special characters
        norm_auction_name = auction_name.lower().replace('-', ' ').replace('(', '').replace(')', '')
    
    logger.info(f"Filtering {len(components)} components for year={year} (matching={year_to_match}), auction={auction_name}")
    
    year_matches = 0
    auction_matches = 0
    
    for comp in components:
        # Get component data
        comp_delivery_year = str(comp.get("Delivery Year", ""))
        comp_auction = comp.get("Auction Name", "")
        comp_type = comp.get("Type", "")
        
        # Log first component for debugging
        if filtered_components == [] and components:
            logger.info(f"Sample component: year={comp_delivery_year}, auction={comp_auction}, type={comp_type}")
        
        # SUPER FLEXIBLE YEAR MATCHING - try multiple approaches
        year_match = False
        # Strategy 1: Check if the component year contains our year
        if year_to_match in comp_delivery_year:
            year_match = True
        # Strategy 2: Check if our year contains the component year
        elif comp_delivery_year in year_to_match:
            year_match = True
        # Strategy 3: For numeric years, check if they're equal when converted to integers
        elif year_to_match.isdigit() and comp_delivery_year.isdigit():
            if int(year_to_match) == int(comp_delivery_year):
                year_match = True
                
        if not year_match:
            continue
            
        year_matches += 1
            
        # If auction name is specified, use very flexible matching
        if auction_name:
            auction_match = False
            
            # Normalize the component auction name
            norm_comp_auction = comp_auction.lower().replace('-', ' ').replace('(', '').replace(')', '')
            norm_comp_type = comp_type.lower().replace('-', ' ')
            
            # Strategy 1: Check if normalized strings have significant overlap
            if norm_auction_name in norm_comp_auction or norm_comp_auction in norm_auction_name:
                auction_match = True
            # Strategy 2: Check for type match (T-4, T-1)
            elif "t 1" in norm_auction_name and ("t 1" in norm_comp_auction or "t1" in norm_comp_auction or "t 1" in norm_comp_type):
                auction_match = True
            elif "t 4" in norm_auction_name and ("t 4" in norm_comp_auction or "t4" in norm_comp_auction or "t 4" in norm_comp_type):
                auction_match = True
            # Strategy 3: Extract and match year ranges
            else:
                # Try to extract year ranges like 2028-29 or 2028 29
                import re
                auction_years = re.findall(r'20\d\d[\s-]\d\d', norm_auction_name)
                comp_years = re.findall(r'20\d\d[\s-]\d\d', norm_comp_auction)
                
                # If we found year ranges in both, see if any match
                if auction_years and comp_years:
                    # Standardize the format by removing spaces
                    norm_auction_years = [y.replace(' ', '') for y in auction_years]
                    norm_comp_years = [y.replace(' ', '') for y in comp_years]
                    
                    # Check for any overlap
                    for a_year in norm_auction_years:
                        for c_year in norm_comp_years:
                            if a_year == c_year:
                                auction_match = True
                                break
            
            if not auction_match:
                continue
                
        auction_matches += 1
        filtered_components.append(comp)
    
    logger.info(f"Filtering results: {len(filtered_components)} matches (year matches: {year_matches}, auction matches: {auction_matches})")
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

def _organize_year_data(company_records, sort_order):
    """
    Organize year data for a company.
    Returns a list of year objects with auctions.
    """
    # Extract unique years and auctions
    year_auctions = {}
    for _, row in company_records.iterrows():
        year = str(row.get("Delivery Year", ""))
        if not year or year == "nan":
            continue
            
        if year not in year_auctions:
            year_auctions[year] = {}
            
        auction = row.get("Auction Name", "")
        if auction and auction != "nan":
            year_auctions[year][auction] = True
            
    # Convert to list of year objects
    year_data = []
    for year, auctions in year_auctions.items():
        year_id = f"year-{year.replace(' ', '-').lower()}"
        year_data.append({
            "year": year,
            "year_id": year_id,
            "auctions": list(auctions.keys())
        })
        
    # Sort by year
    ascending = sort_order == "asc"
    year_data.sort(key=lambda x: try_parse_year(x['year']), reverse=not ascending)
    
    return year_data

def fetch_components_for_cmu_id(cmu_id, limit=1000):
    """
    Fetch components for a specific CMU ID.
    Checks cache and JSON before making API request.
    Includes pagination to handle large result sets.
    """
    logger = logging.getLogger(__name__)
    
    if not cmu_id:
        logger.warning("No CMU ID provided to fetch_components_for_cmu_id")
        return [], 0

    # First check if we already have cached data for this CMU ID
    components_cache_key = get_cache_key("components_for_cmu", cmu_id)
    logger.info(f"Checking cache for components with key: {components_cache_key}")
    cached_components = cache.get(components_cache_key)
    if cached_components is not None:
        logger.info(f"Using cached components for {cmu_id}, found {len(cached_components)}")
        return cached_components, 0

    # Check if we have the data in our JSON file - CASE-INSENSITIVE
    logger.info(f"Checking JSON for components for CMU ID: {cmu_id}")
    json_components = get_component_data_from_json(cmu_id)
    if json_components is not None:
        logger.info(f"Using JSON-stored components for {cmu_id}, found {len(json_components)}")
        # Also update the cache
        cache.set(components_cache_key, json_components, 3600)
        return json_components, 0

    logger.info(f"No components found in cache or JSON for {cmu_id}, fetching from API")
    
    start_time = time.time()
    
    # Try scraping from the API with pagination
    all_components = []
    offset = 0
    page_size = 100  # API might have a max limit per request
    
    base_url = "https://api.neso.energy/api/3/action/datastore_search"
    
    while True:
        try:
            params = {
                "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
                "q": cmu_id,
                "limit": page_size,
                "offset": offset,
                "sort": "Delivery Year desc"
            }
            
            logger.info(f"Making API request for CMU ID: {cmu_id}, offset: {offset}")
            response = requests.get(base_url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            if data.get("success"):
                records = data.get("result", {}).get("records", [])
                logger.info(f"API returned {len(records)} records for {cmu_id} at offset {offset}")
                
                if not records:
                    # No more records to fetch
                    break
                    
                all_components.extend(records)
                
                # Check if we've fetched all available records
                total_available = data.get("result", {}).get("total", 0)
                if offset + len(records) >= total_available or len(records) < page_size:
                    break
                    
                # Move to the next page
                offset += page_size
            else:
                logger.error(f"API request unsuccessful for {cmu_id}: {data.get('error', 'Unknown error')}")
                break
                
        except Exception as e:
            logger.error(f"Error fetching components for {cmu_id}: {e}")
            break
    
    if all_components:
        logger.info(f"Total {len(all_components)} components fetched for {cmu_id}")
        # Update the JSON file
        save_component_data_to_json(cmu_id, all_components)
        # Also update the cache
        cache.set(components_cache_key, all_components, 3600)
    
    elapsed_time = time.time() - start_time
    return all_components, elapsed_time