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
    """Service function for searching companies with improved caching and performance"""
    # Imports at the top
    import logging
    from django.core.cache import cache
    
    # Initialize logger once
    logger = logging.getLogger(__name__)
    
    # Initialize variables
    results = {}
    error_message = None
    api_time = 0
    query = request.GET.get("q", "").strip()
    sort_order = request.GET.get("sort", "desc")  # Get sort order, default to desc (newest first)
    
    # Check cache first for any query
    if query:
        # Use a safe cache key that doesn't contain spaces
        cache_key = get_cache_key("search_results", query.lower())
        cached_results = cache.get(cache_key)
        
        if cached_results:
            logger.info(f"Using cached search results for '{query}'")
            if return_data_only:
                return cached_results
            
            # Handle non-return_data_only case with cached results
            context = {
                "results": cached_results,
                "record_count": sum(len(matches) for matches in cached_results.values()),
                "api_time": 0.1,  # Fast because from cache
                "query": query,
                "sort_order": sort_order,
                "note": "Using cached results"
            }
            if extra_context:
                context.update(extra_context)
            return render(request, "checker/search.html", context)
    
    # Add safety limit for search terms
    if len(query) > 100:
        query = query[:100]
        error_message = "Search query too long, truncated to 100 characters"
        logger.warning(f"Search query truncated: '{query}'")
    
    # Process GET requests
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
            # Execute an optimized search that will work for all companies
            start_time = time.time()
            norm_query = normalize(query)
            
            # First, try a direct database search for companies
            company_results = []
            
            try:
                # Use the Component model to directly find matching companies
                from ..models import Component
                # Get matching companies directly from the database with a count
                from django.db.models import Count, Q
                
                # Build a query that works for spaces using separate terms
                query_terms = query.lower().split()
                query_filter = Q()
                
                for term in query_terms:
                    if len(term) >= 3:  # Only use terms with at least 3 characters
                        query_filter |= Q(company_name__icontains=term)
                
                matching_companies = Component.objects.filter(query_filter)\
                    .values('company_name')\
                    .annotate(cmu_count=Count('cmu_id', distinct=True), 
                              total_components=Count('id'))\
                    .order_by('-cmu_count')[:50]  # Limit to top 50 companies
                
                # Format results directly
                for company in matching_companies:
                    if not company['company_name']:
                        continue
                        
                    company_name = company['company_name'] 
                    company_id = normalize(company_name)
                    cmu_count = company['cmu_count']
                    component_count = company['total_components']
                    
                    # Create formatted HTML
                    company_html = f'<a href="/company/{company_id}/" style="color: blue; text-decoration: underline;">{company_name}</a>'
                    company_results.append(f"""
                    <div>
                        <strong>{company_html}</strong>
                        <div class="mt-1 mb-1"><span class="text-muted">Company in component database</span></div>
                        <div>{component_count} components across {cmu_count} CMU IDs</div>
                    </div>
                    """)
                
                logger.info(f"Direct DB search found {len(company_results)} companies for '{query}'")
                    
            except Exception as e:
                logger.exception(f"Error in direct database search: {e}")
                # If the direct DB search fails, fall back to the dataframe method
                
            # If we found results directly, use them
            if company_results:
                results[query] = company_results
                api_time = time.time() - start_time
            else:
                # Fall back to dataframe-based search with tight limits
                logger.info(f"Falling back to dataframe search for '{query}'")
                
                # Limit company processing to avoid timeouts
                component_limit = 20  # Only process the top 20 companies max
                cmu_limit = 3         # Only check up to 3 CMU IDs per company
                
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
                
                # Limit the number of companies to avoid timeouts
                unique_companies = list(matching_records["Full Name"].unique())[:component_limit]
                
                # Use a version of _build_search_results that limits CMU ID checks
                results = _build_search_results(cmu_df, unique_companies, sort_order, query, 
                                              cmu_limit=cmu_limit, add_debug_info=True)
            
            # Cache these search results - use safe cache key
            if query:
                cache_key = get_cache_key("search_results", query.lower())
                total_items = sum(len(matches) for matches in results.values())
                
                # Cache longer for smaller result sets
                if total_items < 10:
                    cache.set(cache_key, results, 7200)     # 2 hours for very small results
                elif total_items < 100:
                    cache.set(cache_key, results, 3600)     # 1 hour for small results
                elif total_items < 500:
                    cache.set(cache_key, results, 1800)     # 30 minutes for medium results
                else:
                    cache.set(cache_key, results, 600)      # 10 minutes for large results
                    
                logger.info(f"Cached {total_items} results for query '{query}'")

            request.session["search_results"] = results
            request.session["record_count"] = record_count if 'record_count' in locals() else len(company_results)
            request.session["api_time"] = api_time
            request.session["last_query"] = query
            
            if return_data_only:
                return results

            context = {
                "results": results,
                "record_count": record_count if 'record_count' in locals() else len(company_results),
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

def _build_search_results(cmu_df, unique_companies, sort_order, query, cmu_limit=5, add_debug_info=False):
    """
    Build search results for companies.
    Returns a dictionary with query as key and list of formatted company links as values.
    
    Args:
        cmu_df: DataFrame with CMU data
        unique_companies: List of company names to include
        sort_order: Sort order for years ('asc' or 'desc')
        query: Original search query
        cmu_limit: Maximum number of CMU IDs to check per company (default: 5)
        add_debug_info: Whether to log debug information
    
    Returns:
        Dictionary with query as key and list of formatted company links as values
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
        # Skip empty company names
        if not company:
            continue
            
        # Get all CMU IDs for this company
        company_records = cmu_df[cmu_df["Full Name"] == company]
        cmu_ids = company_records["CMU ID"].unique().tolist()
        
        debug_info["total_cmu_ids"] += len(cmu_ids)
        
        # Organize years from the records
        year_data = _organize_year_data(company_records, sort_order)
        
        if year_data:
            debug_info["companies_with_years"] += 1
            
            # Check if any CMU has components, but limit the search to avoid timeouts
            has_components = False
            company_component_count = 0
            cmu_ids_checked = 0
            
            try:
                # Use a direct database query to get component counts
                from ..models import Component
                
                # First, check if ANY components exist for this company name
                if Component.objects.filter(company_name=company).exists():
                    has_components = True
                    # Get count from database
                    company_component_count = Component.objects.filter(company_name=company).count()
                    # Get count of CMU IDs
                    cmu_count_db = Component.objects.filter(company_name=company).values('cmu_id').distinct().count()
                    debug_info["companies_with_components"] += 1
                else:
                    # Only check a limited number of CMU IDs for components
                    cmu_ids_to_check = cmu_ids[:min(cmu_limit, len(cmu_ids))]
                    
                    for cmu_id in cmu_ids_to_check:
                        cmu_ids_checked += 1
                        # Check if this CMU has components
                        if Component.objects.filter(cmu_id=cmu_id).exists():
                            has_components = True
                            # Get an approximation of component count
                            count = Component.objects.filter(cmu_id=cmu_id).count()
                            company_component_count += count
                        
                        # If we found components already, no need to check more CMU IDs
                        if has_components and company_component_count > 0:
                            debug_info["companies_with_components"] += 1
                            break
            except Exception as e:
                # If database check fails, fall back to JSON check
                logger.warning(f"Database component check failed for {company}: {e}")
                
                # Only check a limited number of CMU IDs for components
                cmu_ids_to_check = cmu_ids[:min(cmu_limit, len(cmu_ids))]
                
                for cmu_id in cmu_ids_to_check:
                    cmu_ids_checked += 1
                    # Check if this CMU has components
                    try:
                        from ..models import Component
                        if Component.objects.filter(cmu_id=cmu_id).exists():
                            has_components = True
                            company_component_count += Component.objects.filter(cmu_id=cmu_id).count()
                            break
                    except:
                        # If that fails, try the JSON method
                        try:
                            components = get_component_data_from_json(cmu_id)
                            if components:
                                has_components = True
                                company_component_count += len(components)
                                break
                        except:
                            # If both methods fail, continue to the next CMU ID
                            continue
                
                if has_components:
                    debug_info["companies_with_components"] += 1
            
            debug_info["total_components"] += company_component_count
            
            # Generate a simple blue link for the company
            company_id = normalize(company)
            company_html = f'<a href="/company/{company_id}/" style="color: blue; text-decoration: underline;">{company}</a>'
            
            # Add additional information about CMU IDs and components count
            if len(cmu_ids) <= 3:
                cmu_ids_str = ", ".join(cmu_ids)
            else:
                cmu_ids_str = ", ".join(cmu_ids[:3])
                cmu_ids_str += f" and {len(cmu_ids) - 3} more"
            
            # If we didn't find components, show that we have CMU IDs at least
            if not has_components:
                if cmu_ids_checked < len(cmu_ids):
                    # Indicate we only checked some CMU IDs
                    component_info = f"{len(cmu_ids)} CMU IDs found (checked {cmu_ids_checked})"
                else:
                    component_info = f"{len(cmu_ids)} CMU IDs found"
            else:
                # Show approximate component count
                if 'cmu_count_db' in locals():
                    # If we have the DB count, show that
                    component_info = f"{company_component_count} components across {cmu_count_db} CMU IDs"
                else:
                    # Otherwise estimate based on what we checked
                    component_info = f"At least {company_component_count} components found"
                
            company_html = f"""
            <div>
                <strong>{company_html}</strong>
                <div class="mt-1 mb-1"><span class="text-muted">CMU IDs: {cmu_ids_str}</span></div>
                <div>{component_info}</div>
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

def auction_components(request, company_id, year, auction_name):
    """
    API endpoint for fetching components for a specific auction
    """
    from django.http import HttpResponse
    from django.db.models import Q
    import logging
    import traceback
    from ..models import Component
    from ..utils import normalize, from_url_param
    
    logger = logging.getLogger(__name__)
    logger.info(f"Loading auction components: company_id={company_id}, year={year}, auction={auction_name}")
    
    # Convert URL parameters from underscores back to spaces
    year = from_url_param(year)
    auction_name = from_url_param(auction_name)
    
    try:
        # Find company name
        company_query = Component.objects.values('company_name').distinct()
        company_name = None
        
        for company in company_query:
            curr_name = company['company_name']
            if curr_name and normalize(curr_name) == company_id:
                company_name = curr_name
                break
        
        if not company_name:
            # Try a more flexible match
            for company in company_query:
                curr_name = company['company_name']
                if curr_name and (company_id in normalize(curr_name) or normalize(curr_name) in company_id):
                    company_name = curr_name
                    break
        
        if not company_name:
            return HttpResponse(f"<div class='alert alert-warning'>Company not found: {company_id}</div>")
        
        logger.info(f"Found company: {company_name}")
        
        # Build a more flexible query for components
        query = Q(company_name=company_name)
        
        # Add year filter with flexible matching
        if year:
            # Handle multiple year formats
            year_query = Q(delivery_year=year)
            year_query |= Q(delivery_year__icontains=year)
            # Also try to match just the first part of a year range (e.g., 2026 in "2026-27")
            import re
            year_number_match = re.search(r'(\d{4})', year)
            if year_number_match:
                year_number = year_number_match.group(1)
                year_query |= Q(delivery_year__icontains=year_number)
            
            query &= year_query
        
        # Add auction filter with flexible matching
        if auction_name:
            # The auction name might be stored differently, try several variations
            auction_query = Q(auction_name=auction_name)
            auction_query |= Q(auction_name__icontains=auction_name)
            
            # Handle T-4/T-1 variations
            if "T-4" in auction_name:
                auction_query |= Q(auction_name__icontains="T4")
                auction_query |= Q(auction_name__icontains="T 4")
            elif "T-1" in auction_name:
                auction_query |= Q(auction_name__icontains="T1")
                auction_query |= Q(auction_name__icontains="T 1")
            
            # Match year part of auction name
            year_in_auction = re.search(r'(\d{4})[/-]?(\d{2})?', auction_name)
            if year_in_auction:
                year_part = year_in_auction.group(0)
                auction_query |= Q(auction_name__icontains=year_part)
            
            query &= auction_query
        
        # Get matching components
        components = Component.objects.filter(query)
        
        # Get all unique CMU IDs
        cmu_ids = components.values('cmu_id').distinct()
        
        logger.info(f"Found {components.count()} components across {cmu_ids.count()} CMU IDs")
        
        if components.count() == 0:
            return HttpResponse(f"""
                <div class='alert alert-warning'>
                    <p>No components found matching these criteria:</p>
                    <ul>
                        <li>Company: {company_name}</li>
                        <li>Year: {year}</li>
                        <li>Auction: {auction_name}</li>
                    </ul>
                    <p>This could be due to differences in how the data is stored in the database.</p>
                </div>
            """)
        
        # Generate HTML with the matching components
        html = f"<div class='component-results mb-3'>"
        html += f"<div class='alert alert-info'>Found {components.count()} components across {cmu_ids.count()} CMU IDs</div>"
        
        # Group by CMU ID
        html += "<div class='row'>"
        
        for cmu_id_obj in cmu_ids:
            cmu_id = cmu_id_obj['cmu_id']
            if not cmu_id:
                continue
                
            # Get components for this CMU ID
            cmu_components = components.filter(cmu_id=cmu_id)
            
            # Get all locations for this CMU
            locations = cmu_components.values('location').distinct()
            
            # Format component records for the template
            cmu_html = f"""
            <div class="col-md-6 mb-3">
                <div class="card cmu-card">
                    <div class="card-header bg-light">
                        <div class="d-flex justify-content-between align-items-center">
                            <span>CMU ID: <strong>{cmu_id}</strong></span>
                            <a href="/components/?q={cmu_id}" class="btn btn-sm btn-info">View Components</a>
                        </div>
                        <div class="small text-muted mt-1">Found {cmu_components.count()} components</div>
                    </div>
                    <div class="card-body">
                        <p><strong>Components:</strong> {cmu_components.count()}</p>
            """
            
            # Add locations list
            if locations:
                cmu_html += "<ul class='list-unstyled'>"
                
                for location_obj in locations:
                    location = location_obj['location']
                    if not location:
                        continue
                        
                    # Add components at this location
                    location_components = cmu_components.filter(location=location)
                    
                    # Create component ID for linking
                    component_id = f"{cmu_id}_{normalize(location)}"
                    
                    # Format location as a blue link
                    location_html = f'<a href="/component/{component_id}/" style="color: blue; text-decoration: underline;">{location}</a>'
                    
                    cmu_html += f"""
                        <li class="mb-2">
                            <strong>{location_html}</strong> <span class="text-muted">({location_components.count()} components)</span>
                            <ul class="ms-3">
                    """
                    
                    # Add description of components
                    for component in location_components:
                        desc = component.description or "No description"
                        tech = component.technology or ""
                        
                        cmu_html += f"""
                            <li><i>{desc}</i>{f" - {tech}" if tech else ""}</li>
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
        error_message = f"Error loading auction components: {str(e)}"
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
    
    
def try_parse_year(year_str):
    """Try to parse a year string to an integer for sorting."""
    if not year_str:
        return 0
        
    try:
        # First try direct conversion
        if isinstance(year_str, (int, float)):
            return int(year_str)
            
        # For strings, check different formats
        if isinstance(year_str, str):
            # Look for 4-digit years like "2024"
            import re
            year_matches = re.findall(r'20\d\d', year_str)
            if year_matches:
                return int(year_matches[0])
                
            # Look for year ranges like "2024/25" or "2024-25"
            range_matches = re.findall(r'(20\d\d)[/-]\d\d', year_str)
            if range_matches:
                return int(range_matches[0])
                
            # Last try: just convert any numbers
            numeric_matches = re.findall(r'\d+', year_str)
            if numeric_matches:
                return int(numeric_matches[0])
                
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
        # Get company name from component database directly
        from ..models import Component
        from django.db.models import Count
        import logging
        
        logger = logging.getLogger(__name__)
        logger.info(f"Loading company detail for company_id: {company_id}")
        
        # Look up company name from normalized ID
        company_components = Component.objects.filter(company_name__isnull=False).order_by('company_name')
        company_name = None
        
        # Try exact match first
        for company in company_components.values('company_name').distinct():
            curr_name = company['company_name']
            if curr_name and normalize(curr_name) == company_id:
                company_name = curr_name
                break
        
        # If still not found, try a more flexible match
        if not company_name:
            for company in company_components.values('company_name').distinct():
                curr_name = company['company_name']
                if curr_name and normalize(curr_name) in company_id or company_id in normalize(curr_name):
                    company_name = curr_name
                    break
        
        if not company_name:
            logger.warning(f"Company not found for ID: {company_id}")
            # Fall back to get_cmu_dataframe, but this is slower
            cmu_df, df_api_time = get_cmu_dataframe()
            if cmu_df is not None:
                for _, row in cmu_df.iterrows():
                    if normalize(row.get("Full Name", "")) == company_id:
                        company_name = row.get("Full Name")
                        break
        
        if not company_name:
            logger.error(f"Company not found after all lookups: {company_id}")
            return render(request, "checker/company_detail.html", {
                "error": f"Company not found: {company_id}",
                "company_name": None
            })
        
        logger.info(f"Found company name: {company_name}")
        
        # Get all delivery years and auctions directly from the database for this company
        # This is much faster than using cmu_df
        years_query = Component.objects.filter(company_name=company_name) \
                               .values('delivery_year', 'auction_name', 'cmu_id') \
                               .distinct()
        
        # Group by year and auction
        year_auction_data = []
        years_mapping = {}
        
        # First, collect all unique years
        for item in years_query:
            year = item['delivery_year']
            if not year or year == 'nan':
                continue
                
            if year not in years_mapping:
                year_id = f"year-{normalize(year)}-{company_id}"
                years_mapping[year] = {
                    'year': year,
                    'auctions': {},
                    'auctions_display': [],
                    'year_id': year_id
                }
        
        # Then add auctions to each year
        for item in years_query:
            year = item['delivery_year']
            auction_name = item['auction_name']
            cmu_id = item['cmu_id']
            
            if not year or year == 'nan' or not auction_name or auction_name == 'nan':
                continue
                
            if year in years_mapping:
                auction_data = years_mapping[year]
                
                # Only add auction if not already added
                if auction_name not in auction_data['auctions']:
                    auction_data['auctions'][auction_name] = []
                    
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
                    
                    # Add to display list
                    auction_data['auctions_display'].append((auction_name, auction_id, badge_class, auction_type))
                
                # Add CMU ID to auction if not already added
                if cmu_id not in auction_data['auctions'][auction_name]:
                    auction_data['auctions'][auction_name].append(cmu_id)
        
        # Convert mapping to list
        for year_data in years_mapping.values():
            if year_data['auctions']:  # Only add years that have auctions
                year_auction_data.append(year_data)
                
        # Sort the years
        sort_order = request.GET.get("sort", "desc")
        year_auction_data.sort(
            key=lambda x: try_parse_year(x['year']),
            reverse=(sort_order == "desc")
        )
        
        logger.info(f"Found {len(year_auction_data)} years with auctions for {company_name}")
        
        return render(request, "checker/company_detail.html", {
            "company_name": company_name,
            "company_id": company_id,
            "year_auction_data": year_auction_data,
            "api_time": 0,
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