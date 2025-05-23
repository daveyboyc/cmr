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
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
import re
from django.db.models import Count, Value
from django.db.models.functions import Coalesce
from ..models import Component
from rapidfuzz import fuzz, process
from django.urls import reverse

from ..utils import (
    normalize,
    get_cache_key,
    format_location_list,
    safe_url_param,
    from_url_param,
)
from .data_access import (
    get_cmu_dataframe,
    fetch_components_for_cmu_id,
    get_component_data_from_json,
    save_component_data_to_json,
)
from .search_logic import analyze_query

logger = logging.getLogger(__name__)

# --- Moved imports here ---
from django.db.models import Q, Count, Value
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from ..models import Component
from ..utils import normalize # Ensure utils are imported too
# --- End moved imports ---

def search_companies_service(request, extra_context=None, return_data_only=False):
    """Service function for searching companies with improved caching and performance"""
    # Define logger earlier
    logger = logging.getLogger(__name__) 
    
    # --- DEBUG: Force cache clear ---
    # try:
    #     cache.delete("cmu_df")
    #     logger.warning("DEBUG: Cleared 'cmu_df' cache key.")
    # except Exception as e:
    #     logger.error(f"DEBUG: Failed to clear 'cmu_df' cache: {e}")
    # --- END DEBUG ---
    
    logger.warning("--- ENTERING search_companies_service ---")

    # Initialize variables
    results = {} # Fallback results dict
    error_message = None
    api_time = 0
    query = request.GET.get("q", "").strip()
    sort_order = request.GET.get("sort", "desc") # For company results
    comp_sort_order = request.GET.get("comp_sort", "desc") # <<< Get component sort order
    sort_field = request.GET.get("sort_field", "date") # <<< Get component sort field
    # search_type = request.GET.get("search_type", "general") # <<< REMOVE THIS
    note = None # Initialize note

    # Analyze the query type
    inferred_search_type, analyzed_query_value = analyze_query(query)
    logger.info(f"Analyzed query '{query}'. Type: {inferred_search_type}, Value: {analyzed_query_value}")

    # Check cache first (can be removed later if cache is not desired)
    # if query:
    #     cache_key = get_cache_key("search_results", query.lower())
    #     cached_data = cache.get(cache_key)
    #     if cached_data:
    #         logger.info(f"Using cached search data for '{query}'")
    #         if return_data_only:
    #             return cached_data.get("page_obj", []) # Adjust based on cached structure
    #         # Add note about cached results
    #         cached_data.setdefault("note", "Using cached results")
    #         return render(request, "checker/search.html", cached_data)

    # Add safety limit for search terms
    if len(query) > 100:
        query = query[:100]
        error_message = "Search query too long, truncated to 100 characters"
        logger.warning(f"Search query truncated: '{query}'")

    logger.warning(f"--- Starting live search for query: '{query}' ---")

    # Initialize context variables that might be needed later
    company_links = []
    company_link_count = 0
    page_obj = None
    paginator = None
    component_count = 0
    render_time_links = 0
    per_page = 50
    page = 1
    search_method = "Initial"

    # Process GET requests
    if request.method == "GET":
        if query:
            start_time = time.time()
            search_method = "Hybrid DB Search"
            try:
                # --- 1. Find Matching Companies (Candidate Generation) ---
                start_company_find_time = time.time()
                # Pass the original query for fuzzy/text matching against names
                company_candidates_df, company_find_api_time = find_candidate_companies(query)
                api_time += company_find_api_time # Add time spent finding candidates
                
                # If fuzzy search found candidates, build links
                company_links = []
                company_link_count = 0
                render_time_links = 0
                if not company_candidates_df.empty:
                    unique_top_companies = company_candidates_df['Full Name'].unique().tolist()
                    logger.info(f"Building links for {len(unique_top_companies)} top companies.")
                    try:
                        cmu_df, df_api_time = get_cmu_dataframe()
                        api_time += df_api_time
                        if cmu_df is None:
                             raise ValueError("CMU DataFrame could not be loaded.")

                        company_data_list, render_time_links_ = _build_search_results(
                            cmu_df, unique_top_companies, sort_order, query, cmu_limit=3, add_debug_info=True
                        )
                        # Debug: Log the type and content of the returned list
                        logger.debug(f"Type of _build_search_results return: {type(company_data_list)}")
                        if isinstance(company_data_list, list) and company_data_list:
                            logger.debug(f"First item type in company_data_list: {type(company_data_list[0])}")
                            logger.debug(f"First item content: {str(company_data_list[0])[:200]}") # Log first 200 chars

                        # Extract HTML for template and CMU IDs for filtering
                        company_links = [item['html'] for item in company_data_list]
                        company_link_count = len(company_links)
                        all_found_cmu_ids = set()
                        for company_data in company_data_list:
                            if isinstance(company_data, dict) and 'cmu_ids' in company_data:
                                all_found_cmu_ids.update(company_data['cmu_ids'])
                        render_time_links += render_time_links_
                        logger.info(f"_build_search_results generated {company_link_count} company links.")
                    except Exception as build_e:
                         logger.error(f"Error building company links: {build_e}")
                         company_links = [] # Reset links if building fails
                         company_link_count = 0
                         all_found_cmu_ids = set()
                else:
                     logger.info("No candidate companies found, skipping link generation.")
                     all_found_cmu_ids = set()

                logger.info(f"Company links final count before component search: {company_link_count}")
                logger.info(f"Total unique CMU IDs from company links: {len(all_found_cmu_ids)}")
                company_find_duration = time.time() - start_company_find_time
                logger.info(f"--- STEP 1 (Company Find & Link Build) took: {company_find_duration:.4f}s ---")
                # --- END OF STEP 1 ---
                
                # --- 2. Find Matching Components (Broad & Paginated) ---
                start_component_find_time = time.time()
                component_query_filter = Q()
                
                # Check if the search is specifically for a CMU ID
                if inferred_search_type == 'cmu_id' and analyzed_query_value:
                    logger.info(f"CMU ID specific search detected for query: '{analyzed_query_value}'. Applying exact match filter.")
                    component_query_filter = Q(cmu_id__iexact=analyzed_query_value)
                    # Optionally disable company search if only CMU results are needed
                    # company_links = [] 
                    # company_link_count = 0
                    # all_found_cmu_ids = set()
                elif query: # Use original query for broad filtering
                    # Original broad search logic for general queries
                    logger.info(f"General search detected for query: '{query}'. Applying broad filters.")
                    component_terms = query.split()
                    for term in component_terms:
                        if len(term) >= 2:
                            term_filter = (
                                Q(location__icontains=term) | Q(description__icontains=term) |
                                Q(technology__icontains=term) | Q(company_name__icontains=term) |
                                Q(cmu_id__icontains=term) # Keep icontains here for general search
                            )
                            component_query_filter |= term_filter
                    # Add exact CMU ID match as well for general search if inferred type wasn't CMU
                    if inferred_search_type != 'cmu_id' and analyzed_query_value:
                         component_query_filter |= Q(cmu_id__iexact=analyzed_query_value)
                    
                    # --- BOOST RESULTS BASED ON FOUND CMU IDs --- 
                    # If we found company links and their corresponding CMU IDs, boost those.
                    # We can do this by adding an OR condition for those CMU IDs.
                    # This doesn't strictly filter, but includes them alongside other matches.
                    if all_found_cmu_ids:
                        logger.info(f"Including components from {len(all_found_cmu_ids)} CMU IDs found via company search.")
                        component_query_filter |= Q(cmu_id__in=list(all_found_cmu_ids))
                    # ---------------------------------------------
                
                if not component_query_filter:
                     logger.warning("Component query filter is empty. No components searched.")
                     all_components = Component.objects.none()
                     component_count = 0
                else:
                    logger.warning(f"Attempting component query with filter: {component_query_filter}")
                    # --- FIX SORTING LOGIC HERE ---
                    comp_sort_prefix = '-' if comp_sort_order == 'desc' else ''
                    # Determine the field to sort components by
                    if sort_field == 'location':
                         comp_django_sort_field = f'{comp_sort_prefix}location'
                    else: # Default to date
                         comp_django_sort_field = f'{comp_sort_prefix}delivery_year'
                         
                    logger.info(f"Applying component sort: {comp_django_sort_field}") # Log the sort being applied
                    # --- END FIX ---
                    all_components = Component.objects.filter(component_query_filter).order_by(comp_django_sort_field).distinct()
                    component_count = all_components.count()
                    logger.warning(f"Component query found {component_count} components.")

                # Pagination for Components
                if component_count > 0:
                    paginator = Paginator(all_components, per_page)
                    try:
                        page_obj = paginator.page(page)
                    except PageNotAnInteger: page_obj = paginator.page(1)
                    except EmptyPage: page_obj = paginator.page(paginator.num_pages)
                else:
                    paginator = None
                    page_obj = None # Ensure page_obj is None if no components

                api_time = time.time() - start_time
                logger.warning(f"Successfully completed {search_method}. API time: {api_time:.4f}s")
                # --- END OF STEP 2 & Main Try Block ---
            
            except Exception as e:
                search_method = "Search Failed"
                logger.error(f"!!!!!!!! MAIN SEARCH FAILED! Error: {e} !!!!!!!!")
                logger.exception("Full traceback for main search failure:")
                error_message = f"An unexpected error occurred during the search: {e}"
                api_time = time.time() - start_time # Time until failure
                # Reset potentially partially populated variables
                company_links = []
                company_link_count = 0
                page_obj = None
                paginator = None
                component_count = 0
                render_time_links = 0
            # --- End of Main Except Block --- 

            # --- Build Final Context --- 
            context = {
                "query": query,
                "company_links": company_links, 
                "company_count": company_link_count, 
                "displayed_company_count": company_link_count,
                "page_obj": page_obj, 
                "paginator": paginator, 
                "component_count": component_count, 
                "total_component_count": component_count,
                "total_pages": paginator.num_pages if paginator else 0,
                "page": page,
                "has_prev": page_obj.has_previous() if page_obj else False,
                "has_next": page_obj.has_next() if page_obj else False,
                "page_range": paginator.get_elided_page_range(number=page, on_each_side=2, on_ends=1) if paginator else [],
                "comp_sort": comp_sort_order,
                "sort_field": sort_field,
                "per_page": per_page,
                "error": error_message,
                "api_time": api_time,
                "render_time_links": render_time_links, 
                "sort_order": sort_order,
                "unified_search": True, # Assume unified unless error 
                "search_method": search_method,
                "note": note,
            }
            
            if extra_context:
                context.update(extra_context)
            
            # Cache results if successful?
            # if query and not error_message and search_method == "Hybrid DB Search":
            #    try:
            #       cache_key = get_cache_key("search_results", query.lower())
            #       # Decide cache time based on results?
            #       cache_time = 1800 # e.g., 30 mins
            #       cache.set(cache_key, context, cache_time) 
            #    except Exception as cache_e: logger.error(f"Failed to cache results: {cache_e}")
                
            return render(request, "checker/search.html", context)
            # --- End of if query: block ---

        else: # No query provided
            # Clear session data if needed
            # if "search_results" in request.session: request.session.pop("search_results", None)
            # if "api_time" in request.session: request.session.pop("api_time", None)
            pass # Just render empty search page
    
    # Default context if not GET or no query
    context = {
        "query": query,
        "company_links": company_links,
        "company_count": company_link_count,
        "page_obj": page_obj,
        "component_count": component_count,
        "error": error_message,
        "api_time": api_time,
        "sort_order": sort_order,
        "unified_search": True,
        "search_method": search_method,
        "note": note,
    }
    if extra_context: context.update(extra_context)
    return render(request, "checker/search.html", context)
# --- End of search_companies_service ---


def _perform_company_search(cmu_df, query):
    """
    Perform fuzzy search for companies based on the query using RapidFuzz.
    Uses token_set_ratio for flexibility with extra words.
    Returns a DataFrame of matching records, sorted by score.
    """
    logger = logging.getLogger(__name__)
    min_score = 85
    scorer = fuzz.partial_token_set_ratio
    norm_query = normalize(query) # Normalize the incoming query

    # Prepare choices for fuzzy matching
    company_names = cmu_df["Normalized Full Name"].dropna().unique().tolist()

    # --- DEBUG --- 
    logger.debug(f"Normalized search query: '{norm_query}'")
    sample_size = min(10, len(company_names))
    logger.debug(f"Performing fuzzy match against {len(company_names)} unique normalized company names. Sample: {company_names[:sample_size]}")
    # --- END DEBUG ---

    # Perform fuzzy search
    matches_list = process.extract(
        norm_query, company_names, scorer=scorer, score_cutoff=min_score, limit=None
    )

    if not matches_list:
        logger.info(f"No companies found for query '{query}' (normalized: '{norm_query}') with score >= {min_score}")
        return pd.DataFrame(columns=cmu_df.columns)

    # Build score dictionary and filter DataFrame
    matches_with_scores = {match[0]: match[1] for match in matches_list}
    matched_norm_names = set(matches_with_scores.keys())
    
    potential_matches_df = cmu_df[cmu_df["Normalized Full Name"].isin(matched_norm_names)]
    potential_matches_df = potential_matches_df.drop_duplicates(subset=["Full Name"])

    # Add score
    def get_score(row):
        return matches_with_scores.get(row["Normalized Full Name"], 0)

    if potential_matches_df.empty:
        return potential_matches_df
    else:
        potential_matches_df = potential_matches_df.copy()
        potential_matches_df['match_score'] = potential_matches_df.apply(get_score, axis=1)
        # Keep only matches > 0 (should always be true here)
        potential_matches_df = potential_matches_df[potential_matches_df['match_score'] > 0]

    # Sort
    final_sorted_df = potential_matches_df.sort_values(by='match_score', ascending=False).reset_index(drop=True)

    logger.info(f"Found and sorted {len(final_sorted_df)} companies with score >= {min_score} for query '{query}'")
    return final_sorted_df


def _build_search_results(
    cmu_df, unique_companies, sort_order, query, cmu_limit=5, add_debug_info=False
):
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
    results_list = []
    company_count = len(unique_companies)
    processed_normalized_names = set() # --- ADDED: Track processed normalized names ---
    start_render_time = time.time() # Start timing here

    # --- FIX: Return early if no companies were found ---
    if company_count == 0:
        logger.warning(f"No unique companies provided to _build_search_results for query '{query}'. Returning empty results.")
        return results_list, 0.0 # Return empty list and time
    # --- END FIX ---

    # Debug info for component retrieval
    debug_info = {
        "company_count": company_count,
        "companies_with_years": 0,
        "companies_with_components": 0,
        "total_cmu_ids": 0,
        "total_components": 0,
    }

    # Process each matching company
    for company in unique_companies:
        # Skip empty company names
        if not company:
            continue
        
        # --- ADDED: Check if normalized name already processed ---
        normalized_company_id = normalize(company)
        if normalized_company_id in processed_normalized_names:
            logger.debug(f"Skipping duplicate normalized company: '{company}' (normalized: '{normalized_company_id}')")
            continue
        processed_normalized_names.add(normalized_company_id)
        # --- END ADDED ---

        # Get all CMU IDs for this company
        company_records = cmu_df[cmu_df["Full Name"] == company]
        # --- DEBUG: Log shape of slice passed to _organize_year_data ---
        logger.debug(f"_build_search_results: Processing company '{company}'. Records slice shape: {company_records.shape}")
        # --- END DEBUG ---
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
                    company_component_count = Component.objects.filter(
                        company_name=company
                    ).count()
                    # Get count of CMU IDs
                    cmu_count_db = (
                        Component.objects.filter(company_name=company)
                        .values("cmu_id")
                        .distinct()
                        .count()
                    )
                    debug_info["companies_with_components"] += 1
                else:
                    # Only check a limited number of CMU IDs for components
                    cmu_ids_to_check = cmu_ids[: min(cmu_limit, len(cmu_ids))]

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
                cmu_ids_to_check = cmu_ids[: min(cmu_limit, len(cmu_ids))]

                for cmu_id in cmu_ids_to_check:
                    cmu_ids_checked += 1
                    # Check if this CMU has components
                    try:
                        from ..models import Component

                        if Component.objects.filter(cmu_id=cmu_id).exists():
                            has_components = True
                            company_component_count += Component.objects.filter(
                                cmu_id=cmu_id
                            ).count()
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
            company_url = f'/company/{normalized_company_id}/'
            link_html = f'<div><strong><a href="{company_url}">{company}</a></strong></div>'

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
                    component_info = (
                        f"{len(cmu_ids)} CMU IDs found (checked {cmu_ids_checked})"
                    )
                else:
                    component_info = f"{len(cmu_ids)} CMU IDs found"
            else:
                # Show approximate component count
                if "cmu_count_db" in locals():
                    # If we have the DB count, show that
                    component_info = f"{company_component_count} components across {cmu_count_db} CMU IDs"
                else:
                    # Otherwise estimate based on what we checked
                    component_info = (
                        f"At least {company_component_count} components found"
                    )

            company_html_with_details = f"""
            <div>
                <strong>{link_html}</strong>
                <div class="mt-1 mb-1"><span class="text-muted">CMU IDs: {cmu_ids_str}</span></div>
                <div>{component_info}</div>
            </div>
            """

            # Append dictionary with both HTML and CMU IDs
            results_list.append({
                'html': company_html_with_details, # The generated HTML string
                'cmu_ids': cmu_ids          # The list of CMU IDs
            })

    # Calculate total render time
    render_time = time.time() - start_render_time

    if add_debug_info:
        logger.debug(f"_build_search_results Debug Info: {debug_info}")
    
    logger.info(f"_build_search_results generated {len(results_list)} links for {company_count} initial companies in {render_time:.4f}s.")
    return results_list, render_time


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
        year_auction_data.append(
            {"year": year, "auctions": auctions, "year_id": year_id}
        )

    return year_auction_data


def _build_company_card_html(company_name, company_id, year_auction_data):
    """
    Build HTML for a company card using template.
    """
    return render_to_string(
        "checker/components/company_card.html",
        {
            "company_name": company_name,
            "company_id": company_id,
            "year_auction_data": year_auction_data,
        },
    )


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
    from django.urls import reverse

    logger = logging.getLogger(__name__)
    logger.info(
        f"Loading auction components: company_id={company_id}, year={year}, auction={auction_name}"
    )

    # Convert URL parameters from underscores back to spaces
    year = from_url_param(year)
    auction_name = from_url_param(auction_name)
    
    logger.info(f"Parameters after conversion: company_id='{company_id}', year='{year}', auction_name='{auction_name}'")

    try:
        # Safety check for company_id
        if not company_id:
            return HttpResponse(
                f"<div class='alert alert-warning'>No company ID provided</div>"
            )
        
        # Find company name
        company_query = Component.objects.values("company_name").distinct()
        company_name = None
        company_name_variations = []

        for company in company_query:
            curr_name = company["company_name"]
            if curr_name and normalize(curr_name) == company_id:
                if curr_name not in company_name_variations:  # Avoid duplicates
                    company_name_variations.append(curr_name)
                if company_name is None:
                    company_name = curr_name

        if not company_name:
            # Try a more flexible match
            for company in company_query:
                curr_name = company["company_name"]
                if curr_name and (
                    company_id in normalize(curr_name)
                    or normalize(curr_name) in company_id
                ):
                    if curr_name not in company_name_variations:  # Avoid duplicates
                        company_name_variations.append(curr_name)
                    if company_name is None:
                        company_name = curr_name

        if not company_name:
            return HttpResponse(
                f"<div class='alert alert-warning'>Company not found: {company_id}</div>"
            )

        logger.info(f"Found company name variations for {company_id}: {company_name_variations}")

        # Build a more flexible query for components
        query = Q(company_name__in=company_name_variations)
        
        # Log query parameters for debugging
        logger.critical(f"QUERY PARAMS: company_names={company_name_variations}, delivery_year='{year}', auction_name='{auction_name}'")

        # Add year filter with flexible matching
        if year:
            # Handle multiple year formats
            year_query = Q(delivery_year=year)
            year_query |= Q(delivery_year__icontains=year)
            # Also try to match just the first part of a year range (e.g., 2026 in "2026-27")
            import re

            year_number_match = re.search(r"(\d{4})", year)
            if year_number_match:
                year_number = year_number_match.group(1)
                year_query |= Q(delivery_year__icontains=year_number)
                logger.info(f"Added Year filter: {year_number}")

            query &= year_query

        # Add auction filter with flexible matching
        strict_query = Q()  # More specific query for exact matching
        if auction_name:
            # The auction name might be stored differently, try several variations
            auction_query = Q(auction_name=auction_name)
            auction_query |= Q(auction_name__icontains=auction_name)

            # Handle T-4/T-1 variations
            if "T-4" in auction_name or "T4" in auction_name:
                auction_query |= Q(auction_name__icontains="T-4") 
                auction_query |= Q(auction_name__icontains="(T-4)")
                logger.info(f"Added T-number filter: T-4")
                strict_query |= Q(auction_name__icontains="T-4") | Q(auction_name__icontains="(T-4)")
            elif "T-1" in auction_name or "T1" in auction_name:
                auction_query |= Q(auction_name__icontains="T1")
                auction_query |= Q(auction_name__icontains="T 1")
                logger.info(f"Added T-number filter: T-1")
                strict_query |= Q(auction_name__icontains="T-1") | Q(auction_name__icontains="(T-1)")

            # Match year part of auction name
            year_in_auction = re.search(r"(\d{4})[/-]?(\d{2})?", auction_name)
            if year_in_auction:
                year_part = year_in_auction.group(0)
                auction_query |= Q(auction_name__icontains=year_part)
                # For strict query - add year filter
                year_number = re.search(r"(\d{4})", auction_name)
                if year_number:
                    strict_query &= Q(auction_name__icontains=year_number.group(1))
                    logger.info(f"Added Year filter: {year_number.group(1)}")

            query &= auction_query

        # Combine with strict filter if we have one
        if strict_query:
            specific_query = Q(company_name__in=company_name_variations) & strict_query
            logger.critical(f"FINAL QUERY FILTER (Strict): {specific_query}")
            # Try the more specific query first
            components = Component.objects.filter(specific_query)
            if components.exists():
                logger.info(f"Specific query found {components.count()} components.")
            else:
                # Fall back to broader query if specific query finds nothing
                logger.info("Specific query found no components, using broader query.")
                components = Component.objects.filter(query)
        else:
            # Use broader query if no strict criteria
            components = Component.objects.filter(query)

        # Get all unique CMU IDs
        cmu_ids = components.values("cmu_id").distinct()

        logger.info(
            f"Found {components.count()} components across {cmu_ids.count()} CMU IDs"
        )

        if components.count() == 0:
            return HttpResponse(
                f"""
                <div class='alert alert-warning'>
                    <p>No components found matching these criteria:</p>
                    <ul>
                        <li>Company: {company_name}</li>
                        <li>Year: {year}</li>
                        <li>Auction: {auction_name}</li>
                    </ul>
                    <p>This could be due to differences in how the data is stored in the database.</p>
                </div>
            """
            )

        # Create a structure to organize the data
        cmu_data = {}
        for cmu_id_obj in cmu_ids:
            cmu_id = cmu_id_obj["cmu_id"]
            if not cmu_id:
                continue
                
            # Get components for this CMU ID
            cmu_components = components.filter(cmu_id=cmu_id)
            
            # Track unique location + description combinations
            location_keys = {}
            
            # Prepare location data for this CMU
            locations_data = []
            for component in cmu_components:
                location = component.location
                if not location:
                    continue
                    
                description = component.description or "No description"
                tech = component.technology or ""
                
                # Create a unique key for this location+description
                location_key = f"{location}|{description}|{tech}"
                
                if location_key not in location_keys:
                    # This is a new unique location + description combination
                    location_keys[location_key] = {
                        "location": location,
                        "component_id": f"{cmu_id}_{normalize(location)}",
                        "components": [],
                        "count": 0
                    }
                
                # Add this component to the appropriate location group
                location_keys[location_key]["components"].append({
                    "description": description,
                    "technology": tech,
                    "derated_capacity": component.derated_capacity_mw
                })
                location_keys[location_key]["count"] += 1
            
            # Convert the dict to a list for the template
            for location_data in location_keys.values():
                locations_data.append(location_data)
            
            # Store this CMU's data
            cmu_data[cmu_id] = {
                "cmu_id": cmu_id,
                "component_count": cmu_components.count(),
                "locations": locations_data
            }
        
        logger.info(f"Organized {components.count()} components into {len(cmu_data)} CMU groups for template.")

        # Generate HTML with a list format, not cards
        html = f"<div class='component-results mb-3'>"
        html += f"<div class='alert alert-info'>Found {components.count()} components across {cmu_ids.count()} CMU IDs</div>"

        # Display all CMUs in a list format
        for cmu_id, cmu_info in sorted(cmu_data.items()):
            cmu_detail_url = reverse('cmu_detail', kwargs={'cmu_id': cmu_id})
            
            html += f"""
            <div class="mb-3">
                <h5 class="d-flex justify-content-between align-items-center">
                    <span>CMU ID: <a href="{cmu_detail_url}" class="link-primary">{cmu_id}</a></span>
                    <span class="badge bg-secondary">{cmu_info['component_count']} Component Records</span>
                </h5>
                <div class="ms-3">
                    <p><strong>Components:</strong> {cmu_info['component_count']}</p>
            """
            
            # Add locations
            if cmu_info['locations']:
                html += "<ul class='list-unstyled'>"
                
                for location_data in sorted(cmu_info['locations'], key=lambda x: x['location']):
                    location = location_data['location']
                    component_id = location_data['component_id']
                    components_count = location_data['count']
                    
                    # Format location as a search link instead of direct component link
                    import urllib.parse
                    encoded_location = urllib.parse.quote(location)
                    location_html = f'<a href="/?q={encoded_location}" style="color: blue; text-decoration: underline;">{location}</a>'
                    
                    html += f"""
                    <li class="mb-2">
                        <strong>{location_html}</strong> <span class="text-muted">({components_count} components)</span>
                        <ul class="ms-3">
                    """
                    
                    # Add components with technology and capacity badges
                    for component in location_data['components']:
                        desc = component['description']
                        tech = component['technology']
                        capacity = component.get('derated_capacity')
                        
                        # Create badges for technology and capacity
                        tech_badge = f'<span class="badge bg-info me-1">{tech}</span>' if tech else ''
                        capacity_badge = ''
                        if capacity:
                            try:
                                capacity_value = float(capacity)
                                capacity_badge = f'<span class="badge bg-success me-1">{capacity_value} MW</span>'
                            except (ValueError, TypeError):
                                pass
                        
                        html += f"""
                        <li>
                            {tech_badge} {capacity_badge} <i>{desc}</i>
                        </li>
                        """
                    
                    html += """
                        </ul>
                    </li>
                    """
                
                html += "</ul>"
            else:
                html += "<p>No location information available</p>"
            
            html += """
                </div>
            </div>
            <hr>
            """
        
        html += "</div>"
        
        start_time = request.META.get('HTTP_REQUEST_START_TIME', 0)
        if start_time:
            import time
            elapsed = time.time() - float(start_time)
            logger.info(f"HTMX auction components request for '{company_id}/{year}/{auction_name}' took {elapsed:.2f}s")

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

            year_matches = re.findall(r"20\d\d", year_str)
            if year_matches:
                return int(year_matches[0])

            # Look for year ranges like "2024/25" or "2024-25"
            range_matches = re.findall(r"(20\d\d)[/-]\d\d", year_str)
            if range_matches:
                return int(range_matches[0])

            # Last try: just convert any numbers
            numeric_matches = re.findall(r"\d+", year_str)
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

        filtered_components = _filter_components_by_year_auction(
            components, year, auction_name
        )
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
    Filter components by year and auction name with strict matching for auction types.
    """
    import logging

    logger = logging.getLogger(__name__)

    # If no auction name specified, return all components
    if not auction_name:
        return components

    # Extract auction type from auction name
    auction_type = None
    if "T-1" in auction_name or "T1" in auction_name:
        auction_type = "T-1"
    elif "T-3" in auction_name or "T3" in auction_name:
        auction_type = "T-3"
    elif "T-4" in auction_name or "T4" in auction_name:
        auction_type = "T-4"
    elif "TR" in auction_name:
        auction_type = "TR"

    logger.info(
        f"Filtering {len(components)} components for year={year}, auction_type={auction_type}"
    )

    filtered_components = []
    for comp in components:
        # Ensure we have string values to work with
        comp_year = (
            str(comp.get("Delivery Year", ""))
            if comp.get("Delivery Year") is not None
            else ""
        )
        comp_auction = (
            str(comp.get("Auction Name", ""))
            if comp.get("Auction Name") is not None
            else ""
        )

        # First match by year
        if not year or year in comp_year:
            # Then strictly match by auction type - this is the key part!
            if auction_type:
                # Only include components with this specific auction type
                if auction_type == "T-1" and (
                    "T-1" in comp_auction or "T1" in comp_auction
                ):
                    filtered_components.append(comp)
                elif auction_type == "T-3" and (
                    "T-3" in comp_auction or "T3" in comp_auction
                ):
                    filtered_components.append(comp)
                elif auction_type == "T-4" and (
                    "T-4" in comp_auction or "T4" in comp_auction
                ):
                    filtered_components.append(comp)
                elif auction_type == "TR" and "TR" in comp_auction:
                    filtered_components.append(comp)
            else:
                # If no auction type extracted, include all components for this year
                filtered_components.append(comp)

    logger.info(f"Filtered to {len(filtered_components)} components")
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
    return render_to_string(
        "checker/components/cmu_card.html",
        {
            "cmu_id": cmu_id,
            "components": components,
            "component_debug": component_debug,
            "auction_info": auction_info,
            "location_html": location_html,
            "company_name": company_name,
        },
    )


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
    """Displays details for a specific company, including years, auctions, and potentially components."""
    from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
    from ..models import Component
    from django.db.models import Sum  # Import Sum

    logger.info(f"Company detail page requested for company_id: {company_id}")

    # Determine view mode and sort order from GET parameters
    view_mode = request.GET.get("view_mode", "all_components")
    sort_field = request.GET.get(
        "sort_by", "location"  # Default sort changed to location
    )
    # Check if sort order was explicitly provided in the request
    sort_order_provided = "sort" in request.GET 
    # Get sort order, default to 'asc' initially
    sort_order = request.GET.get("sort", "asc")  
    page = request.GET.get("page", 1)
    per_page = 50  # Components per page for capacity and all_components views

    # Override default sort order to 'desc' for year_auction view if not provided
    if view_mode == 'year_auction' and not sort_order_provided:
        sort_order = 'desc'

    # Validate view_mode
    if view_mode not in ["year_auction", "capacity", "all_components"]:
        view_mode = "all_components"

    # Validate sort_order (apply default based on view_mode if invalid)
    if sort_order not in ["asc", "desc"]:
        sort_order = 'desc' if view_mode == 'year_auction' else 'asc' 

    # Validate sort_field for 'all_components' view
    allowed_sort_fields = [
        "delivery_year",
        "auction_name",
        "derated_capacity_mw",
        "location",
    ]
    if view_mode == "all_components" and sort_field not in allowed_sort_fields:
        sort_field = "location"  # Default if invalid changed to location

    context = {
        "company_id": company_id,
        "company_name": None,  # Will be populated later
        "view_mode": view_mode,
        "sort_field": sort_field,  # Add sort_field to context
        "sort_order": sort_order,
        "error": None,
        "year_auction_data": [],
        "page_obj": None,
        "paginator": None,
        "total_count": 0,
    }

    start_time = time.time()

    # Find all company name variations based on the normalized company_id
    # (This logic might need adjustment if company_id isn't the normalized name)
    all_db_company_names = Component.objects.values_list(
        "company_name", flat=True
    ).distinct()
    company_name_variations = []
    primary_company_name = None
    for name in all_db_company_names:
        if name and normalize(name) == company_id:
            company_name_variations.append(name)
            if primary_company_name is None:
                primary_company_name = name

    if not company_name_variations:
        context["error"] = (
            f"Company variations not found for ID '{company_id}'. Cannot load details."
        )
        return render(request, "checker/company_detail.html", context)

    context["company_name"] = primary_company_name  # Set the display name

    # --- Data Fetching based on view_mode ---
    try:
        if view_mode == "year_auction":
            # Existing logic for year/auction view
            company_records = (
                Component.objects.filter(company_name__in=company_name_variations)
                .values(
                    "delivery_year",
                    "auction_name",  # Removed auction_type as it's not a direct field
                )
                .distinct()
            )
            # Convert queryset to DataFrame for processing - might be inefficient for large sets
            # Consider direct DB aggregation if performance is an issue
            if company_records.exists():
                df = pd.DataFrame(list(company_records))
                
                # Get the raw data and pass the sort_order parameter
                year_data = _organize_year_data(df, sort_order)
                
                # year_data is already formatted for the template
                # It's a list of dicts with year, year_id, auctions, and auctions_display
                context["year_auction_data"] = year_data
            else:
                context["year_auction_data"] = []

        elif view_mode == "capacity":
            # Existing logic for capacity view
            components_query = Component.objects.filter(
                company_name__in=company_name_variations
            ).exclude(derated_capacity_mw__isnull=True)

            # Apply sorting
            order_direction = "-" if sort_order == "desc" else ""
            components_query = components_query.order_by(
                f"{order_direction}derated_capacity_mw", "delivery_year"
            )

            total_count = components_query.count()
            paginator = Paginator(components_query, per_page)
            try:
                page_obj = paginator.page(page)
            except PageNotAnInteger:
                page_obj = paginator.page(1)
            except EmptyPage:
                page_obj = paginator.page(paginator.num_pages)

            context["page_obj"] = page_obj
            context["paginator"] = paginator
            context["total_count"] = total_count

        elif view_mode == "all_components":
            # NEW Logic for all_components view
            components_query = Component.objects.filter(
                company_name__in=company_name_variations
            )

            # Apply sorting based on sort_field and sort_order
            order_direction = "-" if sort_order == "desc" else ""
            # Add nulls_last/nulls_first if needed depending on DB and field type
            if sort_field:
                components_query = components_query.order_by(
                    f"{order_direction}{sort_field}"
                )

            total_count = components_query.count()
            paginator = Paginator(components_query, per_page)
            try:
                page_obj = paginator.page(page)
            except PageNotAnInteger:
                page_obj = paginator.page(1)
            except EmptyPage:
                page_obj = paginator.page(paginator.num_pages)

            context["page_obj"] = page_obj
            context["paginator"] = paginator
            context["total_count"] = total_count

    except Exception as e:
        logger.error(
            f"Error fetching data for company '{company_id}', view '{view_mode}': {e}"
        )
        logger.error(traceback.format_exc())
        context["error"] = f"An error occurred while loading data: {e}"
        # Optionally add traceback to context if debug is enabled
        if request.GET.get("debug"):
            context["traceback"] = traceback.format_exc()

    # Common context updates
    context["api_time"] = time.time() - start_time
    return render(request, "checker/company_detail.html", context)


def cmu_detail(request, cmu_id):
    """Displays details for a specific CMU ID, including components."""
    from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
    from ..models import Component
    import logging
    import time
    import traceback

    logger = logging.getLogger(__name__)
    logger.info(f"CMU detail page requested for cmu_id: {cmu_id}")

    # Get sorting parameters
    sort_field = request.GET.get("sort_by", "location") # Default to location
    sort_order = request.GET.get("sort", "asc")
    page = request.GET.get("page", 1)
    per_page = 50 # Components per page

    # Validate sort_order
    if sort_order not in ["asc", "desc"]:
        sort_order = "asc"

    # Validate sort_field
    allowed_sort_fields = ["location", "delivery_year"]
    if sort_field not in allowed_sort_fields:
        sort_field = "location"

    context = {
        "cmu_id": cmu_id,
        "sort_field": sort_field,
        "sort_order": sort_order,
        "error": None,
        "page_obj": None,
        "paginator": None,
        "total_count": 0,
    }

    start_time = time.time()

    try:
        # Fetch components for the given CMU ID
        components_query = Component.objects.filter(cmu_id__iexact=cmu_id)

        # Apply sorting
        order_direction = "-" if sort_order == "desc" else ""
        # Add nulls_last/nulls_first depending on DB and field type if needed
        # For basic text/year fields, this should be okay
        components_query = components_query.order_by(
            f"{order_direction}{sort_field}"
        )

        total_count = components_query.count()
        paginator = Paginator(components_query, per_page)
        try:
            page_obj = paginator.page(page)
        except PageNotAnInteger:
            page_obj = paginator.page(1)
        except EmptyPage:
            page_obj = paginator.page(paginator.num_pages)

        context["page_obj"] = page_obj
        context["paginator"] = paginator
        context["total_count"] = total_count

    except Exception as e:
        logger.error(
            f"Error fetching data for CMU ID '{cmu_id}': {e}"
        )
        logger.error(traceback.format_exc())
        context["error"] = f"An error occurred while loading data: {e}"
        if request.GET.get("debug"):
            context["traceback"] = traceback.format_exc()

    context["api_time"] = time.time() - start_time
    return render(request, "checker/cmu_detail.html", context)


def _organize_year_data(company_records, sort_order):
    """
    Organize year data for a company.
    Returns a list of year objects with auctions.
    """
    # --- DEBUG: Log input DataFrame ---
    logger.debug(f"_organize_year_data: Input DF shape: {company_records.shape}")
    try:
        logger.debug(f"_organize_year_data: Column names: {list(company_records.columns)}")
        logger.debug(f"_organize_year_data: Input DF head:\n{company_records.head().to_string()}")
    except Exception as e: 
        logger.debug(f"_organize_year_data: Could not log input DF head: {str(e)}")
    # --- END DEBUG ---

    year_auctions = {}
    for index, row in company_records.iterrows():
        # --- FIX: Handle both Title Case and lowercase field names ---
        year = str(row.get("Delivery Year", row.get("delivery_year", ""))) # Check both
        auction = str(row.get("Auction Name", row.get("auction_name", ""))) # Check both
        # --- END FIX ---
        # --- DEBUG: Log extracted values ---
        logger.debug(f"_organize_year_data: Row {index} - Extracted Year: '{year}', Auction: '{auction}'")
        # --- END DEBUG ---
        if not year or year == "nan":
            logger.debug(f"_organize_year_data: Row {index} - Skipping due to invalid year.")
            continue

        if year not in year_auctions:
            year_auctions[year] = {}

        # Use correct DataFrame column names
        # auction = str(row.get("auction_name", "")) # Already got auction above
        if auction and auction != "nan":
            year_auctions[year][auction] = True
            logger.debug(f"_organize_year_data: Row {index} - Added Auction '{auction}' for Year '{year}'")
        else:
            logger.debug(f"_organize_year_data: Row {index} - Skipping auction due to invalid value.")

    # --- DEBUG: Log intermediate dictionary ---
    logger.debug(f"_organize_year_data: Built year_auctions dict: {year_auctions}")
    # --- END DEBUG ---

    # Convert to list of year objects
    year_data = []
    processed_years = set()  # Track processed years to avoid duplicates
    
    for year, auctions in year_auctions.items():
        # Skip if we've already processed this year (normalized)
        normalized_year = year.strip().lower()
        if normalized_year in processed_years:
            continue
        processed_years.add(normalized_year)
        
        year_id = f"year-{year.replace(' ', '-').lower()}"

        # Create auctions_display list with required tuple format
        auctions_display = []
        processed_auctions = set()  # Track processed auctions to avoid duplicates
        
        for auction_name in auctions.keys():
            # Skip duplicate auctions (case-insensitive)
            normalized_auction = auction_name.strip().lower()
            if normalized_auction in processed_auctions:
                continue
            processed_auctions.add(normalized_auction)
            
            # Generate a unique and VALID ID for this auction
            sanitized_auction_name = re.sub(
                r"[^a-zA-Z0-9-]+", "-", normalize(auction_name)
            )  # Replace invalid chars with hyphen
            sanitized_year = re.sub(
                r"[^a-zA-Z0-9-]+", "-", year.replace(" ", "-").lower()
            )
            auction_id = f"auction-{sanitized_auction_name}-{sanitized_year}"  # Combine sanitized parts
            # Ensure ID doesn't start/end with hyphen (optional but good practice)
            auction_id = auction_id.strip("-")

            # Determine auction type and badge class
            auction_type = "Unknown"
            badge_class = "bg-secondary"  # Default

            if "T-1" in auction_name or "T1" in auction_name:
                auction_type = "T-1"
                badge_class = "bg-info"
            elif "T-3" in auction_name or "T3" in auction_name:
                auction_type = "T-3"
                badge_class = "bg-primary"
            elif "T-4" in auction_name or "T4" in auction_name:
                auction_type = "T-4"
                badge_class = "bg-success"
            elif "TR" in auction_name:
                auction_type = "TR"
                badge_class = "bg-warning"

            # Add to display list as a tuple
            auctions_display.append(
                (auction_name, auction_id, badge_class, auction_type)
            )

        year_data.append(
            {
                "year": year,
                "year_id": year_id,
                "auctions": list(auctions.keys()),
                "auctions_display": auctions_display,  # Add the formatted display data
            }
        )

    # Sort by year
    ascending = sort_order == "asc"
    year_data.sort(key=lambda x: try_parse_year(x["year"]), reverse=not ascending)

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
        logger.info(
            f"Using cached components for {cmu_id}, found {len(cached_components)}"
        )
        return cached_components, 0

    # Check if we have the data in our JSON file - CASE-INSENSITIVE
    logger.info(f"Checking JSON for components for CMU ID: {cmu_id}")
    json_components = get_component_data_from_json(cmu_id)
    if json_components is not None:
        logger.info(
            f"Using JSON-stored components for {cmu_id}, found {len(json_components)}"
        )
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
                "sort": "Delivery Year desc",
            }

            logger.info(f"Making API request for CMU ID: {cmu_id}, offset: {offset}")
            response = requests.get(base_url, params=params, timeout=60)
            response.raise_for_status()

            data = response.json()
            if data.get("success"):
                records = data.get("result", {}).get("records", [])
                logger.info(
                    f"API returned {len(records)} records for {cmu_id} at offset {offset}"
                )

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
                logger.error(
                    f"API request unsuccessful for {cmu_id}: {data.get('error', 'Unknown error')}"
                )
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


def find_candidate_companies(query):
    """Find candidate companies for query using the CMU dataframe"""
    logger = logging.getLogger(__name__)
    start_time = time.time()
    empty_df = pd.DataFrame(columns=['Full Name', 'CMU ID', 'match_score'])
    
    try:
        # Get CMU dataframe
        cmu_df, load_time = get_cmu_dataframe()
        if cmu_df is None:
            logger.error(f"Failed to load CMU dataframe for query: '{query}'")
            return empty_df, time.time() - start_time
            
        # Use fuzzy search to find matching companies
        matches_df = _perform_company_search(cmu_df, query)
        api_time = time.time() - start_time
        
        if matches_df.empty:
            logger.info(f"No matching companies found via fuzzy search for: '{query}'")
        else:
            logger.info(f"Found {len(matches_df)} candidate companies for '{query}' in {api_time:.4f}s")
            
        return matches_df, api_time
    except Exception as e:
        logger.error(f"Error finding candidate companies for '{query}': {e}")
        return empty_df, time.time() - start_time


# Add missing compatibility function at the end of the file
def find_company_matches(query, cmu_df):
    """
    Compatibility function to maintain backward compatibility with component_search.py
    This wraps the _perform_company_search function with the same interface.
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"find_company_matches called for query: '{query}'")
    try:
        # Simply delegate to the existing function
        return _perform_company_search(cmu_df, query)
    except Exception as e:
        logger.error(f"Error in find_company_matches: {e}")
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=cmu_df.columns)
