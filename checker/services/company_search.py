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
    sort_order = request.GET.get("sort", "desc")
    note = None # Initialize note

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
    comp_sort_order = 'desc'
    search_method = "Initial"

    # Process GET requests
    if request.method == "GET":
        if query:
            start_time = time.time()
            search_method = "Hybrid DB Search"
            try:
                # --- DB Query for Candidates --- 
                db_limit = 50 # Limit initial candidates from DB
                db_candidate_companies = []
                
                # Try istartswith first (faster)
                logger.debug(f"Attempting DB istartswith query for '{query}'")
                db_candidate_companies_qs = Component.objects.filter(
                    company_name__istartswith=query
                ).values_list('company_name', flat=True).distinct()[:db_limit]
                db_candidate_companies = list(db_candidate_companies_qs)
                logger.info(f"DB istartswith found {len(db_candidate_companies)} candidates.")

                # If startswith yields no results, fallback to icontains
                if not db_candidate_companies:
                    logger.debug(f"No results via istartswith, falling back to icontains for '{query}'")
                    db_candidate_companies_qs = Component.objects.filter(
                        company_name__icontains=query
                    ).values_list('company_name', flat=True).distinct()[:db_limit]
                    db_candidate_companies = list(db_candidate_companies_qs)
                    logger.info(f"DB icontains found {len(db_candidate_companies)} candidates.")
                    
                # --- Fuzzy Search for Candidates --- 
                fuzzy_candidate_companies = []
                cmu_df = None # Initialize cmu_df
                df_api_time = 0
                try:
                    cmu_df, df_api_time = get_cmu_dataframe()
                    api_time += df_api_time
                    if cmu_df is not None:
                        # Limit the number of fuzzy matches considered initially
                        # Note: _perform_company_search already returns sorted by score
                        fuzzy_limit = 50 
                        fuzzy_matches_df = _perform_company_search(cmu_df, query)
                        if not fuzzy_matches_df.empty:
                            fuzzy_candidate_companies = fuzzy_matches_df['Full Name'].unique().tolist()[:fuzzy_limit]
                            logger.info(f"Fuzzy search (_perform_company_search) found {len(fuzzy_candidate_companies)} unique candidates (limited to {fuzzy_limit}).")
                        else:
                            logger.error("CMU DataFrame not available for fuzzy search.")
                except Exception as fuzzy_e:
                    logger.error(f"Error during fuzzy company search step: {fuzzy_e}")
                
                # --- Combine and De-duplicate Candidates --- 
                combined_candidates = set(db_candidate_companies) | set(fuzzy_candidate_companies)
                # Remove None or empty strings just in case
                combined_candidates = {c for c in combined_candidates if c}
                logger.info(f"Combined DB ({len(db_candidate_companies)}) and Fuzzy ({len(fuzzy_candidate_companies)}) candidates, resulting in {len(combined_candidates)} unique candidates.")

                # --- Score, Sort, and Limit the Combined Candidates --- 
                if combined_candidates:
                    scorer = fuzz.token_set_ratio
                    name_score_list = []
                    for name in combined_candidates:
                        # No need to check for empty name here due to set comprehension above
                        try:
                            score = scorer(query, name)
                            name_score_list.append((name, score))
                        except Exception as score_e:
                            logger.error(f"Error scoring company name '{name}': {score_e}")
                            name_score_list.append((name, 0))

                    name_score_list.sort(key=lambda item: item[1], reverse=True)
                    logger.debug(f"Scored combined candidates (Top 10): {name_score_list[:10]}...")

                    display_limit = 20 # Limit final display
                    unique_top_companies = [name for name, score in name_score_list[:display_limit]]
                    logger.info(f"Processing top {len(unique_top_companies)} companies after combining, scoring and limiting.")

                    # --- Build links for the top N --- 
                    if unique_top_companies:
                        try:
                            # Ensure cmu_df is loaded if it wasn't during fuzzy search or failed
                            if cmu_df is None:
                                logger.debug("CMU DataFrame was not loaded previously or failed, attempting to load now...")
                                cmu_df, df_api_time = get_cmu_dataframe()
                                api_time += df_api_time
                                if cmu_df is None:
                                    logger.error("Failed to load CMU DataFrame for building links.")
                                    # Skip building links if DF is unavailable
                                    raise ValueError("CMU DataFrame could not be loaded.") 
                                
                            # Proceed only if cmu_df is available
                            logger.debug(f"Building links for {len(unique_top_companies)} companies using loaded CMU DataFrame.")
                            results_dict, render_time_links_ = _build_search_results(
                                cmu_df, unique_top_companies, sort_order, query, cmu_limit=3, add_debug_info=True
                            )
                            company_links = results_dict.get(query, [])
                            company_link_count = len(company_links)
                            render_time_links += render_time_links_
                            logger.info(f"_build_search_results generated {company_link_count} company links.")
                            
                        except Exception as build_e:
                             logger.error(f"Error building company links: {build_e}")
                             # Reset links if building fails
                             company_links = []
                             company_link_count = 0
                    else:
                        # This case handles when unique_top_companies is empty after scoring/limiting
                        logger.info("No companies selected after scoring/limiting combined candidates.")
                        company_links = []
                        company_link_count = 0
                else:
                    # This case handles when combined_candidates is empty initially
                    logger.warning("No candidate companies found via combined DB and Fuzzy searches.")
                    company_links = []
                    company_link_count = 0

                logger.info(f"Company links final count before component search: {company_link_count}") # Log final count
                # --- END OF STEP 1 (Company Links) ---
                
                # --- 2. Find Matching Components (Broad & Paginated) ---
                component_query_filter = Q()
                component_terms = query.split()
                for term in component_terms:
                    if len(term) >= 2:
                        term_filter = (
                            Q(location__icontains=term) | Q(description__icontains=term) |
                            Q(technology__icontains=term) | Q(company_name__icontains=term) |
                            Q(cmu_id__icontains=term)
                        )
                        component_query_filter |= term_filter
                if query: component_query_filter |= Q(cmu_id__iexact=query)

                if not component_query_filter:
                     logger.warning("Component query filter is empty. No components searched.")
                     all_components = Component.objects.none()
                     component_count = 0
                else:
                    logger.warning(f"Attempting component query with filter: {component_query_filter}")
                    comp_sort_prefix = '-' if comp_sort_order == 'desc' else ''
                    comp_django_sort_field = f'{comp_sort_prefix}delivery_year'
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
    min_score = 70
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
    logger.debug(f"Fuzzy matches list found (score >= {min_score}): {matches_list}")

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
    results = {query: []}
    company_count = len(unique_companies)
    processed_normalized_names = set() # --- ADDED: Track processed normalized names ---
    start_render_time = time.time() # Start timing here

    # --- FIX: Return early if no companies were found ---
    if company_count == 0:
        logger.warning(f"No unique companies provided to _build_search_results for query '{query}'. Returning empty results.")
        return results, 0.0 # Match return signature (results, render_time)
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
            company_html = f'<a href="/company/{normalized_company_id}/" style="color: blue; text-decoration: underline;">{company}</a>'

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
                <strong>{company_html}</strong>
                <div class="mt-1 mb-1"><span class="text-muted">CMU IDs: {cmu_ids_str}</span></div>
                <div>{component_info}</div>
            </div>
            """

            results[query].append(company_html_with_details)

    # Calculate total render time
    render_time = time.time() - start_render_time

    if add_debug_info:
        logger.debug(f"_build_search_results Debug Info: {debug_info}")
    
    logger.info(f"_build_search_results generated {len(results[query])} links for {company_count} initial companies in {render_time:.4f}s.")
    return results, render_time


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

    logger = logging.getLogger(__name__)
    logger.info(
        f"Loading auction components: company_id={company_id}, year={year}, auction={auction_name}"
    )

    # Convert URL parameters from underscores back to spaces
    year = from_url_param(year)
    auction_name = from_url_param(auction_name)

    try:
        # Find company name
        company_query = Component.objects.values("company_name").distinct()
        company_name = None

        for company in company_query:
            curr_name = company["company_name"]
            if curr_name and normalize(curr_name) == company_id:
                company_name = curr_name
                break

        if not company_name:
            # Try a more flexible match
            for company in company_query:
                curr_name = company["company_name"]
                if curr_name and (
                    company_id in normalize(curr_name)
                    or normalize(curr_name) in company_id
                ):
                    company_name = curr_name
                    break

        if not company_name:
            return HttpResponse(
                f"<div class='alert alert-warning'>Company not found: {company_id}</div>"
            )

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

            year_number_match = re.search(r"(\d{4})", year)
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
            year_in_auction = re.search(r"(\d{4})[/-]?(\d{2})?", auction_name)
            if year_in_auction:
                year_part = year_in_auction.group(0)
                auction_query |= Q(auction_name__icontains=year_part)

            query &= auction_query

        # Get matching components
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

        # Generate HTML with the matching components
        html = f"<div class='component-results mb-3'>"
        html += f"<div class='alert alert-info'>Found {components.count()} components across {cmu_ids.count()} CMU IDs</div>"

        # Group by CMU ID
        html += "<div class='row'>"

        for cmu_id_obj in cmu_ids:
            cmu_id = cmu_id_obj["cmu_id"]
            if not cmu_id:
                continue

            # Get components for this CMU ID
            cmu_components = components.filter(cmu_id=cmu_id)

            # Get all locations for this CMU
            locations = cmu_components.values("location").distinct()

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
                    location = location_obj["location"]
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
    view_mode = request.GET.get("view_mode", "year_auction")
    sort_field = request.GET.get(
        "sort_by", "location"  # Default sort changed to location
    )
    sort_order = request.GET.get("sort", "asc")  # Default sort order changed to asc (displays Desc)
    page = request.GET.get("page", 1)
    per_page = 50  # Components per page for capacity and all_components views

    # Validate view_mode
    if view_mode not in ["year_auction", "capacity", "all_components"]:
        view_mode = "year_auction"

    # Validate sort_order (remains the same)
    if sort_order not in ["asc", "desc"]:
        sort_order = "asc" # Default changed to asc

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
                context["year_auction_data"] = _organize_year_data(df, sort_order)
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


def _organize_year_data(company_records, sort_order):
    """
    Organize year data for a company.
    Returns a list of year objects with auctions.
    """
    # --- DEBUG: Log input DataFrame ---
    logger.debug(f"_organize_year_data: Input DF shape: {company_records.shape}")
    try:
        logger.debug(f"_organize_year_data: Input DF head:\n{company_records.head().to_string()}")
    except Exception: logger.debug("_organize_year_data: Could not log input DF head")
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
    for year, auctions in year_auctions.items():
        year_id = f"year-{year.replace(' ', '-').lower()}"

        # Create auctions_display list with required tuple format
        auctions_display = []
        for auction_name in auctions.keys():
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


def _build_db_search_results(company_queryset, query):
    """
    Builds formatted HTML links from a Component QuerySet from the direct DB search.
    Groups by company name, fetches component/CMU counts efficiently using annotate,
    and provides a link.
    
    Args:
        company_queryset: The initial queryset matching potential companies based on the query.
    
    Returns:
        Tuple: (list of company HTML links, float build time)
    """
    start_build_time = time.time()
    company_links = []
    limit = 75 # Limit rendering N unique companies (Reduced from 250 for performance)

    # --- Efficiently get counts using annotate --- 
    # 1. Get unique company names from the initial queryset (limited)
    unique_company_names_qs = company_queryset.values_list('company_name', flat=True).distinct()[:limit]
    unique_company_names = list(unique_company_names_qs)
    logger.info(f"Processing {len(unique_company_names)} unique company names (limited to {limit}) for counts...")

    if not unique_company_names:
        return [], 0.0 # No companies found, return early
    
    # logger.warning("_build_db_search_results: About to execute annotate query...") # Removed log
    # 2. Perform ONE query to get counts for these specific company names
    # Use Coalesce to handle potential null cmu_ids gracefully in Count
    company_data = Component.objects.filter(company_name__in=unique_company_names) \
                                    .values('company_name') \
                                    .annotate(
                                        component_count=Count('id'), 
                                        cmu_count=Count(Coalesce('cmu_id', Value('__EMPTY__')), distinct=True) # Count distinct non-null CMU IDs
                                    ) \
                                    .order_by('company_name') # Keep consistent order
    
    # logger.warning(f"_build_db_search_results: Annotate query EXECUTED. Found data for {len(company_data)} companies.") # Removed log
    # Convert annotated data to a dictionary for easy lookup
    company_counts_dict = {item['company_name']: item for item in company_data}
    logger.info(f"Annotated counts retrieved for {len(company_counts_dict)} companies.")
    # --- End of efficient count retrieval --- 

    # Calculate scores
    scorer = fuzz.token_set_ratio
    name_score_list = []
    for name in unique_company_names:
        if not name: continue
        # Score the original query against the *actual* company name
        score = scorer(query, name) 
        name_score_list.append((name, score))
    
    # Sort by score, descending
    name_score_list.sort(key=lambda item: item[1], reverse=True)
    
    # Get the sorted list of names (NO FILTERING HERE)
    sorted_unique_names = [name for name, score in name_score_list]
    
    logger.debug(f"_build_db_search_results: Names sorted by score: {sorted_unique_names[:10]}...")

    # --- Step 3: Fetch Counts and Build HTML --- 
    # Fetch counts efficiently for the potentially numerous sorted names
    # Limit the names passed to the __in filter if necessary, though the initial limit helps
    names_to_fetch_counts_for = sorted_unique_names # Use the full sorted list for count fetching
    if not names_to_fetch_counts_for:
        logger.warning("_build_db_search_results: No company names found after initial query and sort.")
        return [], 0.0
    
    logger.info(f"Fetching counts for {len(names_to_fetch_counts_for)} sorted companies...")
    company_data = Component.objects.filter(company_name__in=names_to_fetch_counts_for) \
                                    .values('company_name') \
                                    .annotate(
                                        component_count=Count('id'), 
                                        cmu_count=Count(Coalesce('cmu_id', Value('__EMPTY__')), distinct=True)
                                    ) \
                                    .order_by() # Remove default ordering for efficiency
                                    
    company_counts_dict = {item['company_name']: item for item in company_data}
    logger.info(f"Annotated counts retrieved for {len(company_counts_dict)} companies.")

    processed_count = 0
    # Iterate through the SORTED unique names (no threshold filter applied)
    for company_name in sorted_unique_names:
        processed_count += 1
        if not company_name:
            logger.warning(f"Skipping blank company name at index {processed_count-1}")
            continue
        
        # Lookup counts from our annotated dictionary
        counts = company_counts_dict.get(company_name)
        
        if counts:
            component_count = counts.get('component_count', 0)
            # Adjust cmu_count if '__EMPTY__' was counted (means only nulls were present)
            cmu_count_val = counts.get('cmu_count', 0)
            if cmu_count_val == 1 and Component.objects.filter(company_name=company_name, cmu_id='__EMPTY__').exists():
                 cmu_count = 0 # If the only distinct value was our placeholder, it means only nulls
            else:
                 cmu_count = cmu_count_val
            count_text = f"{component_count} components across {cmu_count} CMU IDs"
        else:
            # Should not happen if company_name came from the DB, but handle defensively
            logger.warning(f"Could not find annotated counts for '{company_name}'. Setting counts to 0.")
            count_text = "0 components across 0 CMU IDs"

        try:
            company_id = normalize(company_name)
            company_html = f'<a href="/company/{company_id}/" style="color: blue; text-decoration: underline;">{company_name}</a>'
            company_links.append(f'''
                <div data-company-name="{company_name}">
                    <strong>{company_html}</strong>
                    <div class="mt-1 mb-1"><span class="text-muted">{count_text}</span></div>
                </div>
            ''')
            if processed_count % 50 == 0:
                 logger.info(f"Processed link generation for {processed_count}/{len(unique_company_names)} companies...")
        except Exception as link_e:
            logger.error(f"Error generating link for company '{company_name}': {link_e}")
            continue # Skip this company if link generation fails

    build_time = time.time() - start_build_time
    logger.info(f"Optimized _build_db_search_results generated {len(company_links)} links in {build_time:.4f}s.")
    return company_links, build_time
