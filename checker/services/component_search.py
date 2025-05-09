import urllib.parse
import time
import logging
from django.shortcuts import render
from django.core.cache import cache
import traceback
from django.urls import reverse
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.template.loader import render_to_string
from ..models import Component, CMURegistry
import json
from django.db.models import Q
import re

from ..utils import normalize, get_cache_key
from .data_access import (
    fetch_components_for_cmu_id, 
    get_components_from_database
)
from .company_search import _perform_company_search, get_cmu_dataframe, _build_search_results
from .postcode_helpers import get_location_to_postcodes_mapping, get_all_postcodes_for_area

logger = logging.getLogger(__name__)


def search_components_service(request, extra_context=None, return_data_only=False):
    """Service function for searching companies AND components in a unified interface."""
    # --- PERFORMANCE TIMING --- START ---
    perf_timings = {}
    overall_start_time = time.time()
    perf_checkpoint = time.time()
    # --- PERFORMANCE TIMING --- END ---
    
    # --- REMOVE LOG ENTRY POINT ---
    # --- REMOVED TEMPORARY CACHE CLEAR ---
    start_time = time.time() # Start timer earlier
    # Get query and pagination parameters
    query = request.GET.get("q", "").strip()
    page = int(request.GET.get("page", 1))
    default_per_page = 10
    try:
        per_page = int(request.GET.get('per_page', str(default_per_page)))
        if per_page <= 0: per_page = default_per_page
    except ValueError:
        per_page = default_per_page
        
    # --- REMOVE TEMPORARY LOGGING --- START ---
    sort_by = request.GET.get('sort_by', 'relevance').lower()
    sort_order = request.GET.get('sort_order', 'desc').lower()
    if sort_order not in ['asc', 'desc']:
        sort_order = 'desc'
    # logger.critical(f"--- search_components_service RAW PARAMS: ...") # Keep commented or remove
    # --- REMOVE TEMPORARY LOGGING --- END ---
    
    # --- Standardize Sort Parameters --- START ---
    # Parameters are now extracted above
        
    # Map UI sort fields to backend fields if necessary
    valid_sort_fields = {
        'location': 'location',
        'date': 'delivery_year', 
        'relevance': 'relevance'
    }
    backend_sort_by = valid_sort_fields.get(sort_by, 'relevance')
    ui_sort_by = sort_by # Store the original UI sort value
    if backend_sort_by != ui_sort_by:
        logger.warning(f"Service sort_by '{ui_sort_by}' mapped to backend field '{backend_sort_by}'.")
    # --- Standardize Sort Parameters --- END ---
    
    # --- Cache Key Generation (using mapped backend_sort_by) --- START ---
    normalized_query = query.lower().strip() # Keep this for potential non-cache use
    # Use the backend sort field name in the key for consistency with querying
    cache_key = f"search_service_{normalized_query}_p{page}_pp{per_page}_sort{backend_sort_by}_{sort_order}"
    logger.info(f"Generated Cache Key: {cache_key}")
    # --- Cache Key Generation --- END ---
    
    # Initialize context variables
    company_links = []
    # component_results_dict = {} # No longer needed if we pass list directly
    components_list = []
    total_component_count = 0
    error_message = None
    api_time = 0
    note = None
    from_cache = False # Flag for cache hits
    page_obj = None # Initialize page object

    # Debug info - update with new params
    debug_info = {
        "query": query,
        "page": page,
        "per_page": per_page,
        "sort_by": ui_sort_by, # Use original UI value here
        "sort_order": sort_order,
        "backend_sort_by": backend_sort_by,
        "company_search_attempted": False,
        "component_search_attempted": False,
        "final_total_components": 0,
        "final_displayed_components": 0,
        "data_source": "unknown"
    }

    # If no query, render empty search page
    if not query:
        logger.info("Rendering empty search page (no query)")
        # Pass sort defaults and ensure expected context variables are present
        initial_context = {
            "query": query, 
            "sort_by": ui_sort_by, # Use the UI sort parameter
            "sort_order": sort_order, 
            "per_page": per_page,
            "is_initial_load": True, 
            "company_links": [], # Add empty list for company links
            "page_obj": None, # Add None for the paginator object
            "error": None, # Add None for error messages
            "debug_info": debug_info, # Include basic debug info
            "note": None, # Add None for notes
            "total_component_count": 0, # Add zero count
            "from_cache": False # Add from_cache flag
        }
        return render(request, "checker/search.html", initial_context)

    # --- START SEARCH LOGIC (if query exists) --- 
    
    # --- Cache Check (Simplified key using new params) --- START ---
    # cached_context = cache.get(cache_key)
    # 
    # if cached_context:
    #     # --- REMOVE LOGGING FOR CACHE HIT ---
    #     logger.info(f"Using cached context for query '{query}' (page {page}, sort {ui_sort_by} {sort_order})")
    #     cached_context['from_cache'] = True
    #     cached_context['api_time'] = 0
    #     cached_context.setdefault('query', query)
    #     cached_context.setdefault('per_page', per_page)
    #     cached_context.setdefault('sort_by', ui_sort_by)
    #     cached_context.setdefault('sort_order', sort_order)
    #     return render(request, "checker/search.html", cached_context)
    # --- Cache Check --- END ---
    
    # --- PERFORMANCE TIMING: Setup complete ---
    perf_timings['setup'] = time.time() - perf_checkpoint
    perf_checkpoint = time.time()
    # --- END PERFORMANCE TIMING ---
    
    # If not cached, proceed with fetching data
    try:
        # --- STEP 1: Search for matching companies --- 
        debug_info["company_search_attempted"] = True
        try:
            # --- PERFORMANCE TIMING: Start company search ---
            company_search_start = time.time()
            # --- END PERFORMANCE TIMING ---
            
            cmu_df, df_api_time = get_cmu_dataframe()
            api_time += df_api_time
            
            # --- PERFORMANCE TIMING: CMU dataframe load ---
            perf_timings['cmu_df_load'] = time.time() - company_search_start
            company_search_checkpoint = time.time()
            # --- END PERFORMANCE TIMING ---
            
            if cmu_df is not None:
                norm_query = normalize(query)
                matching_records = _perform_company_search(cmu_df, norm_query)
                
                # --- PERFORMANCE TIMING: Company fuzzy search ---
                perf_timings['company_fuzzy_search'] = time.time() - company_search_checkpoint
                company_search_checkpoint = time.time()
                # --- END PERFORMANCE TIMING ---
                
                unique_companies = list(matching_records["Full Name"].unique())[:20] 
                # Note: _build_search_results uses comp_sort internally, might need update later if company sorting is added
                company_data_list, render_time_links = _build_search_results(
                    cmu_df, unique_companies, sort_order, query, cmu_limit=3, add_debug_info=True
                )
                api_time += render_time_links # Add render time
                
                # --- PERFORMANCE TIMING: Company link building ---
                perf_timings['company_link_building'] = time.time() - company_search_checkpoint
                company_search_checkpoint = time.time()
                # --- END PERFORMANCE TIMING ---
                
                logger.info(f"Found {len(company_data_list)} matching company links for '{query}'")
                # Debug: Log the type and content of the returned list
                logger.debug(f"Type of _build_search_results return: {type(company_data_list)}")
                if isinstance(company_data_list, list) and company_data_list:
                    logger.debug(f"First item type in company_data_list: {type(company_data_list[0])}")
                    logger.debug(f"First item content: {str(company_data_list[0])[:200]}") # Log first 200 chars
                
                # Extract HTML for template and CMU IDs for filtering
                company_links = [item['html'] for item in company_data_list]
                company_cmu_id_map = {item['html']: item['cmu_ids'] for item in company_data_list} # Use HTML as key temporarily, might need better approach
                company_link_count = len(company_links)
                render_time_links += render_time_links
                logger.info(f"_build_search_results generated {company_link_count} company links.")
                
                # --- PERFORMANCE TIMING: Total company search ---
                perf_timings['total_company_search'] = time.time() - company_search_start
                # --- END PERFORMANCE TIMING ---
            else:
                error_message = "Error loading company data (CMU dataframe)."
                perf_timings['total_company_search'] = time.time() - company_search_start
        except Exception as e_comp:
            error_message = f"Error searching companies: {str(e_comp)}"
            logger.exception(error_message)
            perf_timings['total_company_search'] = time.time() - perf_checkpoint
        
        perf_checkpoint = time.time()

        # --- STEP 2: Search for components using data_access --- START ---
        debug_info["component_search_attempted"] = True
        
        # --- Determine Search Type (Revised) --- START ---
        search_type_start = time.time()
        
        search_type = "general"  # Default
        
        query_lower = query.lower().strip()
        query_words = query_lower.split()
        word_count = len(query_words)
        
        # Optimization: Fast path for very short queries to avoid overhead
        if len(query_lower) < 3:
            logger.info(f"Query '{query}' is too short for enhanced search, using basic filtering")
            search_type = "general"
            debug_info["search_type_reason"] = "query_too_short"
            perf_timings['search_type_determination'] = time.time() - search_type_start
            # Don't return here, just set the type and continue
        
        # Check if this is a multi-word query - potentially slow, handle differently
        elif word_count > 1:
            # For multi-word queries, we'll still use the efficient indexed fields
            # but with a more optimized approach in the data_access layer
            debug_info["query_word_count"] = word_count
            debug_info["search_type_reason"] = "multi_word_query"
            logger.info(f"Multi-word query detected: '{query}' with {word_count} words")
            search_type = "general"  # Use general search for multi-word queries
            perf_timings['search_type_determination'] = time.time() - search_type_start
            # Don't return here, just set the type and continue
        
        # Check if this is an exact CMU ID
        else:
            cmu_check_start = time.time()
            is_exact_cmu_id = Component.objects.filter(cmu_id__iexact=query).exists()
            perf_timings['cmu_id_check'] = time.time() - cmu_check_start
            debug_info["is_exact_cmu_id_check"] = is_exact_cmu_id

            if is_exact_cmu_id:
                search_type = "cmu_id"
                debug_info["search_type_reason"] = "exact_cmu_id_match"
                logger.info(f"Query '{query}' identified as an exact CMU ID")
            else:
                # If this looks like a company name (no numbers, longer than 4 chars)
                # try a company-focused search
                company_check_start = time.time()
                if len(query) > 4 and not any(c.isdigit() for c in query):
                    # Common name parts that suggest this is a company name
                    company_keywords = ['ltd', 'limited', 'plc', 'group', 'energy', 'power', 'holdings']
                    
                    # Check for company keywords since we can't use name_of_applicant field
                    if any(keyword in query_lower for keyword in company_keywords):
                        search_type = "company"
                        debug_info["search_type_reason"] = "likely_company_name"
                        logger.info(f"Query '{query}' identified as likely company name")
                perf_timings['company_check'] = time.time() - company_check_start

                # Check if this is a known location directly in the database
                if search_type == "general":  # Only check location if not already identified as company
                    location_check_start = time.time()
                    is_known_location = Component.objects.filter(
                        Q(location__iexact=query) | 
                        Q(county__iexact=query) |
                        Q(outward_code__iexact=query.upper())
                    ).exists()
                    
                    # Only do the expensive postcode check if we need to
                    if not is_known_location and len(query) >= 4:
                        # Check if any postcodes are associated with this location
                        # using the local mapping first before the expensive API call
                        expanded_postcodes = get_all_postcodes_for_area(query_lower)
                        debug_info["expanded_postcodes"] = expanded_postcodes
                        
                        if expanded_postcodes:
                            is_known_location = True
                            search_type = "location"
                            debug_info["search_type_reason"] = "known_location_with_postcodes"
                            logger.info(f"Query '{query}' identified as location with postcodes: {expanded_postcodes}")
                    
                    perf_timings['location_check'] = time.time() - location_check_start
                    
                    if is_known_location:
                        search_type = "location"
                        debug_info["search_type_reason"] = "known_location"
                        logger.info(f"Query '{query}' identified as a location")

        # Log the final determination
        debug_info["determined_search_type"] = search_type
        logger.info(f"Final search type for '{query}': {search_type}")
        perf_timings['search_type_determination'] = time.time() - search_type_start
        # --- Determine Search Type (Revised) --- END ---
        
        # --- Apply component search filter and fetch components --- START ---
        filter_start = time.time()
        component_filter = Q()

        # Add logic based on query type (company, cmu_id, general)
        if search_type == "company" and company_links:
            # Filter by CMU IDs associated with the found companies
            all_cmu_ids = []
            for company_data in company_data_list: 
                 all_cmu_ids.extend(company_data.get('cmu_ids', []))
            # Remove duplicates
            all_cmu_ids = list(set(all_cmu_ids)) 
            if all_cmu_ids:
                component_filter = Q(cmu_id__in=all_cmu_ids)
                logger.info(f"Built component filter for {len(all_cmu_ids)} CMU IDs from found companies.")
        elif search_type == "cmu_id":
             component_filter = Q(cmu_id__iexact=query) # Exact match for CMU ID
        elif search_type == "location":
            # For location searches, we'll use the enhanced location search in data_access.py
            # This will be passed through the query_type parameter
            component_filter = None
            logger.info(f"Using enhanced location search for query: {query}")
            
            # Get expanded postcodes for display in template
            expanded_postcodes = get_all_postcodes_for_area(query.lower())
            if expanded_postcodes:
                debug_info["expanded_postcodes"] = expanded_postcodes
        else: # General search
            # Use broader search across multiple relevant fields
            # Split query into words and apply AND logic between terms
            query_words = query.lower().split()
            if len(query_words) > 1:
                # For multi-word queries, require each word to be present in any of the fields (AND between words, OR between fields)
                multiword_start_time = time.time()
                multi_word_filter = Q()
                
                # Optimization: Start with exact phrase match as it's fastest when available
                exact_phrase_filter = (
                    Q(location__icontains=query) | 
                    Q(description__icontains=query) | 
                    Q(technology__icontains=query) |
                    Q(company_name__icontains=query)
                )
                
                # Then add individual word filters
                word_filters = []
                for word in query_words:
                    if len(word) >= 2:
                        # Create a word filter that checks across all fields
                        # Using Q objects with OR between fields is faster than separate filters
                        word_filter = (
                            Q(location__icontains=word) | 
                            Q(county__icontains=word) |
                            Q(outward_code__icontains=word) |
                            Q(description__icontains=word) | 
                            Q(technology__icontains=word) |
                            Q(company_name__icontains=word)
                        )
                        word_filters.append(word_filter)
                
                # Combine the word filters with AND between them
                if word_filters:
                    combined_word_filter = word_filters[0]
                    for f in word_filters[1:]:
                        combined_word_filter &= f
                    
                    # Combine exact phrase and word filters with OR for maximum recall
                    multi_word_filter = exact_phrase_filter | combined_word_filter
                else:
                    # If no valid words, just use the exact phrase filter
                    multi_word_filter = exact_phrase_filter
                    
                # Apply the multi-word filter
                component_filter = multi_word_filter
                
                perf_timings['multi_word_logic'] = time.time() - multiword_start_time
                logger.info(f"Applied optimized multi-word logic filter for query: {query}")
            else:
                # Single word query - use simpler logic with direct field matches
                single_word = query_words[0] if query_words else ""
                
                if len(single_word) >= 2:
                    # For single words, use a more specific set of filters
                    component_filter = (
                        Q(location__icontains=single_word) | 
                        Q(county__icontains=single_word) |
                        Q(outward_code__icontains=single_word) |
                        Q(description__icontains=single_word) | 
                        Q(technology__icontains=single_word) |
                        Q(company_name__icontains=single_word) |
                        Q(cmu_id__icontains=single_word)
                    )
                else:
                    # Very short word - restrict the search to indexed fields only
                    component_filter = (
                        Q(cmu_id__istartswith=single_word) |
                        Q(location__istartswith=single_word) |
                        Q(company_name__istartswith=single_word)
                    )
                    
                logger.info(f"Applied direct field matching logic for single-word query: {query}")
                
            # Use a lighter-weight version of filtering for general searches
            # Avoiding the heavy postcode mapping helps with faster responses
            
            # Record our filter choice
            debug_info["filter_type"] = "multi_word" if len(query_words) > 1 else "single_word"
        
        debug_info["component_search_filter"] = str(component_filter)
        perf_timings['component_filter_construction'] = time.time() - filter_start
        # --- Remove Added Debug Logging ---
        # logger.debug(f"Component filter constructed for type '{search_type}': {component_filter}")
        # --- End Remove Added Debug Logging ---
        
        # Call get_components_from_database using the constructed filter
        components_list = []

        # --- Add Logging Before Calling Data Access --- START ---
        # --- REMOVE FOCUSSED LOGGING ---
        # logger.warning(f"Value of backend_sort_by JUST BEFORE CALL: '{backend_sort_by}'") 
        # --- REMOVE REDUNDANT DEBUG LOG ---
        # logger.debug(f"Calling get_components_from_database with: ...")
        # --- Add Logging Before Calling Data Access --- END ---
        
        # --- PERFORMANCE TIMING: Database query start ---
        db_query_start = time.time()
        # --- END PERFORMANCE TIMING ---
        
        # --- Conditional call to data_access based on search type --- START ---
        if search_type == "cmu_id":
            # For specific CMU ID searches, use the exact db_filter
            components_list_raw, total_component_count = get_components_from_database(
                 search_term=query, # Pass CMU ID here FOR RANKING within results
                 page=page,
                 per_page=per_page,
                 sort_by=backend_sort_by,
                 sort_order=sort_order,
                 db_filter=component_filter, # Pass the specific CMU ID filter
                 query_type='cmu_id'
             )
            debug_info["data_access_call_type"] = "db_filter (CMU ID)"
        elif search_type == "location":
            # For location searches, specifically set query_type='location'
            components_list_raw, total_component_count = get_components_from_database(
                 search_term=query, # Use query as search term
                 page=page,
                 per_page=per_page,
                 sort_by=backend_sort_by,
                 sort_order=sort_order,
                 db_filter=None, # No specific filter, let data_access build the location filter
                 query_type='location' # This will trigger the enhanced location search logic
             )
            debug_info["data_access_call_type"] = "location (Enhanced Search)"
        else:
            # For general or company searches, use search_term to allow multi-field matching and ranking
            components_list_raw, total_component_count = get_components_from_database(
                 search_term=query, # Use the query as the search term
                 page=page,
                 per_page=per_page,
                 sort_by=backend_sort_by,
                 sort_order=sort_order,
                 db_filter=component_filter, # Now passing component_filter (may be None)
                 query_type=search_type # Pass the analyzed type
             )
            debug_info["data_access_call_type"] = "search_term (General/Company)"
        # --- Conditional call to data_access based on search type --- END ---
        
        # --- PERFORMANCE TIMING: Database query execution ---
        perf_timings['database_query_execution'] = time.time() - db_query_start
        # --- END PERFORMANCE TIMING ---
        
        # Log results after the call
        logger.info(f"get_components_from_database returned {len(components_list_raw)} components for page {page}. Total count: {total_component_count}. Call type: {debug_info.get('data_access_call_type')}")

        # --- Format components and handle pagination object creation --- START ---
        # Format components using the utility function - Apply highlighting here if needed
        # We need the cmu_to_company_mapping for formatting
        format_start = time.time()
        cmu_mapping = cache.get("cmu_to_company_mapping", {})
        components_list = [
            format_component_record(comp, cmu_mapping)
            for comp in components_list_raw
        ]
        perf_timings['format_components'] = time.time() - format_start
        
        # Apply location-based grouping to components_list_raw
        from ..templatetags.checker_tags import group_by_location
        grouped_components = group_by_location(components_list_raw)
        logger.info(f"Grouped {len(components_list_raw)} components into {len(grouped_components)} location groups")
        debug_info["grouped_component_count"] = len(grouped_components)
        
        # Create Paginator and Page object manually for the template
        pagination_start = time.time()
        paginator = Paginator(range(total_component_count), per_page) # Paginate dummy range
        try:
            page_obj = paginator.page(page)
        except PageNotAnInteger:
            page_obj = paginator.page(1)
        except EmptyPage:
            page_obj = paginator.page(paginator.num_pages)
            
        # Replace page_obj.object_list with our grouped components
        page_obj.object_list = grouped_components
        debug_info["final_total_components"] = total_component_count
        debug_info["final_displayed_components"] = len(grouped_components)
        perf_timings['pagination_setup'] = time.time() - pagination_start
        # --- Format components and handle pagination object creation --- END ---
                
    except Exception as e:
        error_message = f"Error fetching search results: {str(e)}"
        logger.exception(error_message)
        debug_info["error"] = error_message
        # Ensure page_obj exists even on error for template rendering
        paginator = Paginator([], per_page) # Empty paginator
        page_obj = paginator.page(1)

    api_time += time.time() - start_time # Add processing time to any DF time
    perf_timings['total_search_time'] = time.time() - overall_start_time

    # --- Prepare final context --- 
    context = {
        "query": query,
        "note": note,
        "company_links": company_links,
        # Pass the page object which contains the components for the current page
        "page_obj": page_obj, 
        # Pass individual components list IF the template iterates over this instead of page_obj
        # "components": components_list, 
        "component_count": total_component_count, # Total matching components
        "error": error_message,
        "api_time": api_time,
        "sort_by": ui_sort_by,         # Use original UI value
        "sort_order": sort_order,   # Use standardized name
        "debug_info": debug_info,
        # Pagination variables (useful if template doesn't solely rely on page_obj)
        "page": page_obj.number if page_obj else page,
        "per_page": per_page,
        "total_pages": page_obj.paginator.num_pages if page_obj else 1,
        "has_prev": page_obj.has_previous() if page_obj else False,
        "has_next": page_obj.has_next() if page_obj else False,
        "page_range": list(page_obj.paginator.get_elided_page_range(number=page_obj.number, on_each_side=1, on_ends=1)) if page_obj else list(range(1, 2)),
        "from_cache": False,  # Add default value for from_cache
        "perf_timings": perf_timings,  # Add performance timings to context
        "expanded_postcodes": debug_info.get("expanded_postcodes", [])  # Add expanded postcodes to context
    }
    
    # Log the performance timings
    logger.warning(f"PERFORMANCE TIMINGS for '{query}' (page {page}):")
    for operation, duration in perf_timings.items():
        logger.warning(f"  {operation}: {duration:.4f}s")
    logger.warning(f"  TOTAL: {perf_timings.get('total_search_time', 0):.4f}s")
    
    # Add extra context if provided
    if extra_context:
        context.update(extra_context)
        
    # Cache the context if results were fetched successfully
    if not error_message and not from_cache:
        context_to_cache = context.copy()
        # Remove potentially large objects not suitable for cache or add specific data
        # We need page_obj details, not the object itself in cache
        context_to_cache.pop('page_obj', None) 
        context_to_cache['components_list_cached'] = components_list # Cache the list explicitly
        context_to_cache['total_component_count_cached'] = total_component_count
        # Add pagination metadata needed to reconstruct context
        context_to_cache['pagination_meta'] = {
            'number': page_obj.number if page_obj else page,
            'num_pages': page_obj.paginator.num_pages if page_obj else 1,
        }
        # Store original time
        context_to_cache['original_time'] = api_time 
        # cache.set(cache_key, context_to_cache, 3600) # Cache for 1 hour
        # logger.info(f"Cached context for key {cache_key}")

    # Return raw data if requested (for API endpoints)
    if return_data_only:
        # Create a structured result that can be easily consumed by API
        raw_results = {
            'components': components_list,  # Use the formatted components
            'total_count': total_component_count,
            'query': query,
            'page': page,
            'per_page': per_page,
            'sort_by': sort_by,
            'sort_order': sort_order,
            'api_time': api_time,
            'error': error_message,
            'perf_timings': perf_timings  # Include performance timings in API response
        }
        
        # Debug logging for API
        logger.info(f"API returning raw results: {len(components_list)} components out of {total_component_count} total for query '{query}'")
        
        return raw_results
        
    # Continue with the render for web requests
    # Render the template (assuming search.html is the correct one)
    template_name = "checker/search.html"
    # --- Add Logging Before Rendering --- START ---
    try:
        # Log key context variables being passed to the template
        context_for_log = {
            k: context.get(k) for k in [
                'query', 'note', 'component_count', 'error', 'api_time', 
                'sort_by', 'sort_order', 'page', 'per_page', 'total_pages',
                'has_prev', 'has_next', 'from_cache'
            ]
        }
        context_for_log['page_obj_exists'] = context.get('page_obj') is not None
        context_for_log['page_obj_len'] = len(context.get('page_obj').object_list) if context.get('page_obj') else 0
        logger.debug(f"Context before rendering {template_name}: {context_for_log}")
    except Exception as log_ex:
        logger.error(f"Error logging context before render: {log_ex}")
    # --- Add Logging Before Rendering --- END ---
    logger.info(f"Rendering template {template_name} with context")
    return render(request, template_name, context)


def format_component_record(record, cmu_to_company_mapping):
    """Format a component record for display with proper company badge"""
    # Check if this is already formatted HTML (might be from cache)
    if isinstance(record, str) and "<div class" in record:
        return record
        
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
            loc_link = f'<a href="{detail_url}">{loc}</a>'
        except Exception as url_error:
            logger.warning(f"Could not reverse URL for component_detail pk={db_id}: {url_error}. Falling back to hardcoded URL.")
            # Fallback to hardcoded URL pattern if reverse fails
            loc_link = f'<a href="/component/{db_id}/">{loc}</a>'
    # --- END FIX ---

    # --- START: Add Map Button Logic ---
    map_button_html = ""
    if loc and loc != "N/A":
        try:
            # URL encode the location for the query parameter
            from urllib.parse import quote
            encoded_loc = quote(loc)
            # Create Google Maps satellite view URL
            map_url = f"https://www.google.com/maps?q={encoded_loc}&t=k"
            # Define button styles
            base_style = "margin-left: 10px; padding: 2px 6px; font-size: 0.8rem; color: white; vertical-align: top; text-decoration: none; border: none; border-radius: 4px; background-color: #5cb85c; transition: background-color 0.2s, transform 0.1s;"
            hover_style = "background-color: #4cae4c;"
            active_style = "background-color: #398439; transform: translateY(1px);"

            map_button_html = f'''
                <a href="{map_url}"
                   target="_blank"
                   class="btn btn-sm" 
                   style="{base_style}"
                   onmouseover="this.style.backgroundColor='#4cae4c';" 
                   onmouseout="this.style.backgroundColor='#5cb85c';"
                   onmousedown="this.style.backgroundColor='#398439'; this.style.transform='translateY(1px)';"
                   onmouseup="this.style.backgroundColor='#4cae4c'; this.style.transform='none';"
                   >
                    <i class="bi bi-geo-alt-fill"></i> Map
                </a>
            '''
        except Exception as map_err:
            logger.warning(f"Error creating map button for location '{loc}': {map_err}")
    # --- END: Add Map Button Logic ---

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
    
    # --- Add De-rated Capacity Badge (modified to always show) ---    
    derated_capacity = record.get("De-Rated Capacity", "N/A")
    # --- DEBUGGING: Log the value received --- 
    logger.info(f"Component DB_ID {db_id}: Received De-rated Capacity = {derated_capacity!r}") 
    # --- END DEBUGGING ---
    
    # Always attempt to format and display
    formatted_capacity = "N/A MW" # Default display
    if derated_capacity != "N/A":
        try:
            formatted_capacity = f"{float(derated_capacity):,.3f} MW"
        except (ValueError, TypeError):
            # Keep original value if it's not a number but not N/A
            formatted_capacity = f"{derated_capacity} MW"
            
    badges.append(f'<span class="badge bg-info me-1">De-rated: {formatted_capacity}</span>')
    # --- End De-rated Capacity Badge ---
    
    badges_html = " ".join(badges)
    badges_div = f'<div class="mb-2">{badges_html}</div>' if badges_html else ""

    return f"""
    <div class="component-record">
        <strong>{loc_link}{map_button_html}</strong>
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

# Helper function to safely parse year (moved from company_detail)
def try_parse_year(year_str):
    try:
        return int(year_str)
    except (ValueError, TypeError):
        return -1 # Treat non-numeric years as oldest