from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.views.decorators.http import require_http_methods
import urllib.parse

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
