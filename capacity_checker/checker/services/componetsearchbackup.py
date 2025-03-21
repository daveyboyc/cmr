import urllib.parse
import time
import logging
from django.shortcuts import render
from django.core.cache import cache

from ..utils import normalize
from .data_access import fetch_components_for_cmu_id, get_component_data_from_json

logger = logging.getLogger(__name__)


def search_components_service(request):
    """Service function for searching components"""
    results = {}
    error_message = None
    api_time = 0
    query = request.GET.get("q", "").strip()

    if request.method == "GET":
        if "search_results" in request.session and not query:
            # Only use session results if no new query is provided
            results = request.session.pop("search_results")
            record_count = request.session.pop("record_count", None)
            api_time = request.session.pop("api_time", 0)
            last_query = request.session.pop("last_query", "")
            return render(request, "checker/search_components.html", {
                "results": results,
                "record_count": record_count,
                "error": error_message,
                "api_time": api_time,
                "query": last_query,
            })
        elif query:
            start_time = time.time()

            # Get mapping of CMU IDs to company names from cache
            cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})

            # FIXED: Use existing functions instead of get_component_data
            components = []
            components_api_time = 0
            try:
                # First try to get components directly from JSON
                components = get_component_data_from_json(query)

                # If not found, try to fetch components by CMU ID
                if not components:
                    components, components_api_time = fetch_components_for_cmu_id(query)
                    api_time += components_api_time
            except Exception as e:
                logger.error(f"Error fetching component data: {str(e)}")
                error_message = f"Error fetching component data: {str(e)}"
                components = []

            record_count = len(components) if components else 0

            # Format results for display
            sentences = []
            if components:
                for record in components:
                    # Format each component record
                    formatted_record = format_component_record(record, cmu_to_company_mapping)
                    sentences.append(formatted_record)
            else:
                sentences = [f"No matching components found for '{query}'."]

            results[query] = sentences

            # Calculate API time
            api_time += (time.time() - start_time)

            # Store in session
            request.session["search_results"] = results
            request.session["record_count"] = record_count
            request.session["api_time"] = api_time
            request.session["last_query"] = query

            return render(request, "checker/search_components.html", {
                "results": results,
                "record_count": record_count,
                "error": error_message,
                "api_time": api_time,
                "query": query,
            })
        else:
            # Clear session but keep query parameter in case it's needed
            if "search_results" in request.session:
                request.session.pop("search_results", None)
            if "api_time" in request.session:
                request.session.pop("api_time", None)

            return render(request, "checker/search_components.html", {
                "results": {},
                "api_time": api_time,
                "query": query,
            })

    return render(request, "checker/search_components.html", {
        "results": {},
        "api_time": api_time,
        "query": query,
    })


def format_component_record(record, cmu_to_company_mapping):
    """Format a component record for display with proper company badge"""
    loc = record.get("Location and Post Code", "N/A")
    desc = record.get("Description of CMU Components", "N/A")
    tech = record.get("Generating Technology Class", "N/A")
    typ = record.get("Type", "N/A")
    delivery_year = record.get("Delivery Year", "N/A")
    auction = record.get("Auction Name", "N/A")
    cmu_id = record.get("CMU ID", "N/A")

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
        encoded_company_name = urllib.parse.quote(company_name)
        company_link = f'<a href="/?q={encoded_company_name}" class="badge bg-success" style="font-size: 1rem; text-decoration: none;">{company_name}</a>'
        company_info = f'<div class="mt-2 mb-2">{company_link}</div>'
    else:
        company_info = f'<div class="mt-2 mb-2"><span class="badge bg-warning">No Company Found</span></div>'

    # Create blue link for location pointing to component detail page
    normalized_loc = normalize(loc)
    component_id = f"{cmu_id}_{normalized_loc}"
    encoded_component_id = urllib.parse.quote(component_id)
    loc_link = f'<a href="/component/{encoded_component_id}/" style="color: blue; text-decoration: underline;">{loc}</a>'

    # Format badges for type and delivery year
    type_badge = f'<span class="badge bg-info">{typ}</span>' if typ != "N/A" else ""
    year_badge = f'<span class="badge bg-secondary">{delivery_year}</span>' if delivery_year != "N/A" else ""
    badges = " ".join(filter(None, [type_badge, year_badge]))
    badges_div = f'<div class="mb-2">{badges}</div>' if badges else ""

    return f"""
    <div class="component-record">
        <strong>{loc_link}</strong>
        <div class="mt-1 mb-1"><i>{desc}</i></div>
        <div>Technology: {tech} | <b>{auction}</b> | <span class="text-muted">CMU ID: {cmu_id}</span></div>
        {badges_div}
        {company_info}
    </div>
    """

