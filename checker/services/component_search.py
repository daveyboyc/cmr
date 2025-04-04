import urllib.parse
import time
import logging
import re # Import re for regex matching
from django.shortcuts import render
from django.core.cache import cache
import traceback
from django.urls import reverse
from django.db.models import Q # Import Q for general search
from . import data_access # Ensure data_access is imported
from ..utils import normalize, get_cache_key # Import normalize/cache key if needed
from ..models import Component # <<< ADD MODEL IMPORT HERE

from .company_search import _perform_company_search, get_cmu_dataframe, _build_search_results

logger = logging.getLogger(__name__)


def search_components_service(request):
    """Handles ONLY the general search logic (companies + components)."""
    # Initialize variables with defaults in case of errors
    query = ""
    page = 1
    per_page = 50
    sort_order = "desc"
    start_time = time.time()
    logger = logging.getLogger(__name__)
    
    try:
        # Safe parameter extraction
        query = request.GET.get("q", "").strip()
        page = int(request.GET.get("page", 1))
        per_page = int(request.GET.get("per_page", 50))
        sort_order = request.GET.get("comp_sort", "desc") 
    except ValueError as e:
        logger.warning(f"Parameter parsing error: {str(e)}, using defaults")
    
    # --- Handle General Search (Existing Logic from previous ELSE block) --- 
    logger.info(f"[search_components_service] Performing general search for '{query}'")
    try:
        # 1. Search Companies (Placeholder - Adapt based on actual requirements)
        try:
            companies_qs = Component.objects.filter(
                Q(company_name__icontains=query) | Q(cmu_id__icontains=query)
            ).values_list('company_name', flat=True).distinct()
            
            matching_company_names = list(companies_qs[:20]) 
            company_count = len(matching_company_names)
        except Exception as company_error:
            logger.error(f"Error querying companies: {str(company_error)}")
            matching_company_names = []
            company_count = 0
        
        # 2. Search Components (General)
        try:
            components, total_component_count = data_access.get_components_from_database(
                search_term=query, 
                page=page, 
                per_page=per_page, 
                sort_order=sort_order
            )
        except Exception as e:
            logger.error(f"Error calling data_access.get_components_from_database: {str(e)}")
            # Create a fallback empty result if data_access fails
            components = []
            total_component_count = 0
        
        api_time = time.time() - start_time

        # 3. Format Components
        formatted_components = []
        try:
            cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
            formatted_components = [
                format_component_record(comp, cmu_to_company_mapping) 
                for comp in components
            ]
        except Exception as format_error:
            logger.error(f"Error formatting components: {str(format_error)}")
            # Use raw components if formatting fails
            formatted_components = []
        
        # 4. Prepare Company List for Display (Placeholder logic)
        final_companies = []
        try:
            if matching_company_names:
                company_details_qs = Component.objects.filter(
                    company_name__in=matching_company_names
                ).values('company_name', 'cmu_id')
                company_summary = {}
                for detail in company_details_qs:
                    name = detail['company_name']
                    cmu_id = detail['cmu_id']
                    if name not in company_summary:
                        company_summary[name] = {'name': name, 'cmu_ids': set()}
                    if cmu_id:
                        company_summary[name]['cmu_ids'].add(cmu_id)
                for name in company_summary:
                    approx_comp_count = Component.objects.filter(company_name=name).count()
                    company_summary[name]['component_count'] = approx_comp_count
                    cmu_ids_list = sorted(list(company_summary[name]['cmu_ids']))
                    company_summary[name]['cmu_ids_display'] = ", ".join(cmu_ids_list[:3]) + (f" and {len(cmu_ids_list) - 3} more" if len(cmu_ids_list) > 3 else "")
                    company_summary[name]['cmu_ids'] = cmu_ids_list 
                    final_companies.append(company_summary[name])
                final_companies.sort(key=lambda x: x['name']) # Example sort
        except Exception as company_process_error:
            logger.error(f"Error processing company data: {str(company_process_error)}")
            final_companies = []

        # 5. Build Context for General Search
        context = {
            'query': query,
            'components': formatted_components,
            'total_count': total_component_count,
            'api_time': api_time,
            'companies': final_companies,
            'company_count': len(final_companies),
            'is_cmu_list_view': False, # ALWAYS False for general search
            'page_title': f'Search Results for "{query}"' if query else 'Search',
            'per_page': per_page,
        }
        
        logger.info(f"[search_components_service] Completed search for '{query}'")
        return context # Return context dictionary

    except Exception as e:
        logger.exception(f"[search_components_service] Critical error during search for '{query}': {str(e)}")
        # Return a valid error context dictionary
        return {
            'query': query,
            'error': f'An error occurred: {str(e)}',
            'components': [],
            'companies': [],
            'is_cmu_list_view': False,
            'total_count': 0,
            'api_time': time.time() - start_time,
            'company_count': 0,
            'page_title': 'Search Error',
            'per_page': per_page
        }
    # --- End General Search Handling --- 


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
            loc_link = f'<a href="{detail_url}" style="color: blue; text-decoration: underline;">{loc}</a>'
        except Exception as url_error:
            logger.warning(f"Could not reverse URL for component_detail pk={db_id}: {url_error}. Falling back to hardcoded URL.")
            # Fallback to hardcoded URL pattern if reverse fails
            loc_link = f'<a href="/component/{db_id}/" style="color: blue; text-decoration: underline;">{loc}</a>'
    # --- END FIX ---

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