import time
import logging
import requests
from django.conf import settings
from django.core.cache import cache
from django.db.models import Q
from ..models import Component
from ..utils import get_cache_key

logger = logging.getLogger(__name__)

# ----- DATABASE FIRST COMPONENT ACCESS FUNCTIONS -----

def search_database_components(query, page=1, per_page=100, sort_order="desc"):
    """
    Search for components in the database using a multi-term search approach.
    This handles space-separated queries properly without special cases.
    
    Args:
        query: Search string (can contain multiple words)
        page: Page number for pagination
        per_page: Items per page
        sort_order: 'asc' or 'desc' for sorting by delivery year
        
    Returns:
        tuple: (components_list, metadata_dict)
    """
    start_time = time.time()
    
    try:
        # First, check if this is a direct CMU ID query
        if query and not ' ' in query and (query.upper().startswith('CM') or query.upper().startswith('T-')):
            # Direct CMU ID search - case insensitive
            queryset = Component.objects.filter(cmu_id__iexact=query)
        else:
            # Multi-term search approach
            query_terms = query.lower().split()
            
            # Start with empty query
            query_filter = Q()
            
            # Process each search term independently
            for term in query_terms:
                if len(term) >= 3:  # Only use terms with at least 3 characters
                    term_filter = (
                        Q(company_name__icontains=term) | 
                        Q(location__icontains=term) | 
                        Q(description__icontains=term) |
                        Q(cmu_id__icontains=term)
                    )
                    # Add each term with OR logic
                    query_filter |= term_filter
            
            # Apply the combined filter
            queryset = Component.objects.filter(query_filter)
        
        # Make the queryset distinct to avoid duplicates
        queryset = queryset.distinct()
        
        # Get total count for pagination
        total_count = queryset.count()
        
        # Apply sorting
        if sort_order == "asc":
            queryset = queryset.order_by('delivery_year')
        else:  # Default to desc
            queryset = queryset.order_by('-delivery_year')
        
        # Apply pagination
        offset = (page - 1) * per_page
        paginated_queryset = queryset[offset:offset+per_page]
        
        # Convert to list of dictionaries for API compatibility
        components = []
        for comp in paginated_queryset:
            comp_dict = {
                "CMU ID": comp.cmu_id,
                "Location and Post Code": comp.location,
                "Description of CMU Components": comp.description,
                "Generating Technology Class": comp.technology,
                "Company Name": comp.company_name,
                "Auction Name": comp.auction_name,
                "Delivery Year": comp.delivery_year,
                "Status": comp.status,
                "Type": comp.type,
                "_id": comp.component_id
            }
            
            # Add any additional data if available
            if comp.additional_data:
                for key, value in comp.additional_data.items():
                    if key not in comp_dict:
                        comp_dict[key] = value
                        
            components.append(comp_dict)
        
        # Create metadata for pagination and timing
        metadata = {
            "total_count": total_count,
            "page": page,
            "per_page": per_page,
            "total_pages": (total_count + per_page - 1) // per_page if total_count > 0 else 1,
            "source": "database",
            "processing_time": time.time() - start_time
        }
        
        # Cache results if not too large (for performance)
        cache_key = get_cache_key(f"components_search_p{page}_s{per_page}", query)
        if total_count <= 1000:
            cache.set(cache_key, (components, metadata), 3600)  # 1 hour for small results
        elif total_count <= 5000:
            cache.set(cache_key, (components, metadata), 900)   # 15 minutes for medium results
            
        return components, metadata
        
    except Exception as e:
        logger.exception(f"Error in database search for '{query}': {str(e)}")
        return [], {
            "error": str(e),
            "total_count": 0,
            "processing_time": time.time() - start_time
        }


def fetch_components_for_cmu_id(cmu_id, limit=None, page=1, per_page=100):
    """
    Fetch components for a given CMU ID. This is a compatibility wrapper
    around search_database_components to maintain backward compatibility.
    """
    # Use the more general search function
    return search_database_components(cmu_id, page, per_page)


def get_company_matches(query, limit=20):
    """
    Search for companies in the database that match the query.
    Uses a multi-term approach for better search results.
    
    Args:
        query: Search string (can contain multiple words)
        limit: Maximum number of companies to return
        
    Returns:
        list: List of dictionaries with company_name and cmu_ids
    """
    start_time = time.time()
    
    try:
        # Multi-term search approach
        query_terms = query.lower().split()
        
        # Start with empty query
        query_filter = Q()
        
        # Process each search term independently
        for term in query_terms:
            if len(term) >= 3:  # Only use terms with at least 3 characters
                term_filter = (
                    Q(company_name__icontains=term) | 
                    Q(cmu_id__icontains=term)
                )
                # Add each term with OR logic
                query_filter |= term_filter
                
        # Get distinct company names that match query
        company_query = Component.objects.filter(query_filter) \
                         .values('company_name') \
                         .distinct()[:limit]
        
        companies = []
        for company in company_query:
            company_name = company['company_name']
            
            # Get all CMU IDs for this company
            cmu_ids = Component.objects.filter(company_name=company_name) \
                      .values_list('cmu_id', flat=True).distinct()
            
            companies.append({
                'company_name': company_name,
                'cmu_ids': list(cmu_ids)
            })
            
        logger.info(f"Found {len(companies)} companies for query '{query}' in {time.time() - start_time:.2f}s")
        return companies
        
    except Exception as e:
        logger.exception(f"Error searching companies for '{query}': {str(e)}")
        return []


def get_company_details(company_id):
    """
    Get detailed information about a company including its CMU IDs,
    years, and auctions.
    
    Args:
        company_id: Normalized company identifier
        
    Returns:
        dict: Company details or None if not found
    """
    # Convert company_id to searchable format
    searchable_company = company_id.replace('-', ' ')
    
    try:
        # Find components for this company
        company_components = Component.objects.filter(
            company_name__icontains=searchable_company
        )
        
        if not company_components.exists():
            return None
            
        # Get company name from first component
        company_name = company_components.first().company_name
        
        # Get all CMU IDs for this company
        cmu_ids = company_components.values_list('cmu_id', flat=True).distinct()
        
        # Get all years and auctions
        years_auctions = {}
        year_query = company_components.values_list('delivery_year', 'auction_name') \
                    .distinct().order_by('delivery_year')
                    
        for year, auction in year_query:
            if year not in years_auctions:
                years_auctions[year] = []
            
            if auction and auction not in years_auctions[year]:
                years_auctions[year].append(auction)
        
        return {
            'company_name': company_name,
            'cmu_ids': list(cmu_ids),
            'years_auctions': years_auctions
        }
        
    except Exception as e:
        logger.exception(f"Error fetching company details for '{company_id}': {str(e)}")
        return None


def get_auction_components(company_id, year, auction_name):
    """
    Get components for a specific company, year and auction.
    
    Args:
        company_id: Normalized company identifier
        year: Delivery year
        auction_name: Name of the auction
        
    Returns:
        list: Components matching the criteria
    """
    searchable_company = company_id.replace('-', ' ')
    
    try:
        # First try exact matching
        components = Component.objects.filter(
            company_name__icontains=searchable_company,
            delivery_year__icontains=year,
            auction_name__icontains=auction_name
        ).order_by('location')
        
        # If no results, try more flexible matching
        if not components.exists():
            components = Component.objects.filter(
                company_name__icontains=searchable_company
            ).filter(
                Q(delivery_year__icontains=year) | 
                Q(auction_name__icontains=year)
            ).filter(
                Q(auction_name__icontains=auction_name) |
                Q(auction_name__icontains=auction_name.replace("T-", "T"))
            ).order_by('location')
            
        return list(components)
        
    except Exception as e:
        logger.exception(f"Error fetching auction components: {str(e)}")
        return []


# ----- API FALLBACK FUNCTIONS -----

def api_fetch_components(query, limit=100, offset=0):
    """
    Fetch components from the API. This is only used as a fallback or for
    initial data loading when the database is empty.
    
    Args:
        query: Search string
        limit: Maximum records to return
        offset: Pagination offset
        
    Returns:
        tuple: (components_list, total_count, api_time)
    """
    start_time = time.time()
    
    try:
        params = {
            "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
            "q": query,
            "limit": limit,
            "offset": offset
        }
        
        response = requests.get(
            "https://data.nationalgrideso.com/api/3/action/datastore_search",
            params=params,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json().get("result", {})
            components = result.get("records", [])
            total_count = result.get("total", 0)
            
            # Save to database for future use
            for component in components:
                cmu_id = component.get("CMU ID", "")
                if cmu_id:
                    save_component_to_database(cmu_id, component)
            
            return components, total_count, time.time() - start_time
        else:
            logger.error(f"API error: {response.status_code}")
            return [], 0, time.time() - start_time
            
    except Exception as e:
        logger.exception(f"Error fetching components from API: {str(e)}")
        return [], 0, time.time() - start_time


def save_component_to_database(cmu_id, component_data):
    """
    Save a single component to the database.
    
    Args:
        cmu_id: CMU ID
        component_data: Component data dictionary
    """
    try:
        component_id = component_data.get("_id", "")
        
        # Skip if component already exists
        if component_id and Component.objects.filter(component_id=component_id).exists():
            return
            
        # Create component in database
        Component.objects.create(
            component_id=component_id,
            cmu_id=cmu_id,
            location=component_data.get("Location and Post Code", ""),
            description=component_data.get("Description of CMU Components", ""),
            technology=component_data.get("Generating Technology Class", ""),
            company_name=component_data.get("Company Name", ""),
            auction_name=component_data.get("Auction Name", ""),
            delivery_year=component_data.get("Delivery Year", ""),
            status=component_data.get("Status", ""),
            type=component_data.get("Type", ""),
            additional_data=component_data
        )
    except Exception as e:
        logger.exception(f"Error saving component to database: {str(e)}")


def save_components_to_database(cmu_id, components):
    """
    Save multiple components to the database.
    
    Args:
        cmu_id: CMU ID
        components: List of component dictionaries
    """
    from django.db import transaction
    
    if not components:
        return
        
    # Extract component_ids for efficient existence check
    component_ids = [c.get("_id", "") for c in components if c.get("_id")]
    
    # Find existing IDs in one query
    existing_ids = set()
    if component_ids:
        existing_ids = set(Component.objects.filter(
            component_id__in=component_ids
        ).values_list('component_id', flat=True))
    
    # Create list of new components only
    new_components = []
    for component in components:
        component_id = component.get("_id", "")
        
        # Skip if already in database
        if component_id and component_id in existing_ids:
            continue
            
        # Add to list for bulk create
        new_components.append(Component(
            component_id=component_id,
            cmu_id=cmu_id,
            location=component.get("Location and Post Code", ""),
            description=component.get("Description of CMU Components", ""),
            technology=component.get("Generating Technology Class", ""),
            company_name=component.get("Company Name", ""),
            auction_name=component.get("Auction Name", ""),
            delivery_year=component.get("Delivery Year", ""),
            status=component.get("Status", ""),
            type=component.get("Type", ""),
            additional_data=component
        ))
    
    # Bulk create if we have new components
    if new_components:
        try:
            with transaction.atomic():
                Component.objects.bulk_create(new_components, ignore_conflicts=True)
        except Exception as e:
            logger.exception(f"Error bulk creating components: {str(e)}")


# ----- DATABASE STATISTICS -----

def get_database_statistics():
    """
    Get statistics about the database.
    
    Returns:
        dict: Statistics about the database
    """
    try:
        return {
            'total_components': Component.objects.count(),
            'unique_cmus': Component.objects.values('cmu_id').distinct().count(),
            'unique_companies': Component.objects.values('company_name').distinct().count(),
            'top_companies': list(Component.objects.values('company_name')
                               .annotate(count=Count('id'))
                               .order_by('-count')[:10]),
            'delivery_years': list(Component.objects.values_list('delivery_year', flat=True)
                               .distinct().order_by('delivery_year')),
        }
    except Exception as e:
        logger.exception(f"Error getting database statistics: {str(e)}")
        return {
            'error': str(e),
            'total_components': 0
        }


# ----- LEGACY FUNCTIONS (MAINTAINED FOR BACKWARDS COMPATIBILITY) -----

def get_cmu_data_from_json():
    """
    LEGACY: Get CMU data from JSON file.
    Database should be used instead via Component.objects.values('cmu_id', 'company_name').distinct()
    """
    logger.warning("get_cmu_data_from_json is deprecated, use database instead")
    
    try:
        # Get CMU data from database instead
        cmu_data = []
        for component in Component.objects.values('cmu_id', 'company_name').distinct()[:1000]:
            cmu_data.append({
                "CMU ID": component['cmu_id'],
                "Name of Applicant": component['company_name']
            })
        return cmu_data
    except:
        # Fall back to original function
        import os
        import json
        
        json_path = os.path.join(settings.BASE_DIR, 'cmu_data.json')
        if not os.path.exists(json_path):
            return None
            
        try:
            with open(json_path, 'r') as f:
                all_cmu_data = json.load(f)
            return all_cmu_data
        except Exception as e:
            logger.error(f"Error reading CMU data from JSON: {e}")
            return None


def get_cmu_data_by_id(cmu_id):
    """
    LEGACY: Get CMU data for a specific CMU ID.
    """
    if not cmu_id:
        return None
        
    # Try database first
    try:
        component = Component.objects.filter(cmu_id=cmu_id).first()
        if component:
            return {
                "CMU ID": component.cmu_id,
                "Name of Applicant": component.company_name,
                "Delivery Year": component.delivery_year,
                "Auction Name": component.auction_name
            }
    except:
        pass
        
    # Fall back to cache
    cache_key = f"cmu_data_{cmu_id}"
    cached_data = cache.get(cache_key)
    if cached_data:
        return cached_data
        
    # Fall back to original JSON lookup
    all_cmu_data = get_cmu_data_from_json()
    if not all_cmu_data:
        return None
        
    for cmu_data in all_cmu_data:
        if str(cmu_data.get("CMU ID", "")) == str(cmu_id):
            cache.set(cache_key, cmu_data, 3600)
            return cmu_data
            
    return None


def analyze_component_duplicates(components):
    """
    Analyze a list of components to identify duplicates.
    
    Args:
        components: List of component dictionaries
        
    Returns:
        dict: Duplicate analysis results
    """
    if not components:
        return {
            "total_components": 0,
            "unique_locations": 0,
            "location_counts": {},
            "duplicate_locations": []
        }
    
    # Count locations
    location_counts = {}
    for component in components:
        location = component.get("Location and Post Code", "")
        if not location:
            location = "(No Location)"
        
        if location in location_counts:
            location_counts[location] += 1
        else:
            location_counts[location] = 1
    
    # Find duplicates
    duplicate_locations = [
        {"location": loc, "count": count} 
        for loc, count in location_counts.items() 
        if count > 1
    ]
    
    return {
        "total_components": len(components),
        "unique_locations": len(location_counts),
        "location_counts": location_counts,
        "duplicate_locations": duplicate_locations
    }