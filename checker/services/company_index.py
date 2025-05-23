import logging
import pickle
import base64
import time
from django.core.cache import cache
from rapidfuzz import fuzz
from ..utils import normalize

logger = logging.getLogger(__name__)

# Redis keys - must match the ones in build_company_index.py
COMPANY_INDEX_KEY = "company_index_v1"
COMPANY_INDEX_UPDATE_KEY = "company_index_last_updated"

def get_company_index():
    """
    Load the company index from Redis.
    
    Returns:
        dict: The company index mapping normalized company names to data
        float: Time to load the index
        bool: Whether the index was loaded from Redis cache
    """
    start_time = time.time()
    
    # Try to get from Redis cache
    serialized_index = cache.get(COMPANY_INDEX_KEY)
    if serialized_index is None:
        logger.warning("Company index not found in Redis")
        return {}, time.time() - start_time, False
    
    try:
        # Deserialize the index from Redis
        company_index = pickle.loads(base64.b64decode(serialized_index))
        load_time = time.time() - start_time
        num_companies = len(company_index)
        
        logger.info(f"Loaded company index from Redis with {num_companies} companies in {load_time:.4f}s")
        return company_index, load_time, True
    except Exception as e:
        logger.error(f"Error deserializing company index from Redis: {str(e)}")
        return {}, time.time() - start_time, False

def find_companies_by_name(search_term, company_index, score_cutoff=75, limit=20):
    """
    Find companies in the index that match the search term using fuzzy matching.
    
    Args:
        search_term (str): The search term
        company_index (dict): The company index from get_company_index()
        score_cutoff (int): Minimum similarity score (0-100)
        limit (int): Maximum number of results to return
        
    Returns:
        list: Matching company data objects sorted by relevance
        float: Time to search
    """
    if not search_term or not isinstance(search_term, str) or not company_index:
        return [], 0.0
    
    start_time = time.time()
    normalized_term = normalize(search_term)
    
    # Find matches using RapidFuzz against normalized company names
    matches = []
    for norm_name, company_data in company_index.items():
        # Calculate similarity score
        score = fuzz.ratio(normalized_term, norm_name)
        if score >= score_cutoff:
            # Add score to the company data for sorting
            company_with_score = company_data.copy()
            company_with_score["score"] = score
            matches.append(company_with_score)
    
    # Sort by score descending
    matches.sort(key=lambda x: x["score"], reverse=True)
    
    # Apply limit
    matches = matches[:limit]
    
    search_time = time.time() - start_time
    logger.info(f"Found {len(matches)} companies matching '{search_term}' (normalized: '{normalized_term}') in {search_time:.4f}s")
    
    return matches, search_time

def get_company_links_html(search_term):
    """
    Get HTML links for companies matching the search term.
    This is the main function to call from views/search services.
    
    Args:
        search_term (str): The search term
        
    Returns:
        list: List of HTML strings for each matching company
        int: Number of matches found
        float: Total time spent (loading + searching)
    """
    start_time = time.time()
    
    # Get the company index
    company_index, load_time, is_cached = get_company_index()
    if not company_index:
        logger.error("Cannot search companies - index not available")
        return [], 0, time.time() - start_time
    
    # Find matching companies
    matches, search_time = find_companies_by_name(search_term, company_index)
    
    # Extract just the HTML for each match
    html_links = [company_data["html"] for company_data in matches]
    
    total_time = time.time() - start_time
    logger.info(f"Generated {len(html_links)} company links for '{search_term}' in {total_time:.4f}s")
    
    return html_links, len(matches), total_time 