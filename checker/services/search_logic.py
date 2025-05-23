"""
Functions related to analyzing search queries and determining search intent.
"""
import logging
import re

logger = logging.getLogger(__name__)

# Example: Basic query type analysis (to be expanded)
def analyze_query(query):
    """Analyzes the query to determine its likely type."""
    if not query:
        return 'empty', None

    query_lower = query.lower()
    
    # Simple CMU ID check (e.g., ABC123, AAA001)
    if re.fullmatch(r"[a-zA-Z]{3}\d{3}", query, re.IGNORECASE):
        logger.debug(f"Query '{query}' identified as potential CMU ID.")
        return 'cmu_id', query.upper()

    # Add checks for postcodes, locations, etc. here later

    logger.debug(f"Query '{query}' identified as general term.")
    return 'general', query 