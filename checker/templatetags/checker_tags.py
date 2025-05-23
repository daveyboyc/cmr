from django import template
import json
from django.utils.safestring import mark_safe
from django.core.serializers.json import DjangoJSONEncoder
from ..utils import normalize # Import normalize function
from django.utils.html import format_html
from django.contrib.humanize.templatetags.humanize import intcomma
import logging
import re


register = template.Library()
logger = logging.getLogger(__name__)

@register.filter(name='get_item')
def get_item(dictionary, key):
    """Get an item from a dictionary using bracket notation."""
    return dictionary.get(key, 'N/A')

@register.filter(name='dictitem')
def dictitem(dictionary, key):
    """Get an item from a dictionary - alternative to get_item."""
    if not dictionary or not isinstance(dictionary, dict):
        return None
    return dictionary.get(key)

@register.filter
def replace(value, arg):
    """
    Replace one string with another in a given value.
    Usage: {{ value|replace:'oldstring,newstring' }}
    Example: {{ "hello-world"|replace:'-,_' }} becomes "hello_world"
    """
    if len(arg.split(',')) != 2:
        return value
    
    old_str, new_str = arg.split(',')
    return value.replace(old_str, new_str)


@register.filter
def url_safe(value):
    """
    Make a string URL-safe by replacing spaces with underscores.
    Usage: {{ value|url_safe }}
    Example: {{ "hello world"|url_safe }} becomes "hello_world"
    """
    # First slugify to handle special characters, then replace hyphens with underscores
    from django.utils.text import slugify
    return slugify(value).replace('-', '_')

@register.filter(name='slugify_for_url')
def slugify_for_url(value):
    import re
    from django.utils.text import slugify
    
    # Remove or replace invalid characters before slugifying
    value = re.sub(r'[^\w\s-]', '', value)
    return slugify(value) if value else 'unknown'

@register.filter(name='pprint')
def pretty_print(value):
    import pprint
    return pprint.pformat(value, indent=2)

@register.filter(is_safe=True)
def jsonify(value):
    """Converts a Python dict or list to a JSON string for <pre> display."""
    try:
        return json.dumps(value, indent=2, ensure_ascii=False)
    except Exception:
        return str(value) # Fallback

@register.filter(name='replace_underscores')
def replace_underscores(value):
    if isinstance(value, str):
        return value.replace('_', ' ')
    return value

@register.filter(name='normalize')
def normalize_filter(value):
    from ..utils import normalize # Local import to avoid circular dependency issues
    if isinstance(value, str):
        return normalize(value)
    return value

@register.filter(name='urlencode')
def urlencode_filter(value):
    """URL encodes a string, handling spaces and special characters."""
    import urllib.parse
    return urllib.parse.quote(str(value))

@register.filter(name='format_value')
def format_value(value):
    """Formats a value for display, handling numbers, None, and strings."""
    if value is None or value == '':
        return "N/A"
    
    # Try converting to float for formatting
    try:
        float_val = float(value)
        # Check if it's effectively an integer
        if float_val == int(float_val):
            return intcomma(int(float_val))
        else:
            # Format as float with commas and reasonable precision (e.g., 2 decimal places)
            # Use f-string formatting for precision control before intcomma
            formatted_num = f"{float_val:,.2f}" 
            return formatted_num
    except (ValueError, TypeError):
        # If it's not a number, return the original value as a string
        return str(value)

@register.filter(name='shorten_auction_name')
def shorten_auction_name(value):
    """Shortens auction names like '2024-25 (T-4) Four Year Ahead Capacity Auction' to '2024-25 (T-4)'."""
    if not isinstance(value, str):
        return value
    
    # Regex to capture the year range and auction type (T-1, T-3, T-4, TR)
    # Allows for year formats like 2024/25, 2024-25, 2024
    # Allows for T-1, T1, T-3, T3, T-4, T4, TR
    match = re.match(r"^\s*(\d{4}[/-]?\d{2,4}|\d{4})\s*\((T[-]?\d|TR)\).*", value, re.IGNORECASE)
    
    if match:
        year_part = match.group(1)
        type_part = match.group(2).upper().replace('-', '') # Normalize to T1, T3, T4, TR
        # Re-add hyphen for T-1, T-3, T-4
        if type_part in ['T1', 'T3', 'T4']:
            type_part = f"T-{type_part[1]}"
            
        return f"{year_part} ({type_part})"
    else:
        # If no match, return the original string (or a truncated version)
        return value # Or perhaps value[:30] + '...' if you want to truncate unknowns

@register.filter(name='strip_prefix')
def strip_prefix(value, prefix):
    """Removes a specific prefix from a string if it exists."""
    if isinstance(value, str) and value.startswith(prefix):
        return value[len(prefix):]
    return value

@register.filter(name='group_by_location')
def group_by_location(components):
    """
    Groups components by location and description, preserving unique CMU IDs and delivery years.
    
    Returns a list of grouped components, where each group has:
    - location: The shared location
    - description: The shared description
    - cmu_ids: List of unique CMU IDs
    - auction_names: List of unique auction names
    - auction_to_components: Mapping of auction names to component IDs
    - active_status: Whether any components in the group are from 2024-25 or later
    - first_component: The first component in the group (for reference)
    - count: Number of components in the group
    """
    if not components:
        return []
    
    def normalize_location(loc):
        """Normalize location for consistent grouping"""
        if not loc:
            return ""
        # Convert to lowercase and strip whitespace
        norm = loc.lower().strip()
        # Remove common punctuation
        norm = re.sub(r'[,\.\-_\/\\]', ' ', norm)
        # Remove extra whitespace
        norm = re.sub(r'\s+', ' ', norm)
        # Handle special cases
        if "energy centre" in norm and "mosley" in norm:
            return "energy centre lower mosley street"
        return norm
    
    def is_auction_year_active(auction_name):
        """Check if an auction year is 2024-25 or later"""
        if not auction_name:
            return False
        
        # Extract year from auction name using regex
        year_match = re.search(r'(\d{4})[-/]?(\d{2,4})', auction_name)
        if year_match:
            start_year = year_match.group(1)
            try:
                # Convert to integer for comparison
                year_int = int(start_year)
                # Active if 2024 or later
                return year_int >= 2024
            except ValueError:
                return False
        return False
    
    groups = {}
    
    for comp in components:
        # Extract key fields
        location = comp.get('location', '')
        description = comp.get('description', '')
        cmu_id = comp.get('cmu_id', '')
        auction_name = comp.get('auction_name', '')
        component_id = comp.get('id')  # Get component ID for linking
        
        # Create normalized keys for grouping
        norm_location = normalize_location(location)
        
        # Create a group key
        group_key = (norm_location, description)
        
        if group_key not in groups:
            groups[group_key] = {
                'location': location,  # Keep original formatting
                'description': description,
                'cmu_ids': set(),
                'auction_names': set(),
                'auction_to_components': {},  # Map auction names to component IDs
                'active_status': False,  # Initialize as inactive
                'components': [],
                'first_component': comp  # Store first component
            }
        
        if cmu_id:
            groups[group_key]['cmu_ids'].add(cmu_id)
        
        if auction_name:
            groups[group_key]['auction_names'].add(auction_name)
            
            # Check if this auction makes the group active
            if is_auction_year_active(auction_name):
                groups[group_key]['active_status'] = True
            
            # Store the component ID for this auction name
            if component_id and auction_name:
                if auction_name not in groups[group_key]['auction_to_components']:
                    groups[group_key]['auction_to_components'][auction_name] = []
                groups[group_key]['auction_to_components'][auction_name].append(component_id)
        
        groups[group_key]['components'].append(comp)
    
    # Convert to list and add count
    result = []
    for key, group in groups.items():
        group['count'] = len(group['components'])
        group['cmu_ids'] = list(group['cmu_ids'])
        
        # Sort auction names by year in descending order (newest first)
        def extract_year(auction_name):
            year_match = re.search(r'(\d{4})[-/]?(\d{2,4})', auction_name)
            if year_match:
                try:
                    return int(year_match.group(1))
                except ValueError:
                    return 0
            return 0
        
        # Convert to list and sort in descending order
        auction_names_list = list(group['auction_names'])
        auction_names_list.sort(key=extract_year, reverse=True)
        group['auction_names'] = auction_names_list
        
        result.append(group)
        
    # Sort the result list alphabetically by location if requested via query params
    request = None
    try:
        from django.core.handlers.wsgi import WSGIRequest
        import inspect
        for frame in inspect.stack():
            if 'request' in frame.frame.f_locals:
                possible_request = frame.frame.f_locals['request']
                if isinstance(possible_request, WSGIRequest):
                    request = possible_request
                    break
        
        if request and request.GET.get('sort_by') == 'location':
            sort_order = request.GET.get('sort_order', 'asc').lower()
            logger.info(f"Applying additional location sort to grouped results ({sort_order})")
            
            # Sort the groups by location (safely handling None values)
            reverse_sort = (sort_order == 'desc')
            # Use a lambda that safely handles None or empty location values
            result.sort(key=lambda x: (x['location'] or '').lower(), reverse=reverse_sort)
    except Exception as e:
        logger.error(f"Error applying location sort to grouped results: {e}")
    
    return result

@register.filter(name='is_dict')
def is_dict(value):
    """Check if a value is a dictionary."""
    return isinstance(value, dict)

@register.filter(name='is_list')
def is_list(value):
    """Check if a value is a list."""
    return isinstance(value, list)