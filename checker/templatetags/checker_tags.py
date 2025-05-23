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