from django import template
import json
from django.utils.safestring import mark_safe
from django.core.serializers.json import DjangoJSONEncoder
from ..utils import normalize # Import normalize function


register = template.Library()

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
def jsonify(obj):
    """
    Safely dump Python object to JSON string, suitable for <pre> tags.
    """
    try:
        # Use DjangoJSONEncoder to handle dates, decimals etc.
        json_string = json.dumps(obj, cls=DjangoJSONEncoder, indent=2)
        return mark_safe(json_string)
    except Exception:
        return "(Error converting to JSON)"    

@register.filter(name='replace_underscores')
def replace_underscores(value):
    return str(value).replace('_', ' ')

@register.filter(name='normalize')
def normalize_filter(value):
    """Template filter to normalize a string (lowercase, remove punctuation/spaces)."""
    return normalize(value)

@register.filter(name='urlencode')
def urlencode_filter(value):
    """URL encodes a string, handling spaces and special characters."""
    import urllib.parse
    return urllib.parse.quote(str(value))