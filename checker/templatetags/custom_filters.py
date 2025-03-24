from django import template


register = template.Library()

@register.filter
def get_item(dictionary, key):
    """Get an item from a dictionary using bracket notation."""
    return dictionary.get(key, '')

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