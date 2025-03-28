import logging
from django.http import JsonResponse
from django.shortcuts import render
from .services.data_access import fetch_components_for_cmu_id, analyze_component_duplicates

logger = logging.getLogger(__name__)

def debug_component_duplicates(request, cmu_id):
    """
    Debug view that analyzes a CMU ID for duplicate components.
    
    Args:
        request: HTTP request
        cmu_id: The CMU ID to analyze
        
    Returns:
        Rendered template with duplicate analysis
    """
    # Check if we should return JSON
    format_json = request.GET.get('format') == 'json'
    
    # Fetch all components for this CMU ID
    components, api_time = fetch_components_for_cmu_id(cmu_id)
    
    # Analyze for duplicates
    analysis = analyze_component_duplicates(components)
    
    # Add CMU ID to the analysis
    analysis['cmu_id'] = cmu_id
    analysis['component_count'] = len(components) if components else 0
    analysis['api_time'] = api_time
    
    if format_json:
        return JsonResponse(analysis)
    
    # Render a template with the results
    return render(request, "checker/debug_duplicates.html", {
        "cmu_id": cmu_id,
        "analysis": analysis,
        "components": components,
        "api_time": api_time
    }) 