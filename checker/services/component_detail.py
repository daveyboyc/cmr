import logging
import traceback
from django.shortcuts import render
from django.core.cache import cache
from ..utils import normalize
from .data_access import fetch_components_for_cmu_id, get_cmu_data_by_id

logger = logging.getLogger(__name__)


def get_component_details(request, component_id):
    """
    View function for component details page.
    Displays all available information about a specific component.

    Args:
        request: The HTTP request
        component_id: The ID of the component in format "cmu_id_normalized_location"
    """
    try:
        # Parse component_id to extract CMU ID and location
        parts = component_id.split('_', 1)
        if len(parts) != 2:
            return render(request, "checker/component_detail.html", {
                "error": f"Invalid component ID format: {component_id}",
                "component": None
            })

        cmu_id, normalized_location = parts
        logger.info(f"Looking up component with CMU ID: {cmu_id}, normalized location: {normalized_location}")

        # Fetch components for this CMU
        components, api_time = fetch_components_for_cmu_id(cmu_id)
        
        # Fetch additional CMU data from cmu_data.json
        additional_cmu_data = get_cmu_data_by_id(cmu_id)
        logger.info(f"Additional CMU data found: {additional_cmu_data is not None}")

        if not components:
            logger.warning(f"No components found for CMU ID: {cmu_id}")
            return render(request, "checker/component_detail.html", {
                "error": f"No components found for CMU ID: {cmu_id}",
                "component": None,
                "api_time": api_time,
                "additional_cmu_data": additional_cmu_data
            })

        # Find the specific component that matches the normalized location
        target_component = None
        for component in components:
            location = component.get("Location and Post Code", "")
            if normalize(location) == normalized_location:
                target_component = component
                logger.info(f"Found matching component with location: {location}")
                break

        if not target_component:
            logger.warning(f"Component with location {normalized_location} not found for CMU ID {cmu_id}")
            return render(request, "checker/component_detail.html", {
                "error": f"Component with location '{normalized_location}' not found for CMU ID '{cmu_id}'",
                "component": None,
                "api_time": api_time,
                "cmu_id": cmu_id,
                "all_components": components,  # Provide all components in case they want to browse
                "additional_cmu_data": additional_cmu_data
            })

        # Add CMU ID to the component data if not already present
        if "CMU ID" not in target_component:
            target_component["CMU ID"] = cmu_id

        # Get additional data that might be available from the component's JSON
        # This can be expanded as more fields become available
        component_detail = {
            "CMU ID": cmu_id,
            "Location": target_component.get("Location and Post Code", "N/A"),
            "Description": target_component.get("Description of CMU Components", "N/A"),
            "Technology": target_component.get("Generating Technology Class", "N/A"),
            "Auction": target_component.get("Auction Name", "N/A"),
            "Delivery Year": target_component.get("Delivery Year", "N/A"),
            "Status": target_component.get("Status", "N/A"),
            "Company": target_component.get("Company Name", "N/A"),
            # Add all other available fields from the component
            **{k: v for k, v in target_component.items() if k not in [
                "CMU ID", "Location and Post Code", "Description of CMU Components",
                "Generating Technology Class", "Auction Name", "Delivery Year", "Status", "Company Name"
            ]}
        }

        # Organize fields into categories for better display
        organized_data = {
            "Basic Information": {
                "CMU ID": component_detail.get("CMU ID", "N/A"),
                "Location": component_detail.get("Location", "N/A"),
                "Description": component_detail.get("Description", "N/A"),
                "Company": component_detail.get("Company", "N/A"),
            },
            "Technical Details": {
                "Technology": component_detail.get("Technology", "N/A"),
                "Generation Type": component_detail.get("Generation Type", "N/A"),
                "Connection Type": component_detail.get("Connection Type", "N/A"),
                "Capacity (MW)": component_detail.get("Capacity (MW)", "N/A"),
                "Type": component_detail.get("Type", "N/A"),
                # Add any other technical fields here
            },
            "Auction Information": {
                "Auction": component_detail.get("Auction", "N/A"),
                "Delivery Year": component_detail.get("Delivery Year", "N/A"),
                "Status": component_detail.get("Status", "N/A"),
                "Clearing Price": component_detail.get("Clearing Price", "N/A"),
                # Add any auction-related fields here
            },
            "Additional Information": {}
        }
        
        # Add a separate section for additional CMU data if available
        if additional_cmu_data:
            cmu_data_section = {}
            
            # Add relevant fields from additional_cmu_data
            # Include fields that would be useful for users
            important_fields = [
                "Auction", "Type", "Delivery Year", "Name of Applicant", 
                "Agent Name", "CM Unit Name", "Low Carbon Exclusion CMU",
                "Agreement End Date", "Agreement Start Date", "Auction Result Date",
                "Capacity Obligation (MW)", "Capacity Agreement", "Contact Name",
                "CM Trading Contact Email", "CM Trading Contact Phone",
                "Secondary Trading", "Price Cap (£/kW)", "Price Taker Threshold (£/kW)"
            ]
            
            for field in important_fields:
                if field in additional_cmu_data and additional_cmu_data[field] not in ["", "N/A", None]:
                    cmu_data_section[field] = additional_cmu_data[field]
            
            if cmu_data_section:
                organized_data["CMU Registry Data"] = cmu_data_section

        # Add remaining fields to Additional Information
        for k, v in component_detail.items():
            if k not in [field for section in organized_data.values() for field in section.keys()]:
                organized_data["Additional Information"][k] = v

        # Remove empty sections
        organized_data = {k: v for k, v in organized_data.items() if v}

        logger.info(
            f"Rendering component detail page for CMU ID: {cmu_id}, Location: {target_component.get('Location and Post Code', 'N/A')}")
        return render(request, "checker/component_detail.html", {
            "component": target_component,
            "component_detail": component_detail,
            "organized_data": organized_data,
            "api_time": api_time,
            "cmu_id": cmu_id,
            "location": target_component.get("Location and Post Code", "N/A"),
            "additional_cmu_data": additional_cmu_data
        })

    except Exception as e:
        logger.error(f"Error in get_component_details: {str(e)}")
        logger.error(traceback.format_exc())
        return render(request, "checker/component_detail.html", {
            "error": f"Error loading component details: {str(e)}",
            "component": None,
            "traceback": traceback.format_exc() if request.GET.get("debug") else None
        })