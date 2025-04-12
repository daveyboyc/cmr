import logging
import traceback
from django.shortcuts import render
from django.core.cache import cache
from ..utils import normalize
from ..models import Component
from ..models import CMURegistry

logger = logging.getLogger(__name__)


def get_component_details(request, pk):
    """
    View function for component details page.
    Displays all available information about a specific component using its database ID.

    Args:
        request: The HTTP request
        pk: The primary key (database ID) of the component
    """
    try:
        logger.info(f"Looking up component with database ID (pk): {pk}")

        # DATABASE FIRST: Get the component directly using primary key
        target_component_obj = Component.objects.get(pk=pk)
        api_time = 0 # Assume fetched from DB initially

        if not target_component_obj:
             # This case should ideally not happen if the link was generated correctly
            logger.error(f"Component with pk={pk} not found in database.")
            return render(request, "checker/component_detail.html", {
                "error": f"Component with ID {pk} not found.",
                "component": None
            })
        
        # Convert the database object to the dictionary format needed by the template
        target_component = {
            "CMU ID": target_component_obj.cmu_id,
            "Location and Post Code": target_component_obj.location,
            "Description of CMU Components": target_component_obj.description,
            "Generating Technology Class": target_component_obj.technology,
            "Company Name": target_component_obj.company_name,
            "Auction Name": target_component_obj.auction_name,
            "Delivery Year": target_component_obj.delivery_year,
            "Status": target_component_obj.status,
            "Type": target_component_obj.type,
            "_id": target_component_obj.component_id # Keep original ID if needed
        }
        
        # explicitly get the raw component data
        raw_component_data = target_component_obj.additional_data or {}

        # Add all additional data if available (This merging can be kept or removed, 
        # but we now have the raw data separately anyway)
        if target_component_obj.additional_data:
            for key, value in target_component_obj.additional_data.items():
                if key not in target_component:
                    target_component[key] = value

        cmu_id = target_component_obj.cmu_id # Get CMU ID for fetching additional data

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

        # Fetch additional CMU data from CMURegistry model
        raw_cmu_data = None
        cmu_registry_entry = CMURegistry.objects.filter(cmu_id=cmu_id).first()
        if cmu_registry_entry:
            raw_cmu_data = cmu_registry_entry.raw_data
            logger.info(f"Raw CMU data found in CMURegistry for {cmu_id}")
        else:
            logger.warning(f"No entry found in CMURegistry for CMU ID: {cmu_id}")

        # Organize fields into categories for better display
        organized_data = {
            "Basic Information": {
                "Location": target_component.get("Location and Post Code", "N/A"),
                "Description": target_component.get("Description of CMU Components", "N/A"),
                "Company": target_component.get("Company Name", "N/A"),
            },
            "Technical Details": {
                "Technology": target_component.get("Generating Technology Class", "N/A"),
                "Generation Type": target_component.get("Generation Type", "N/A"),
                "Connection Type": target_component.get("Connection Type", "N/A"),
                "De-Rated Capacity": target_component.get("De-Rated Capacity", "N/A"),
                "Connection / DSR Capacity": target_component.get("Connection / DSR Capacity", "N/A"),
                "Type": target_component.get("Type", "N/A"),
            },
            "Auction Information": {
                "Auction": target_component.get("Auction Name", "N/A"),
                "Delivery Year": target_component.get("Delivery Year", "N/A"),
                "Status": target_component.get("Status", "N/A"),
                "Clearing Price": target_component.get("Clearing Price", "N/A"),
            },
            "Additional Information": {}
        }
        
        # Add a separate section for additional CMU data if available
        if raw_cmu_data:
            cmu_data_section = {}
            important_fields = [
                "Auction", "Type", "Delivery Year", "Name of Applicant", 
                "Agent Name", "CM Unit Name", "Low Carbon Exclusion CMU",
                "Agreement End Date", "Agreement Start Date", "Auction Result Date",
                "Capacity Obligation (MW)", "Capacity Agreement", "Contact Name",
                "CM Trading Contact Email", "CM Trading Contact Phone",
                "Secondary Trading", "Price Cap (£/kW)", "Price Taker Threshold (£/kW)"
            ]
            for field in important_fields:
                if field in raw_cmu_data and raw_cmu_data[field] not in ["", "N/A", None]:
                    cmu_data_section[field] = raw_cmu_data[field]
            if cmu_data_section:
                organized_data["CMU Registry Data"] = cmu_data_section

        # Add remaining fields to Additional Information
        for k, v in target_component.items():
            # Check if key exists in any section before adding to Additional Info
            key_exists = False
            for section_data in organized_data.values():
                if k in section_data:
                    key_exists = True
                    break
            if not key_exists:
                organized_data["Additional Information"][k] = v

        # -- Add De-Rated Capacity Prioritization Logic --
        capacity_from_registry = False
        missing_values = {None, "N/A", ""}
        component_capacity = target_component.get("De-Rated Capacity") # Get from component dict
        registry_capacity = raw_cmu_data.get("De-Rated Capacity") if raw_cmu_data else None # Get from registry dict

        # Check if component capacity is missing/invalid
        is_component_capacity_missing = component_capacity in missing_values
        try:
            # Also treat non-float values as missing
            if not is_component_capacity_missing:
                float(component_capacity)
        except (ValueError, TypeError):
            is_component_capacity_missing = True
            
        # Check if registry capacity is valid
        is_registry_capacity_valid = registry_capacity not in missing_values
        if is_registry_capacity_valid:
            try:
                float(registry_capacity)
            except (ValueError, TypeError):
                is_registry_capacity_valid = False

        # If component capacity is missing but registry is valid, use registry value
        final_capacity_value = component_capacity # Default to component value
        if is_component_capacity_missing and is_registry_capacity_valid:
            final_capacity_value = registry_capacity
            capacity_from_registry = True
            logger.info(f"Component {pk}: Using De-Rated Capacity '{registry_capacity}' from CMU Registry.")
            
        # Update the Technical Details section with the prioritized value
        organized_data.setdefault("Technical Details", {})["De-Rated Capacity"] = final_capacity_value
        # -- End Prioritization Logic --

        # Remove empty sections
        organized_data = {k: v for k, v in organized_data.items() if v}

        logger.info(
            f"Rendering component detail page for Component PK: {pk}, CMU ID: {cmu_id}")
        return render(request, "checker/component_detail.html", {
            "component": target_component,
            "component_detail": component_detail,
            "organized_data": organized_data,
            "api_time": api_time,
            "cmu_id": cmu_id,
            "location": target_component.get("Location and Post Code", "N/A"),
            "raw_component_data": raw_component_data,
            "raw_cmu_data": raw_cmu_data,
            "source": "database",
            "capacity_from_registry": capacity_from_registry # Pass flag to template
        })

    except Exception as e:
        logger.error(f"Error in get_component_details: {str(e)}")
        logger.error(traceback.format_exc())
        return render(request, "checker/component_detail.html", {
            "error": f"Error loading component details: {str(e)}",
            "component": None,
            "traceback": traceback.format_exc() if request.GET.get("debug") else None
        })