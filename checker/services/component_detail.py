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

        # Remove empty sections (including Technical Details if it becomes empty)
        organized_data = {k: v for k, v in organized_data.items() if v}

        # --- Explicitly get values for template --- 
        # Component Capacity
        component_derated_capacity = target_component_obj.derated_capacity_mw 
        # CMU Registry Data (use .get() with defaults)
        registry_derated_capacity = raw_cmu_data.get("De-Rated Capacity") if raw_cmu_data else None
        # Use CORRECTED keys based on sample data
        connection_capacity = raw_cmu_data.get("Connection / DSR Capacity") if raw_cmu_data else None 
        anticipated_capacity = raw_cmu_data.get("Anticipated De-Rated Capacity") if raw_cmu_data else None
        parent_company = raw_cmu_data.get("Parent Company") if raw_cmu_data else None
        trading_email = raw_cmu_data.get("Secondary Trading Contact - Email") if raw_cmu_data else None
        trading_phone = raw_cmu_data.get("Secondary Trading Contact - Telephone") if raw_cmu_data else None
        # --- End explicit value fetching --- 

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
            # Pass individual values for clarity
            "component_derated_capacity": component_derated_capacity, 
            "registry_derated_capacity": registry_derated_capacity,  
            "connection_capacity": connection_capacity,
            "anticipated_capacity": anticipated_capacity,
            "parent_company": parent_company,
            "trading_email": trading_email,
            "trading_phone": trading_phone,
        })

    except Exception as e:
        logger.error(f"Error in get_component_details: {str(e)}")
        logger.error(traceback.format_exc())
        return render(request, "checker/component_detail.html", {
            "error": f"Error loading component details: {str(e)}",
            "component": None,
            "traceback": traceback.format_exc() if request.GET.get("debug") else None
        })