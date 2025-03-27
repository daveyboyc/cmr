import logging
import traceback
from django.shortcuts import render
from django.core.cache import cache
from ..utils import normalize
from ..models import Component
from .data_access import fetch_components_for_cmu_id, get_cmu_data_by_id

logger = logging.getLogger(__name__)


def get_component_details(request, component_id):
    """
    View function for component details page.
    Displays all available information about a specific component.
    Now uses the database as primary data source.

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
        
        # Clean up the normalized location if it starts with a number_
        if normalized_location and normalized_location.split('_', 1)[0].isdigit():
            normalized_location = normalized_location.split('_', 1)[1] if '_' in normalized_location else normalized_location
            
        logger.info(f"Looking up component with CMU ID: {cmu_id}, normalized location: {normalized_location}")

        # DATABASE FIRST: Try to get components from the database
        api_time = 0
        target_component = None
        
        try:
            # Query for components with this CMU ID
            db_components = Component.objects.filter(cmu_id=cmu_id)
            
            if db_components.exists():
                logger.info(f"Found {db_components.count()} components in database for CMU ID: {cmu_id}")
                
                # Look for a matching location
                for component in db_components:
                    location = component.location
                    normalized_component_location = normalize(location)
                    
                    # Try exact match first
                    if normalized_component_location == normalized_location:
                        # Convert database object to dictionary for template
                        target_component = {
                            "CMU ID": component.cmu_id,
                            "Location and Post Code": component.location,
                            "Description of CMU Components": component.description,
                            "Generating Technology Class": component.technology,
                            "Company Name": component.company_name,
                            "Auction Name": component.auction_name,
                            "Delivery Year": component.delivery_year,
                            "Status": component.status,
                            "Type": component.type
                        }
                        
                        # Add all additional data if available
                        if component.additional_data:
                            for key, value in component.additional_data.items():
                                if key not in target_component:
                                    target_component[key] = value
                        
                        logger.info(f"Found exact matching component in database with location: {location}")
                        break
                        
                    # If not found, try relaxed matching where one contains the other
                    if (normalized_component_location and normalized_location and 
                        (normalized_component_location in normalized_location or 
                         normalized_location in normalized_component_location)):
                        # Convert database object to dictionary for template
                        target_component = {
                            "CMU ID": component.cmu_id,
                            "Location and Post Code": component.location,
                            "Description of CMU Components": component.description,
                            "Generating Technology Class": component.technology,
                            "Company Name": component.company_name,
                            "Auction Name": component.auction_name,
                            "Delivery Year": component.delivery_year,
                            "Status": component.status,
                            "Type": component.type
                        }
                        
                        # Add all additional data if available
                        if component.additional_data:
                            for key, value in component.additional_data.items():
                                if key not in target_component:
                                    target_component[key] = value
                        
                        logger.info(f"Found partial matching component in database with location: {location}")
                        break
                
                # If no matching component found in database, prepare filtered components list
                if not target_component:
                    filtered_components = []
                    seen_locations = set()
                    
                    for component in db_components:
                        location = component.location
                        if location and location not in seen_locations:
                            seen_locations.add(location)
                            # Convert database object to dictionary for template
                            comp_dict = {
                                "CMU ID": component.cmu_id,
                                "Location and Post Code": component.location,
                                "Description of CMU Components": component.description,
                                "Generating Technology Class": component.technology,
                                "Company Name": component.company_name,
                                "Auction Name": component.auction_name,
                                "Delivery Year": component.delivery_year,
                                "Status": component.status,
                                "Type": component.type
                            }
                            filtered_components.append(comp_dict)
            
        except Exception as db_error:
            logger.error(f"Error querying database: {str(db_error)}")
            logger.error(traceback.format_exc())
            # Fall through to API fetching

        # If not found in database, try the API fetch method as fallback
        if not target_component:
            logger.info(f"Component not found in database, trying API fallback")
            components, api_time = fetch_components_for_cmu_id(cmu_id)
            
            if not components:
                logger.warning(f"No components found for CMU ID: {cmu_id}")
                return render(request, "checker/component_detail.html", {
                    "error": f"No components found for CMU ID: {cmu_id}",
                    "component": None,
                    "api_time": api_time,
                    "additional_cmu_data": None
                })
            
            # Find the specific component that matches the normalized location
            for component in components:
                location = component.get("Location and Post Code", "")
                normalized_component_location = normalize(location)
                
                # Try exact match first
                if normalized_component_location == normalized_location:
                    target_component = component
                    logger.info(f"Found exact matching component from API with location: {location}")
                    break
                    
                # If not found, try relaxed matching where one contains the other
                if (normalized_component_location and normalized_location and 
                    (normalized_component_location in normalized_location or 
                     normalized_location in normalized_component_location)):
                    target_component = component
                    logger.info(f"Found partial matching component from API with location: {location}")
                    break

            # If component still not found, prepare filtered components list
            if not target_component:
                filtered_components = []
                seen_locations = set()
                
                for comp in components:
                    location = comp.get("Location and Post Code", "")
                    if location and location not in seen_locations:
                        seen_locations.add(location)
                        filtered_components.append(comp)

        # If still no target component found, show error with available components
        if not target_component:
            logger.warning(f"Component with location {normalized_location} not found for CMU ID {cmu_id}")
            
            return render(request, "checker/component_detail.html", {
                "error": f"Component with location '{normalized_location}' not found for CMU ID '{cmu_id}'",
                "component": None,
                "api_time": api_time,
                "cmu_id": cmu_id,
                "all_components": filtered_components if 'filtered_components' in locals() else []
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

        # Fetch additional CMU data from cmu_data.json
        additional_cmu_data = get_cmu_data_by_id(cmu_id)
        logger.info(f"Additional CMU data found: {additional_cmu_data is not None}")

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
            "additional_cmu_data": additional_cmu_data,
            "source": "database" if api_time == 0 else "api"
        })

    except Exception as e:
        logger.error(f"Error in get_component_details: {str(e)}")
        logger.error(traceback.format_exc())
        return render(request, "checker/component_detail.html", {
            "error": f"Error loading component details: {str(e)}",
            "component": None,
            "traceback": traceback.format_exc() if request.GET.get("debug") else None
        })