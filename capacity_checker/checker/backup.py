from django.shortcuts import render, redirect
import pandas as pd
import requests
from django.core.cache import cache
import time
import json
import os
from django.conf import settings
import urllib.parse
from django.http import HttpResponse


def normalize(text):
    """Lowercase and remove all whitespace."""
    if not isinstance(text, str):
        return ""
    return "".join(text.lower().split())


def get_component_data_from_json(cmu_id):
    if not cmu_id:
        return None

    # Get first character of CMU ID
    prefix = cmu_id[0].upper() if cmu_id else "0"

    # Path for the file that would contain this CMU ID
    json_dir = os.path.join(settings.BASE_DIR, 'json_data')
    json_path = os.path.join(json_dir, f'components_{prefix}.json')

    # Check if the file exists
    if not os.path.exists(json_path):
        # Try the old path as fallback
        old_json_path = os.path.join(settings.BASE_DIR, 'component_data.json')
        if os.path.exists(old_json_path):
            try:
                with open(old_json_path, 'r') as f:
                    all_components = json.load(f)
                return all_components.get(cmu_id)
            except Exception as e:
                print(f"Error reading component data from old JSON: {e}")
        return None

    try:
        with open(json_path, 'r') as f:
            all_components = json.load(f)

        # Try exact match first
        if cmu_id in all_components:
            return all_components[cmu_id]

        # If not found, try case-insensitive match
        for file_cmu_id in all_components.keys():
            if file_cmu_id.lower() == cmu_id.lower():
                print(f"Found case-insensitive match: {file_cmu_id} for {cmu_id}")
                return all_components[file_cmu_id]

        # If still not found, return None
        return None
    except Exception as e:
        print(f"Error reading component data from JSON: {e}")
        return None


def ensure_component_has_company_name(component, cmu_id):
    """
    Makes sure a component has the Company Name field.
    Returns the component with Company Name added if needed.
    """
    # If component already has Company Name, return it unchanged
    if component.get("Company Name", ""):
        return component

    # Get the company name from the mapping
    cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
    company_name = cmu_to_company_mapping.get(cmu_id, "")

    # Try case-insensitive match if not found
    if not company_name:
        for mapping_cmu_id, mapping_company in cmu_to_company_mapping.items():
            if mapping_cmu_id.lower() == cmu_id.lower():
                company_name = mapping_company
                break

    if company_name:
        # Make a copy to avoid modifying the original
        component = component.copy()
        component["Company Name"] = company_name

    return component


def matched_component(component1, component2):
    """Helper function to determine if two components match"""
    # Define key fields to compare
    key_fields = ["CMU ID", "Location and Post Code", "Description of CMU Components"]

    # Check if all key fields match
    for field in key_fields:
        if component1.get(field) != component2.get(field):
            return False

    return True


def save_component_data_to_json(cmu_id, components):
    if not cmu_id:
        return False

    # Get first character of CMU ID as folder name
    prefix = cmu_id[0].upper()

    # Create a directory for split files if it doesn't exist
    json_dir = os.path.join(settings.BASE_DIR, 'json_data')
    os.makedirs(json_dir, exist_ok=True)

    # Path for this specific CMU's components
    json_path = os.path.join(json_dir, f'components_{prefix}.json')

    # Initialize or load existing data
    all_components = {}
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r') as f:
                all_components = json.load(f)
        except Exception as e:
            print(f"Error reading existing component data: {e}")

    # Get company name from mapping cache
    cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
    company_name = cmu_to_company_mapping.get(cmu_id, "")

    # Try case-insensitive match if needed
    if not company_name:
        for mapping_cmu_id, mapping_company in cmu_to_company_mapping.items():
            if mapping_cmu_id.lower() == cmu_id.lower():
                company_name = mapping_company
                break

    # Make sure each component has the Company Name field
    updated_components = []
    for component in components:
        if "Company Name" not in component and company_name:
            component = component.copy()  # Make a copy to avoid modifying the original
            component["Company Name"] = company_name

        # Add CMU ID to the component for reference
        if "CMU ID" not in component:
            component = component.copy()
            component["CMU ID"] = cmu_id

        updated_components.append(component)

    # Add or update the components for this CMU ID
    all_components[cmu_id] = updated_components

    # Write the updated data back to the file
    try:
        with open(json_path, 'w') as f:
            json.dump(all_components, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving component data to JSON: {e}")
        return False


def get_cmu_data_from_json():
    """
    Get all CMU data from the JSON file.
    If the JSON doesn't exist, returns None.
    """
    json_path = os.path.join(settings.BASE_DIR, 'cmu_data.json')

    # Check if the file exists
    if not os.path.exists(json_path):
        return None

    try:
        with open(json_path, 'r') as f:
            all_cmu_data = json.load(f)
        return all_cmu_data
    except Exception as e:
        print(f"Error reading CMU data from JSON: {e}")
        return None


def save_cmu_data_to_json(cmu_records):
    """
    Save all CMU records to a JSON file.
    Creates the file if it doesn't exist, otherwise replaces it.
    """
    json_path = os.path.join(settings.BASE_DIR, 'cmu_data.json')

    # Write the data to the file
    try:
        with open(json_path, 'w') as f:
            json.dump(cmu_records, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving CMU data to JSON: {e}")
        return False


def fetch_all_cmu_records(limit=1000):
    """
    Fetch all CMU records from the API.
    First checks if we have them stored in JSON.
    """
    # Check if we have the data in our JSON file
    json_cmu_data = get_cmu_data_from_json()
    if json_cmu_data is not None:
        print(f"Using JSON-stored CMU data, found {len(json_cmu_data)} records")
        return json_cmu_data, 0

    params = {
        "resource_id": "25a5fa2e-873d-41c5-8aaf-fbc2b06d79e6",
        "limit": limit,
        "offset": 0
    }
    all_records = []
    total_time = 0
    while True:
        start_time = time.time()
        response = requests.get(
            "https://api.neso.energy/api/3/action/datastore_search",
            params=params,
            timeout=20
        )
        response.raise_for_status()
        total_time += time.time() - start_time
        result = response.json()["result"]
        records = result["records"]
        all_records.extend(records)
        if len(all_records) >= result["total"]:
            break
        params["offset"] += limit

    # Save to JSON for future use
    save_cmu_data_to_json(all_records)

    return all_records, total_time


def fetch_components_for_cmu_id(cmu_id, limit=100):
    """Fetch components for a specific CMU ID."""
    if not cmu_id:
        return [], 0

    # First check if we already have cached data for this CMU ID
    components_cache_key = f"components_for_cmu_{cmu_id.lower()}"  # ALWAYS USE LOWERCASE FOR CACHE KEYS
    cached_components = cache.get(components_cache_key)
    if cached_components is not None:
        print(f"Using cached components for {cmu_id}, found {len(cached_components)}")
        return cached_components, 0

    # Check if we have the data in our JSON file - CASE-INSENSITIVE
    json_components = get_component_data_from_json(cmu_id)
    if json_components is not None:
        print(f"Using JSON-stored components for {cmu_id}, found {len(json_components)}")
        # Also update the cache
        cache.set(components_cache_key, json_components, 3600)
        return json_components, 0

    print(f"Fetching components for {cmu_id} from API")
    params = {
        "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
        "limit": limit,
        "q": cmu_id
    }
    start_time = time.time()
    try:
        response = requests.get(
            "https://api.neso.energy/api/3/action/datastore_search",
            params=params,
            timeout=10
        )
        response.raise_for_status()
        elapsed = time.time() - start_time
        result = response.json()["result"]
        components = result.get("records", [])

        print(f"Found {len(components)} components for {cmu_id}")

        # Add company name to each component
        cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})

        # Try exact match first
        company_name = cmu_to_company_mapping.get(cmu_id, "")

        # If not found, try case-insensitive match
        if not company_name:
            for cache_cmu_id, cache_company in cmu_to_company_mapping.items():
                if cache_cmu_id.lower() == cmu_id.lower():
                    company_name = cache_company
                    break

        if company_name:
            for component in components:
                if "Company Name" not in component:
                    component["Company Name"] = company_name

        # Cache the results for this CMU ID - always lowercase in cache
        cache.set(components_cache_key, components, 3600)  # Cache for 1 hour

        # Also save to JSON for persistence
        save_component_data_to_json(cmu_id, components)

        return components, elapsed
    except Exception as e:
        print(f"Error fetching components for CMU ID {cmu_id}: {e}")
        elapsed = time.time() - start_time
        return [], elapsed


def fetch_limited_components(query, limit=1000):
    params = {
        "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
        "limit": limit,
        "q": query
    }
    start_time = time.time()
    try:
        response = requests.get(
            "https://api.neso.energy/api/3/action/datastore_search",
            params=params,
            timeout=20
        )
        response.raise_for_status()
        elapsed = time.time() - start_time
        result = response.json()["result"]
        return result.get("records", []), elapsed
    except Exception as e:
        print(f"Error fetching components: {e}")
        elapsed = time.time() - start_time
        return [], elapsed


def search_companies(request):
    # Add this test function immediately
    def test_component_lookup():
        test_cmu_id = 'END003'  # One we know exists
        components_cache_key = f"components_for_cmu_{test_cmu_id}"  # FIXED: Use correct cache key format
        components = cache.get(components_cache_key)
        if components:
            print(f"TEST: Found {len(components)} components for {test_cmu_id}")
            locations = set()
            for comp in components:
                loc = comp.get("Location and Post Code", "")
                if loc:
                    locations.add(loc)
            print(f"TEST: Found locations: {locations}")
        else:
            print(f"TEST: No components in cache for {test_cmu_id}")
            # Try from JSON as fallback
            json_components = get_component_data_from_json(test_cmu_id)
            if json_components:
                print(f"TEST: Found {len(json_components)} components in JSON for {test_cmu_id}")
                locations = set()
                for comp in json_components:
                    loc = comp.get("Location and Post Code", "")
                    if loc:
                        locations.add(loc)
                print(f"TEST: Found locations in JSON: {locations}")
            else:
                print(f"TEST: No components in JSON for {test_cmu_id}")

    # Call the test function
    test_component_lookup()

    # Rest of your function continues...
    results = {}
    error_message = None
    api_time = 0
    query = request.GET.get("q", "").strip()  # Get query early to use it throughout

    if request.method == "GET":
        if "search_results" in request.session and not query:
            # Only use session results if no new query is provided
            results = request.session.pop("search_results")
            record_count = request.session.pop("record_count", None)
            api_time = request.session.pop("api_time", 0)
            last_query = request.session.pop("last_query", "")
            return render(request, "checker/search.html", {
                "results": results,
                "record_count": record_count,
                "error": error_message,
                "api_time": api_time,
                "query": last_query,  # Pass the last query back to the template
            })
        elif query:
            norm_query = normalize(query)

            cmu_df = cache.get("cmu_df")
            if cmu_df is None:
                try:
                    all_records, api_time = fetch_all_cmu_records(limit=5000)
                    cmu_df = pd.DataFrame(all_records)

                    # Set up necessary columns
                    cmu_df["Name of Applicant"] = cmu_df.get("Name of Applicant", pd.Series()).fillna("").astype(str)
                    cmu_df["Parent Company"] = cmu_df.get("Parent Company", pd.Series()).fillna("").astype(str)

                    # Identify CMU ID field
                    possible_cmu_id_fields = ["CMU ID", "cmu_id", "CMU_ID", "cmuId", "id", "identifier", "ID"]
                    cmu_id_field = next((field for field in possible_cmu_id_fields if field in cmu_df.columns), None)
                    if cmu_id_field:
                        cmu_df["CMU ID"] = cmu_df[cmu_id_field].fillna("N/A").astype(str)
                    else:
                        cmu_df["CMU ID"] = "N/A"

                    # Set Full Name
                    cmu_df["Full Name"] = cmu_df["Name of Applicant"].str.strip()
                    cmu_df["Full Name"] = cmu_df.apply(
                        lambda row: row["Full Name"] if row["Full Name"] else row["Parent Company"],
                        axis=1
                    )

                    # Create complete mapping of all CMU IDs to company names
                    cmu_to_company_mapping = {}
                    for _, row in cmu_df.iterrows():
                        cmu_id = row.get("CMU ID", "").strip()
                        if cmu_id and cmu_id != "N/A":
                            cmu_to_company_mapping[cmu_id] = row.get("Full Name", "")

                    cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 3600)
                    cache.set("cmu_df", cmu_df, 900)
                except requests.exceptions.RequestException as e:
                    error_message = f"Error fetching CMU data: {e}"
                    return render(request, "checker/search.html", {
                        "error": error_message,
                        "api_time": api_time,
                        "query": query,  # Keep the query in the search box
                    })

            record_count = len(cmu_df)

            cmu_df["Name of Applicant"] = cmu_df.get("Name of Applicant", pd.Series()).fillna("").astype(str)
            cmu_df["Parent Company"] = cmu_df.get("Parent Company", pd.Series()).fillna("").astype(str)
            cmu_df["Delivery Year"] = cmu_df.get("Delivery Year", pd.Series()).fillna("").astype(str)

            possible_cmu_id_fields = ["CMU ID", "cmu_id", "CMU_ID", "cmuId", "id", "identifier", "ID"]
            cmu_id_field = next((field for field in possible_cmu_id_fields if field in cmu_df.columns), None)
            if cmu_id_field:
                cmu_df["CMU ID"] = cmu_df[cmu_id_field].fillna("N/A").astype(str)
            else:
                cmu_df["CMU ID"] = "N/A"

            cmu_df["Full Name"] = cmu_df["Name of Applicant"].str.strip()
            cmu_df["Full Name"] = cmu_df.apply(
                lambda row: row["Full Name"] if row["Full Name"] else row["Parent Company"],
                axis=1
            )

            cmu_df["Normalized Full Name"] = cmu_df["Full Name"].apply(normalize)
            cmu_df["Normalized CMU ID"] = cmu_df["CMU ID"].apply(normalize)

            # Try direct CMU ID match first (case-insensitive)
            cmu_id_matches = cmu_df[cmu_df["Normalized CMU ID"].str.contains(norm_query, regex=False, na=False)]

            # Then look for company name matches
            company_matches = cmu_df[cmu_df["Normalized Full Name"].str.contains(norm_query, regex=False, na=False)]

            # Combine the matches
            matching_records = pd.concat([cmu_id_matches, company_matches]).drop_duplicates()

            # Cache the mapping of CMU IDs to company names for later use in components search
            cmu_to_company_mapping = {}
            for _, row in matching_records.iterrows():
                cmu_id = row.get("CMU ID", "").strip()
                if cmu_id and cmu_id != "N/A":
                    cmu_to_company_mapping[cmu_id] = row.get("Full Name", "")

            # Update the existing cache or create a new one
            existing_mapping = cache.get("cmu_to_company_mapping", {})
            existing_mapping.update(cmu_to_company_mapping)
            cache.set("cmu_to_company_mapping", existing_mapping, 3600)  # Cache for 1 hour

            if not matching_records.empty:
                sentences = []
                for full_name in matching_records["Full Name"].unique():
                    # Insert the debugging code here
                    print(f"Processing company: {full_name}")
                    records = cmu_df[cmu_df["Full Name"] == full_name]
                    grouped = records.groupby("Delivery Year")

                    # Track all CMU IDs for this company
                    all_company_cmu_ids = []
                    for year, group in grouped:
                        cmu_ids = group["CMU ID"].unique().tolist()
                        all_company_cmu_ids.extend(cmu_ids)

                    print(f"Adding mapping for company: {full_name}")
                    for cmu_id in all_company_cmu_ids:
                        print(f"  Mapping CMU ID {cmu_id} -> {full_name}")
                        cmu_to_company_mapping[cmu_id] = full_name

                    print(f"All CMU IDs for {full_name}: {all_company_cmu_ids}")

                    # Collect all components for this company's CMU IDs
                    all_company_components = []
                    for cmu_id in all_company_cmu_ids:
                        # Try to get components from cache or JSON
                        components = cache.get(f"components_for_cmu_{cmu_id.lower()}")  # Use lowercase for cache key
                        if not components:
                            components = get_component_data_from_json(cmu_id)

                        if components:
                            # Add CMU ID to each component for reference
                            for component in components:
                                if "CMU ID" not in component:
                                    component["CMU ID"] = cmu_id

                                # Add company name to each component
                                component["Company Name"] = full_name

                            all_company_components.extend(components)

                    # Prepare the year sections
                    year_sections = []

                    # Track if we've already fetched components to avoid timeouts
                    already_fetched_components = False

                    for year, group in grouped:
                        if year.startswith("Years:"):
                            year = year.replace("Years:", "").strip()

                        auction_label = ""
                        if "Auction Name" in group.columns:
                            auctions = group["Auction Name"].unique().tolist()
                            if auctions:
                                auction_str = auctions[0].upper()
                                if "T-1" in auction_str:
                                    auction_label = "T-1"
                                elif "T-4" in auction_str:
                                    auction_label = "T-4"
                                else:
                                    auction_label = auction_str

                        cmu_ids = group["CMU ID"].unique().tolist()

                        if cmu_ids:
                            # Generate a unique ID for this year's collapsible section
                            year_collapse_id = f"year-{year.replace(' ', '')}-{full_name.replace(' ', '').lower()}"

                            # Create expandable year section
                            year_html = f"""
                            <div class="card mb-2">
                                <div class="card-header" id="heading-{year_collapse_id}">
                                    <h5 class="mb-0">
                                        <button class="btn btn-link" data-bs-toggle="collapse" data-bs-target="#collapse-{year_collapse_id}" aria-expanded="false" aria-controls="collapse-{year_collapse_id}">
                                            <strong>{year} ({auction_label})</strong>
                                        </button>
                                    </h5>
                                </div>
                                <div id="collapse-{year_collapse_id}" class="collapse" aria-labelledby="heading-{year_collapse_id}">
                                    <div class="card-body">
                                        <div class="row">
                            """

                            # Prepare CMU items for this year
                            # remvoved for simpler nestling # cmu_items = []

                            for cmu_id in cmu_ids:
                                # Generate a unique ID for this CMU's collapsible section
                                #cmu_collapse_id = f"cmu-{cmu_id.replace(' ', '').lower()}-{year.replace(' ', '')}"

                                # Print the CMU ID we're checking
                                #print(f"Checking components for CMU ID: {cmu_id}")

                                # Check cache first - use lowercase for cache key
                                components_cache_key = f"components_for_cmu_{cmu_id.lower()}"
                                components = cache.get(components_cache_key)

                                # Debug what's in cache
                                if not components:
                                    components = get_component_data_from_json(cmu_id)
                                    if not components and not already_fetched_components:
                                        try:
                                            components, component_api_time = fetch_components_for_cmu_id(cmu_id)
                                            api_time += component_api_time
                                            already_fetched_components = True
                                        except Exception as e:
                                            print(f"Error fetching components: {e}")
                                            components = []

                                    # Filter components for this year
                                year_components = []
                                if components:
                                    for component in components:
                                        if str(component.get("Delivery Year", "")) == year:
                                            year_components.append(component)

                                # Create a summary card for this CMU ID with location count
                                location_count = 0
                                location_set = set()

                                for component in year_components:
                                    location = component.get("Location and Post Code", "")
                                    if location:
                                        location_set.add(location)

                                location_count = len(location_set)

                                year_html += f"""
                                            <div class="col-md-6 mb-3">
                                                <div class="card">
                                                    <div class="card-header bg-light">
                                                        <div class="d-flex justify-content-between align-items-center">
                                                            <span>CMU ID: <strong>{cmu_id}</strong></span>
                                                            <a href="/components/?q={cmu_id}" class="btn btn-sm btn-info">View</a>
                                                        </div>
                                                    </div>
                                                    <div class="card-body">
                                                        <p><strong>Locations:</strong> {location_count}</p>
                                                        <ul class="list-unstyled">
                                            """

                                # Add a simple list of locations with descriptions
                                for location in sorted(location_set):
                                    year_html += f"""
                                                            <li class="mb-2">
                                                                <strong>{location}</strong>
                                                                <ul class="ms-3">
                                                """

                                    # Add components at this location
                                    for component in year_components:
                                        if component.get("Location and Post Code", "") == location:
                                            desc = component.get("Description of CMU Components", "N/A")
                                            tech = component.get("Generating Technology Class", "")

                                            year_html += f"""
                                                                    <li><i>{desc}</i>{f" - {tech}" if tech else ""}</li>
                                                        """

                                    year_html += """
                                                                </ul>
                                                            </li>
                                                """

                                year_html += """
                                                        </ul>
                                                    </div>
                                                </div>
                                            </div>
                                            """

                            year_html += """
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                        """

                            year_sections.append(year_html)
                        else:
                            year_html = f"<div class='mb-3'><strong>{year} ({auction_label})</strong> - No CMU IDs found</div>"
                            year_sections.append(year_html)

                    # Build the complete HTML for all years
                    years_html = "".join(year_sections)

                    # Create the final company sentence with all years
                    sentence = f"<strong>{full_name}</strong> has the following Delivery Years:<br/>{years_html}"
                    sentences.append(sentence)

                results[query] = sentences
            else:
                results[query] = [f"No matching record found for '{query}'."]

            # Store in session but also render directly (no redirect)
            request.session["search_results"] = results
            request.session["record_count"] = record_count
            request.session["api_time"] = api_time
            request.session["last_query"] = query  # Store the query for later

            # Render directly instead of redirecting
            return render(request, "checker/search.html", {
                "results": results,
                "record_count": record_count,
                "error": error_message,
                "api_time": api_time,
                "query": query,  # Keep the query in the search box
            })
        else:
            # Clear session but keep query parameter in case it's needed
            if "search_results" in request.session:
                request.session.pop("search_results", None)
            if "api_time" in request.session:
                request.session.pop("api_time", None)

            return render(request, "checker/search.html", {
                "results": {},
                "api_time": api_time,
                "query": query,  # Keep any query in the search box
            })

    return render(request, "checker/search.html", {
        "results": {},
        "api_time": api_time,
        "query": query,  # Keep any query in the search box
    })


def search_components(request):
    results = {}
    error_message = None
    overall_total = 0
    api_time = 0

    if request.method == "GET" and request.GET.get("q"):
        query = request.GET.get("q", "").strip()
        sort_order = request.GET.get("sort", "desc")
        norm_query = normalize(query)

        total_cache_key = "components_overall_total"
        overall_total = cache.get(total_cache_key)
        if overall_total is None:
            try:
                start_time = time.time()
                count_response = requests.get(
                    "https://api.neso.energy/api/3/action/datastore_search",
                    params={
                        "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
                        "limit": 1
                    },
                    timeout=20
                )
                count_response.raise_for_status()
                api_time += time.time() - start_time
                count_data = count_response.json()["result"]
                overall_total = count_data.get("total", 0)
                cache.set(total_cache_key, overall_total, 3600)
            except Exception as e:
                print(f"Error fetching overall total: {e}")
                api_time += time.time() - start_time
                overall_total = 0

        search_cache_key = f"components_search_{query.lower()}"  # Case-insensitive cache key
        records = cache.get(search_cache_key)
        if records is None:
            try:
                start_time = time.time()
                response = requests.get(
                    "https://api.neso.energy/api/3/action/datastore_search",
                    params={
                        "resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8",
                        "q": query,
                        "limit": 1000
                    },
                    timeout=20
                )
                response.raise_for_status()
                api_time += time.time() - start_time
                data = response.json()["result"]
                records = data.get("records", [])
                cache.set(search_cache_key, records, 300)

                # Also cache component records by CMU ID for use in company search
                if records:
                    for record in records:
                        cmu_id = record.get("CMU ID", "")
                        if cmu_id:
                            components_cache_key = f"components_for_cmu_{cmu_id.lower()}"  # Using correct key format
                            existing_components = cache.get(components_cache_key, [])
                            if record not in existing_components:
                                existing_components.append(record)
                                cache.set(components_cache_key, existing_components, 3600)

                                # Also save to JSON for persistence
                                json_components = get_component_data_from_json(cmu_id) or []
                                if record not in json_components:
                                    json_components.append(record)
                                    save_component_data_to_json(cmu_id, json_components)

            except Exception as e:
                error_message = f"Error fetching components data: {e}"
                api_time += time.time() - start_time
                return render(request, "checker/search_components.html", {
                    "results": {},
                    "record_count": overall_total,
                    "error": error_message,
                    "api_time": api_time
                })

        if not records:
            results[query] = [f"No matching components found for '{query}'."]
            return render(request, "checker/search_components.html", {
                "results": results,
                "record_count": overall_total,
                "error": error_message,
                "api_time": api_time
            })

        components_df = pd.DataFrame(records)
        components_df["Location and Post Code"] = components_df.get("Location and Post Code", pd.Series()).fillna(
            "").astype(str)

        possible_cmu_id_fields = ["CMU ID", "cmu_id", "CMU_ID", "cmuId", "id", "identifier", "ID"]
        cmu_id_field = next((field for field in possible_cmu_id_fields if field in components_df.columns), None)
        if cmu_id_field:
            components_df["CMU ID"] = components_df[cmu_id_field].fillna("N/A").astype(str)
        else:
            components_df["CMU ID"] = "N/A"

        components_df["Normalized Location"] = components_df["Location and Post Code"].apply(normalize)
        components_df["Normalized CMU ID"] = components_df["CMU ID"].apply(normalize)

        matching_df = components_df[
            components_df["Normalized Location"].str.contains(norm_query, regex=False, na=False) |
            components_df["Normalized CMU ID"].str.contains(norm_query, regex=False, na=False)
            ]

        # Convert delivery year to numeric for sorting
        matching_df['Delivery Year Numeric'] = pd.to_numeric(
            matching_df['Delivery Year'], errors='coerce'
        ).fillna(0).astype(int)

        # Toggle sorting based on sort_order parameter
        ascending = sort_order != "desc"  # If not "desc", use ascending order
        matching_df = matching_df.sort_values(
            by=['Delivery Year Numeric'],
            ascending=ascending
        )



        sort_direction = "oldest first" if ascending else "newest first"
        print(
            f"Sorted results by delivery year ({sort_direction}), range: {matching_df['Delivery Year Numeric'].min()} - {matching_df['Delivery Year Numeric'].max()}")


        # Get the cached mapping of CMU IDs to company names
        cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})

        # Print debug information about the company mapping cache
        print(f"DEBUG: Company mapping cache has {len(cmu_to_company_mapping)} entries")
        if cmu_to_company_mapping:
            sample_entries = list(cmu_to_company_mapping.items())[:5]
            print(f"DEBUG: Sample entries: {sample_entries}")
        else:
            print("DEBUG: Company mapping cache is empty")

            # If empty, try to fill it from components that have Company Name stored
            if not matching_df.empty:
                print("DEBUG: Attempting to rebuild mapping from components")
                for cmu_id in matching_df["CMU ID"].unique():
                    components = get_component_data_from_json(cmu_id)
                    if components and isinstance(components, list) and len(components) > 0:
                        company_name = None
                        # Check if any component has Company Name
                        for comp in components:
                            if "Company Name" in comp and comp["Company Name"]:
                                company_name = comp["Company Name"]
                                break

                        if company_name:
                            print(f"DEBUG: Found company name {company_name} for {cmu_id}")
                            cmu_to_company_mapping[cmu_id] = company_name

            # Update cache with rebuilt mapping
            if cmu_to_company_mapping:
                print(f"DEBUG: Rebuilt {len(cmu_to_company_mapping)} cache entries")
                cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 3600)

        def format_component_record(record):
            """Format a component record for display with proper company badge"""
            loc = record.get("Location and Post Code", "N/A")
            desc = record.get("Description of CMU Components", "N/A")
            tech = record.get("Generating Technology Class", "N/A")
            typ = record.get("Type", "N/A")
            delivery_year = record.get("Delivery Year", "N/A")
            auction = record.get("Auction Name", "N/A")
            cmu_id = record.get("CMU ID", "N/A")

            # Debug output to see what's in the record
            print(f"Processing component with CMU ID: {cmu_id}")
            if "Company Name" in record:
                print(f"  Component has Company Name: '{record['Company Name']}'")
            else:
                print(f"  Component does NOT have Company Name field")

            # Get company name with multiple fallbacks
            company_name = ""

            # 1. First try the record itself
            if "Company Name" in record and record["Company Name"]:
                company_name = record["Company Name"]
                print(f"  Using Company Name from record: {company_name}")

            # 2. If not found, try to get from the mapping
            if not company_name:
                # Try exact match first
                company_name = cmu_to_company_mapping.get(cmu_id, "")

                # Try case-insensitive match
                if not company_name:
                    for mapping_id, mapping_name in cmu_to_company_mapping.items():
                        if mapping_id.lower() == cmu_id.lower():
                            company_name = mapping_name
                            break

                if company_name:
                    print(f"  Found company name in mapping: {company_name}")

            # 3. If still not found, try to get directly from JSON file
            if not company_name:
                try:
                    import os, json
                    from django.conf import settings

                    prefix = cmu_id[0].upper()
                    json_path = os.path.join(settings.BASE_DIR, 'json_data', f'components_{prefix}.json')

                    if os.path.exists(json_path):
                        with open(json_path, 'r') as f:
                            data = json.load(f)

                            # Try exact match
                            if cmu_id in data and data[cmu_id]:
                                for component in data[cmu_id]:
                                    if "Company Name" in component and component["Company Name"]:
                                        company_name = component["Company Name"]
                                        print(f"  Found company name in JSON file: {company_name}")
                                        break

                            # If not found, try case-insensitive match
                            if not company_name:
                                for file_id, components in data.items():
                                    if file_id.lower() == cmu_id.lower() and components:
                                        for component in components:
                                            if "Company Name" in component and component["Company Name"]:
                                                company_name = component["Company Name"]
                                                print(
                                                    f"  Found company name in JSON file (case-insensitive): {company_name}")
                                                break
                                        if company_name:
                                            break
                except Exception as e:
                    print(f"  Error checking JSON file: {e}")

            # Create company badge if we have a name
            company_info = ""
            if company_name:
                # Make badge more prominent with larger text
                encoded_company_name = urllib.parse.quote(company_name)
                company_link = f'<a href="/?q={encoded_company_name}" class="badge bg-success" style="font-size: 1rem; text-decoration: none;">{company_name}</a>'
                company_info = f'<div class="mt-2 mb-2">{company_link}</div>'
                print(f"  Created badge for: {company_name}")
            else:
                # Add a warning badge if no company found
                company_info = f'<div class="mt-2 mb-2"><span class="badge bg-warning">No Company Found</span></div>'
                print("  No company name found for this component")

            # Convert loc to a blue link pointing to Companies Search
            loc_link = f'<a href="/?q={urllib.parse.quote(cmu_id)}" style="color: blue; text-decoration: underline;">{loc}</a>'

            # Adjust formatting to make more readable
            return (
                f"<strong>{loc_link}</strong><br>"
                f"<i>{desc}</i><br>"
                f"Technology: {tech}<br>"
                f"<b>{auction}</b><br>"
                f"<i>CMU ID: {cmu_id}</i>"
                f"{company_info}"
            )

        if not matching_df.empty:
            formatted_records = [format_component_record(r) for idx, r in matching_df.iterrows()]
            results[query] = formatted_records
        else:
            results[query] = [f"No matching components found for '{query}'."]

        return render(request, "checker/search_components.html", {
            "results": results,
            "record_count": overall_total,
            "error": error_message,
            "api_time": api_time,
            "query": query,
            "sort_order": sort_order
        })
    else:
        sort_order = "desc"
        return render(request, "checker/search_components.html", {
            "results": {},
            "record_count": overall_total,
            "error": error_message,
            "api_time": api_time,
            "sort_order": sort_order
        })


def debug_mapping_cache(request):
    """Debug endpoint to view the CMU to company mapping cache."""
    cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})

    output = f"<h1>CMU to Company Mapping Cache</h1>"
    output += f"<p>Total entries: {len(cmu_to_company_mapping)}</p>"

    output += "<h2>Sample Entries</h2>"
    output += "<table border='1'><tr><th>CMU ID</th><th>Company Name</th></tr>"

    for i, (cmu_id, company) in enumerate(cmu_to_company_mapping.items()):
        output += f"<tr><td>{cmu_id}</td><td>{company}</td></tr>"
        if i > 20:  # Show only first 20 entries
            break

    output += "</table>"

    # Add a form to add a new mapping manually
    output += """
        <h2>Add Mapping</h2>
        <form method="post">
            <label>CMU ID: <input type="text" name="cmu_id"></label><br>
            <label>Company Name: <input type="text" name="company"></label><br>
            <input type="submit" value="Add Mapping">
        </form>
        """

    # Handle form submission
    if request.method == "POST":
        cmu_id = request.POST.get("cmu_id", "").strip()
        company = request.POST.get("company", "").strip()

        if cmu_id and company:
            # Update the mapping
            cmu_to_company_mapping[cmu_id] = company
            cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 3600)

            # Also update any components for this CMU ID
            components = get_component_data_from_json(cmu_id)
            if components:
                for component in components:
                    component["Company Name"] = company
                save_component_data_to_json(cmu_id, components)

            output += f"<p style='color:green'>Added mapping: {cmu_id} -> {company}</p>"
        else:
            output += "<p style='color:red'>Both CMU ID and Company Name are required</p>"

    return HttpResponse(output)