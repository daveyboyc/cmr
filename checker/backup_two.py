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
    """Retrieve component data from JSON files based on CMU ID."""
    if not cmu_id:
        return None

    prefix = cmu_id[0].upper() if cmu_id else "0"
    json_dir = os.path.join(settings.BASE_DIR, 'json_data')
    json_path = os.path.join(json_dir, f'components_{prefix}.json')

    if not os.path.exists(json_path):
        old_json_path = os.path.join(settings.BASE_DIR, 'component_data.json')
        if os.path.exists(old_json_path):
            try:
                with open(old_json_path, 'r') as f:
                    all_components = json.load(f)
                components = all_components.get(cmu_id)
            except Exception as e:
                print(f"Error reading old JSON: {e}")
                return None
        else:
            return None
    else:
        try:
            with open(json_path, 'r') as f:
                all_components = json.load(f)
            components = all_components.get(cmu_id)
        except Exception as e:
            print(f"Error reading JSON: {e}")
            return None

    if components:
        cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
        company_name = cmu_to_company_mapping.get(cmu_id, "")
        for component in components:
            if "Company Name" not in component or not component["Company Name"]:
                component["Company Name"] = company_name or "Unknown"
    return components

def save_component_data_to_json(cmu_id, components):
    """Save component data to JSON, including company name."""
    if not cmu_id:
        return False

    prefix = cmu_id[0].upper()
    json_dir = os.path.join(settings.BASE_DIR, 'json_data')
    os.makedirs(json_dir, exist_ok=True)
    json_path = os.path.join(json_dir, f'components_{prefix}.json')

    all_components = {}
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r') as f:
                all_components = json.load(f)
        except Exception as e:
            print(f"Error reading existing JSON: {e}")

    cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
    company_name = cmu_to_company_mapping.get(cmu_id, "")

    if not company_name:
        cmu_records, _ = fetch_all_cmu_records()
        cmu_df = pd.DataFrame(cmu_records)
        cmu_df["Full Name"] = cmu_df["Name of Applicant"].str.strip()
        cmu_df["Full Name"] = cmu_df.apply(
            lambda row: row["Full Name"] if row["Full Name"] else row["Parent Company"],
            axis=1
        )
        cmu_to_company_mapping = dict(zip(cmu_df["CMU ID"], cmu_df["Full Name"]))
        cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 3600)
        company_name = cmu_to_company_mapping.get(cmu_id, "")

    updated_components = []
    for component in components:
        component = component.copy()
        if "Company Name" not in component and company_name:
            component["Company Name"] = company_name
        if "CMU ID" not in component:
            component["CMU ID"] = cmu_id
        updated_components.append(component)

    all_components[cmu_id] = updated_components
    try:
        with open(json_path, 'w') as f:
            json.dump(all_components, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving JSON: {e}")
        return False

def get_cmu_data_from_json():
    """Retrieve CMU data from JSON."""
    json_path = os.path.join(settings.BASE_DIR, 'cmu_data.json')
    if not os.path.exists(json_path):
        return None
    try:
        with open(json_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading CMU JSON: {e}")
        return None

def save_cmu_data_to_json(cmu_records):
    """Save CMU data to JSON."""
    json_path = os.path.join(settings.BASE_DIR, 'cmu_data.json')
    try:
        with open(json_path, 'w') as f:
            json.dump(cmu_records, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving CMU JSON: {e}")
        return False

def fetch_all_cmu_records(limit=1000):
    """Fetch all CMU records from API or JSON."""
    json_cmu_data = get_cmu_data_from_json()
    if json_cmu_data is not None:
        print(f"Using JSON CMU data, found {len(json_cmu_data)} records")
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
        all_records.extend(result["records"])
        if len(all_records) >= result["total"]:
            break
        params["offset"] += limit

    save_cmu_data_to_json(all_records)
    return all_records, total_time

def fetch_components_for_cmu_id(cmu_id, limit=100):
    """Fetch components for a CMU ID from cache, JSON, or API."""
    components_cache_key = f"components_for_cmu_{cmu_id}"
    cached_components = cache.get(components_cache_key)
    if cached_components is not None:
        print(f"Using cached components for {cmu_id}, found {len(cached_components)}")
        return cached_components, 0

    json_components = get_component_data_from_json(cmu_id)
    if json_components is not None:
        print(f"Using JSON components for {cmu_id}, found {len(json_components)}")
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

        cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
        company_name = cmu_to_company_mapping.get(cmu_id, "")
        if company_name:
            for component in components:
                if "Company Name" not in component:
                    component["Company Name"] = company_name

        cache.set(components_cache_key, components, 3600)
        save_component_data_to_json(cmu_id, components)
        return components, elapsed
    except Exception as e:
        print(f"Error fetching components for {cmu_id}: {e}")
        elapsed = time.time() - start_time
        return [], elapsed

def search_companies(request):
    """Display companies and their component locations by CMU ID."""
    results = {}
    error_message = None
    api_time = 0
    query = request.GET.get("q", "").strip()

    if request.method == "GET":
        if "search_results" in request.session and not query:
            results = request.session.pop("search_results")
            record_count = request.session.pop("record_count", None)
            api_time = request.session.pop("api_time", 0)
            last_query = request.session.pop("last_query", "")
            return render(request, "checker/search.html", {
                "results": results,
                "record_count": record_count,
                "error": error_message,
                "api_time": api_time,
                "query": last_query,
            })
        elif query:
            norm_query = normalize(query)
            cmu_df = cache.get("cmu_df")
            if cmu_df is None:
                try:
                    all_records, api_time = fetch_all_cmu_records(limit=5000)
                    cmu_df = pd.DataFrame(all_records)
                    cmu_df["Name of Applicant"] = cmu_df.get("Name of Applicant", pd.Series()).fillna("").astype(str)
                    cmu_df["Parent Company"] = cmu_df.get("Parent Company", pd.Series()).fillna("").astype(str)
                    cmu_df["CMU ID"] = cmu_df.get("CMU ID", pd.Series()).fillna("N/A").astype(str)
                    cmu_df["Full Name"] = cmu_df["Name of Applicant"].str.strip()
                    cmu_df["Full Name"] = cmu_df.apply(
                        lambda row: row["Full Name"] if row["Full Name"] else row["Parent Company"],
                        axis=1
                    )
                    cmu_to_company_mapping = dict(zip(cmu_df["CMU ID"], cmu_df["Full Name"]))
                    cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 3600)
                    cache.set("cmu_df", cmu_df, 900)
                except requests.exceptions.RequestException as e:
                    error_message = f"Error fetching CMU data: {e}"
                    return render(request, "checker/search.html", {
                        "error": error_message,
                        "api_time": api_time,
                        "query": query,
                    })

            record_count = len(cmu_df)
            cmu_df["Normalized Full Name"] = cmu_df["Full Name"].apply(normalize)
            cmu_df["Normalized CMU ID"] = cmu_df["CMU ID"].apply(normalize)

            matching_records = cmu_df[
                cmu_df["Normalized Full Name"].str.contains(norm_query, regex=False, na=False) |
                cmu_df["Normalized CMU ID"].str.contains(norm_query, regex=False, na=False)
            ]

            if not matching_records.empty:
                sentences = []
                for full_name in matching_records["Full Name"].unique():
                    records = cmu_df[cmu_df["Full Name"] == full_name]
                    grouped = records.groupby("Delivery Year")
                    all_company_cmu_ids = records["CMU ID"].unique().tolist()
                    all_company_components = []
                    for cmu_id in all_company_cmu_ids:
                        components = get_component_data_from_json(cmu_id) or []
                        for component in components:
                            if "CMU ID" not in component:
                                component["CMU ID"] = cmu_id
                            if "Company Name" not in component or not component["Company Name"]:
                                component["Company Name"] = full_name
                        all_company_components.extend(components)

                    year_sections = []
                    for year, group in grouped:
                        if year.startswith("Years:"):
                            year = year.replace("Years:", "").strip()
                        auction_label = group["Auction Name"].iloc[0].upper() if "Auction Name" in group.columns else ""
                        if "T-1" in auction_label:
                            auction_label = "T-1"
                        elif "T-4" in auction_label:
                            auction_label = "T-4"
                        cmu_ids = group["CMU ID"].unique().tolist()
                        cmu_items = []
                        for cmu_id in cmu_ids:
                            components = get_component_data_from_json(cmu_id) or []
                            cmu_link = f'<a href="/components/?q={cmu_id}" class="btn btn-sm btn-info ms-2">View {cmu_id}</a>'
                            locations_html = ""
                            if components:
                                unique_locations = {comp.get("Location and Post Code", "") for comp in components if comp.get("Location and Post Code")}
                                if unique_locations:
                                    location_bullets = [f"<li class='small text-muted'>{loc}</li>" for loc in unique_locations]
                                    locations_html = f"""
                                    <div class="ms-4 mt-1">
                                        <span class="badge bg-secondary">Locations ({len(unique_locations)})</span>
                                        <ul class="mb-1">
                                            {''.join(location_bullets)}
                                        </ul>
                                    </div>
                                    """
                            cmu_items.append(f"<div class='mt-1'>CMU ID: {cmu_link}{locations_html}</div>")
                        year_sections.append(f"<strong>{year} ({auction_label})</strong><div class='ms-3'>{''.join(cmu_items)}</div>")

                    years_html = "<ul class='list-unstyled'>" + "".join(f"<li class='mb-3'>{ys}</li>" for ys in year_sections) + "</ul>"
                    components_html = ""
                    if all_company_components:
                        components_html = "<div class='mt-3'><h4>Components</h4><div class='ms-3'>"
                        locations = {}
                        for component in all_company_components:
                            location = component.get("Location and Post Code", "Unknown Location")
                            locations.setdefault(location, []).append(component)
                        for location, comps in locations.items():
                            components_html += f"<div class='mb-2'><strong>{location}</strong> ({len(comps)} components)"
                            components_html += "<ul class='list-unstyled ms-3'>"
                            for comp in comps:
                                cmu_id = comp.get("CMU ID", "N/A")
                                desc = comp.get("Description of CMU Components", "N/A")
                                tech = comp.get("Generating Technology Class", "N/A")
                                components_html += f"<li class='mb-1'><i>{desc}</i> - {tech} (CMU ID: {cmu_id})</li>"
                            components_html += "</ul></div>"
                        components_html += "</div></div>"

                    sentences.append(f"<strong>{full_name}</strong> has the following Delivery Years:<br/>{years_html}{components_html}")
                results[query] = sentences
            else:
                results[query] = [f"No matching record found for '{query}'."]

            request.session["search_results"] = results
            request.session["record_count"] = record_count
            request.session["api_time"] = api_time
            request.session["last_query"] = query

            return render(request, "checker/search.html", {
                "results": results,
                "record_count": record_count,
                "error": error_message,
                "api_time": api_time,
                "query": query,
            })
        else:
            request.session.pop("search_results", None)
            request.session.pop("api_time", None)
            return render(request, "checker/search.html", {
                "results": {},
                "api_time": api_time,
                "query": query,
            })
    return render(request, "checker/search.html", {
        "results": {},
        "api_time": api_time,
        "query": query,
    })

def search_components(request):
    """Display components with their company names via CMU ID."""
    results = {}
    error_message = None
    overall_total = 0
    api_time = 0

    if request.method == "GET" and request.GET.get("q"):
        query = request.GET.get("q", "").strip()
        norm_query = normalize(query)

        total_cache_key = "components_overall_total"
        overall_total = cache.get(total_cache_key)
        if overall_total is None:
            try:
                start_time = time.time()
                count_response = requests.get(
                    "https://api.neso.energy/api/3/action/datastore_search",
                    params={"resource_id": "790f5fa0-f8eb-4d82-b98d-0d34d3e404e8", "limit": 1},
                    timeout=20
                )
                count_response.raise_for_status()
                api_time += time.time() - start_time
                overall_total = count_response.json()["result"].get("total", 0)
                cache.set(total_cache_key, overall_total, 3600)
            except Exception as e:
                print(f"Error fetching total: {e}")
                api_time += time.time() - start_time
                overall_total = 0

        search_cache_key = f"components_search_{query}"
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
                records = response.json()["result"].get("records", [])
                cache.set(search_cache_key, records, 300)
                for record in records:
                    cmu_id = record.get("CMU ID", "")
                    if cmu_id:
                        components_cache_key = f"components_for_cmu_{cmu_id}"
                        existing = cache.get(components_cache_key, [])
                        if record not in existing:
                            existing.append(record)
                            cache.set(components_cache_key, existing, 3600)
                            json_components = get_component_data_from_json(cmu_id) or []
                            if record not in json_components:
                                json_components.append(record)
                                save_component_data_to_json(cmu_id, json_components)
            except Exception as e:
                error_message = f"Error fetching components: {e}"
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
        components_df["Location and Post Code"] = components_df.get("Location and Post Code", pd.Series()).fillna("").astype(str)
        components_df["CMU ID"] = components_df.get("CMU ID", pd.Series()).fillna("N/A").astype(str)
        components_df["Normalized Location"] = components_df["Location and Post Code"].apply(normalize)
        components_df["Normalized CMU ID"] = components_df["CMU ID"].apply(normalize)

        matching_df = components_df[
            components_df["Normalized Location"].str.contains(norm_query, regex=False, na=False) |
            components_df["Normalized CMU ID"].str.contains(norm_query, regex=False, na=False)
        ]

        cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
        def format_component_record(record):
            loc = record.get("Location and Post Code", "N/A")
            desc = record.get("Description of CMU Components", "N/A")
            tech = record.get("Generating Technology Class", "N/A")
            auction = record.get("Auction Name", "N/A")
            cmu_id = record.get("CMU ID", "N/A")
            company_name = record.get("Company Name", cmu_to_company_mapping.get(cmu_id, ""))
            if company_name and "Company Name" not in record:
                record["Company Name"] = company_name
                components = get_component_data_from_json(cmu_id) or []
                for comp in components:
                    if comp.get("CMU ID") == cmu_id and "Company Name" not in comp:
                        comp["Company Name"] = company_name
                save_component_data_to_json(cmu_id, components)

            company_info = f'<div class="mt-1 mb-1"><span class="badge bg-success">{company_name}</span></div>' if company_name else ""
            loc_link = f'<a href="/?q={urllib.parse.quote(cmu_id)}" style="color: blue; text-decoration: underline;">{loc}</a>'
            return (
                f"{loc_link}<br>"
                f"<i>{desc}</i><br>"
                f"Technology Listed: {tech}<br>"
                f"<b>{auction}</b><br>"
                f"<i>CMU ID: {cmu_id}</i>"
                f"{company_info}"
            )

        if not matching_df.empty:
            results[query] = [format_component_record(r) for _, r in matching_df.iterrows()]
        else:
            results[query] = [f"No matching components found for '{query}'."]
        return render(request, "checker/search_components.html", {
            "results": results,
            "record_count": overall_total,
            "error": error_message,
            "api_time": api_time
        })
    return render(request, "checker/search_components.html", {
        "results": {},
        "record_count": overall_total,
        "error": error_message,
        "api_time": api_time
    })

def debug_mapping_cache(request):
    """Debug utility to view and edit the CMU to company mapping."""
    cmu_to_company_mapping = cache.get("cmu_to_company_mapping", {})
    output = f"<h1>CMU to Company Mapping Cache</h1><p>Total entries: {len(cmu_to_company_mapping)}</p>"
    output += "<h2>Sample Entries</h2><table border='1'><tr><th>CMU ID</th><th>Company Name</th></tr>"
    for i, (cmu_id, company) in enumerate(cmu_to_company_mapping.items()):
        output += f"<tr><td>{cmu_id}</td><td>{company}</td></tr>"
        if i > 20:
            break
    output += "</table>"
    output += """
        <h2>Add Mapping</h2>
        <form method="post">
            <label>CMU ID: <input type="text" name="cmu_id"></label><br>
            <label>Company Name: <input type="text" name="company"></label><br>
            <input type="submit" value="Add Mapping">
        </form>
    """
    if request.method == "POST":
        cmu_id = request.POST.get("cmu_id", "").strip()
        company = request.POST.get("company", "").strip()
        if cmu_id and company:
            cmu_to_company_mapping[cmu_id] = company
            cache.set("cmu_to_company_mapping", cmu_to_company_mapping, 3600)
            components = get_component_data_from_json(cmu_id) or []
            for component in components:
                component["Company Name"] = company
            save_component_data_to_json(cmu_id, components)
            output += f"<p style='color:green'>Added mapping: {cmu_id} -> {company}</p>"
        else:
            output += "<p style='color:red'>Both CMU ID and Company Name required</p>"
    return HttpResponse(output)