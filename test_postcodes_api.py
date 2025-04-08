import requests
import json
import time

def get_outcodes_for_area(area_name):
    """Get outcodes associated with an area by using reverse geocoding"""
    # Approximate coordinates for areas
    area_coordinates = {
        "streatham": {"latitude": 51.4256, "longitude": -0.1272},
        "london": {"latitude": 51.5074, "longitude": -0.1278},
        "manchester": {"latitude": 53.4808, "longitude": -2.2426},
    }
    
    if area_name.lower() not in area_coordinates:
        print(f"No coordinates found for {area_name}")
        return []
    
    coords = area_coordinates[area_name.lower()]
    
    # Use reverse geocoding to find nearby postcodes
    response = requests.post(
        "https://api.postcodes.io/postcodes",
        json={"geolocations": [coords]}
    )
    
    if response.status_code != 200:
        print(f"API error: {response.status_code}")
        return []
    
    results = response.json()["result"][0]["result"]
    
    # Extract outcodes
    outcodes = set()
    for result in results:
        outcodes.add(result["outcode"])
    
    # Get neighboring outcodes
    all_outcodes = set(outcodes)
    for outcode in outcodes:
        try:
            neighbors_response = requests.get(f"https://api.postcodes.io/outcodes/{outcode}/nearest")
            if neighbors_response.status_code == 200:
                for neighbor in neighbors_response.json()["result"]:
                    all_outcodes.add(neighbor["outcode"])
        except Exception as e:
            print(f"Error getting neighbors for {outcode}: {e}")
    
    return list(all_outcodes)

def get_outcodes_for_postcode(postcode):
    """Get outcode for a postcode and its neighboring outcodes"""
    # First try to get the outcode directly
    outcode = postcode.split()[0] if " " in postcode else postcode
    
    try:
        # Validate the outcode
        response = requests.get(f"https://api.postcodes.io/outcodes/{outcode}")
        if response.status_code != 200:
            print(f"Invalid outcode: {outcode}")
            return []
        
        # Get neighboring outcodes
        neighbors_response = requests.get(f"https://api.postcodes.io/outcodes/{outcode}/nearest")
        if neighbors_response.status_code != 200:
            print(f"Error getting neighbors for {outcode}")
            return [outcode]
        
        outcodes = [outcode]
        for neighbor in neighbors_response.json()["result"]:
            outcodes.append(neighbor["outcode"])
        
        return outcodes
    except Exception as e:
        print(f"Error processing outcode {outcode}: {e}")
        return []

def search_components_using_api_postcodes(location, use_api=True):
    """
    Simulate the search logic using API postcodes instead of hardcoded ones
    """
    start_time = time.time()
    print(f"Searching for components with location: {location}")
    
    # Start with basic location filter
    print(f"Starting with basic filter for: {location}")
    
    related_postcodes = []
    
    if use_api:
        # Check if location is an area name and get related postcodes via API
        outcodes = get_outcodes_for_area(location)
        if outcodes:
            related_postcodes = outcodes
            print(f"API: Found {len(related_postcodes)} related outcodes for area: {location}")
            print(f"Outcodes: {related_postcodes}")
        else:
            # Check if location might be a postcode and get related areas
            outcodes = get_outcodes_for_postcode(location)
            if outcodes:
                related_postcodes = outcodes
                print(f"API: Found {len(related_postcodes)} related outcodes for postcode: {location}")
                print(f"Outcodes: {related_postcodes}")
    else:
        # Use hardcoded values for comparison
        hardcoded = {
            "streatham": ["SW16", "SW17", "CR4"],
            "london": ["SW", "SE", "W", "E", "N", "NW", "EC", "WC"],
        }
        
        location_lower = location.lower()
        if location_lower in hardcoded:
            related_postcodes = hardcoded[location_lower]
            print(f"Hardcoded: Found {len(related_postcodes)} related postcodes for: {location}")
            print(f"Postcodes: {related_postcodes}")
    
    end_time = time.time()
    print(f"Search completed in {end_time - start_time:.2f} seconds")
    return related_postcodes

# Test with Streatham using API
print("=== Testing with Streatham using API ===")
api_results = search_components_using_api_postcodes("streatham", use_api=True)

# Test with Streatham using hardcoded values
print("\n=== Testing with Streatham using hardcoded values ===")
hardcoded_results = search_components_using_api_postcodes("streatham", use_api=False)

# Test with SW16 postcode
print("\n=== Testing with SW16 postcode using API ===")
sw16_results = search_components_using_api_postcodes("SW16", use_api=True)

print("\n=== Comparison ===")
print(f"API results for 'streatham': {len(api_results)} outcodes")
print(f"Hardcoded results for 'streatham': {len(hardcoded_results)} postcodes")
