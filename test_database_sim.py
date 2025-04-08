import time
import requests

# Simulate a database of components with postcodes in their locations
# This is a simplified mock that mimics how our database search would work
class MockDatabase:
    def __init__(self):
        self.components = [
            {"id": 1, "location": "123 Main St, London SW16 1AA"},
            {"id": 2, "location": "456 High St, London SW16 2BB"},
            {"id": 3, "location": "789 Park Rd, Streatham SW16 3CC"},
            {"id": 4, "location": "101 Hill Ave, London SW17 4DD"},
            {"id": 5, "location": "202 Vale St, Streatham SW17 5EE"},
            {"id": 6, "location": "303 Dale Rd, London CR4 6FF"},
            {"id": 7, "location": "404 Field Ln, Streatham CR4 7GG"},
            {"id": 8, "location": "505 Brook Dr, London SW12 8HH"},
            {"id": 9, "location": "606 River Rd, Streatham SW2 9II"},
            {"id": 10, "location": "707 Lake Ave, London SE27 0JJ"},
            {"id": 11, "location": "808 Ocean St, London SE21 1KK"},
            {"id": 12, "location": "909 Sea Ln, Streatham SE24 2LL"},
            {"id": 13, "location": "1010 Bay Dr, London CR7 3MM"},
            {"id": 14, "location": "1111 Gulf Ave, Streatham SE19 4NN"},
            {"id": 15, "location": "1212 Pond Rd, London SE19 5OO"},
        ]
    
    def search(self, location_filters):
        """
        Search components based on location filters
        location_filters: list of postcodes or area names to search for
        """
        results = []
        for component in self.components:
            for filter_term in location_filters:
                if filter_term.lower() in component["location"].lower():
                    results.append(component)
                    break  # No need to check other filters once we have a match
        return results

# Import the postcode helpers from the test script
from test_postcodes_api import get_outcodes_for_area, get_outcodes_for_postcode

def search_with_hardcoded_postcodes(db, location):
    """Search using hardcoded postcodes"""
    start_time = time.time()
    
    # Simulate hardcoded postcode mappings
    hardcoded = {
        "streatham": ["SW16", "SW17", "CR4"],
        "london": ["SW", "SE", "W", "E", "N", "NW", "EC", "WC"],
    }
    
    location_lower = location.lower()
    if location_lower in hardcoded:
        postcodes = hardcoded[location_lower]
    else:
        postcodes = [location]  # Just use the location as is
    
    results = db.search(postcodes + [location])  # Include the original location too
    
    end_time = time.time()
    return results, end_time - start_time

def search_with_api_postcodes(db, location):
    """Search using postcodes from the API"""
    start_time = time.time()
    
    # Try to get postcodes for an area name
    postcodes = get_outcodes_for_area(location)
    
    # If that fails, try treating the location as a postcode
    if not postcodes:
        postcodes = get_outcodes_for_postcode(location)
    
    # If all else fails, just use the location as is
    if not postcodes:
        postcodes = [location]
    
    results = db.search(postcodes + [location])  # Include the original location too
    
    end_time = time.time()
    return results, end_time - start_time

# Create a mock database
db = MockDatabase()

# Test with "streatham"
print("=== Testing search for 'streatham' ===")
hardcoded_results, hardcoded_time = search_with_hardcoded_postcodes(db, "streatham")
print(f"Hardcoded search found {len(hardcoded_results)} results in {hardcoded_time:.2f} seconds")
print(f"Component IDs: {[c['id'] for c in hardcoded_results]}")

api_results, api_time = search_with_api_postcodes(db, "streatham")
print(f"API search found {len(api_results)} results in {api_time:.2f} seconds")
print(f"Component IDs: {[c['id'] for c in api_results]}")

# Test with "SW16"
print("\n=== Testing search for 'SW16' ===")
hardcoded_results, hardcoded_time = search_with_hardcoded_postcodes(db, "SW16")
print(f"Hardcoded search found {len(hardcoded_results)} results in {hardcoded_time:.2f} seconds")
print(f"Component IDs: {[c['id'] for c in hardcoded_results]}")

api_results, api_time = search_with_api_postcodes(db, "SW16")
print(f"API search found {len(api_results)} results in {api_time:.2f} seconds")
print(f"Component IDs: {[c['id'] for c in api_results]}")

# Calculate the overlap and unique results
hardcoded_ids = set([c['id'] for c in hardcoded_results])
api_ids = set([c['id'] for c in api_results])
print(f"\nResults in both searches: {len(hardcoded_ids.intersection(api_ids))}")
print(f"Results only in hardcoded search: {len(hardcoded_ids - api_ids)}")
print(f"Results only in API search: {len(api_ids - hardcoded_ids)}")
print(f"Additional postcodes from API: {set(get_outcodes_for_area('streatham')) - set(['SW16', 'SW17', 'CR4'])}") 