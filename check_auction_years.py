#!/usr/bin/env python.
import json
import os
import glob
from collections import defaultdict

# Directory containing JSON data files
json_dir = os.path.join(os.getcwd(), 'json_data')

# Function to check components in a JSON file
def check_auction_years(json_file):
    print(f"Checking {os.path.basename(json_file)}...")
    
    try:
        with open(json_file, 'r') as f:
            components = json.load(f)
        
        # Group components by CMU ID and location
        grouped = defaultdict(list)
        for comp in components:
            # Skip components that don't have the required fields
            if not all(k in comp for k in ['CMU ID', 'Location and Post Code', 'Auction', 'Delivery Year']):
                continue
            
            # Use CMU ID and location as the key for grouping
            key = (comp.get('CMU ID', ''), comp.get('Location and Post Code', ''))
            grouped[key].append({
                'Auction': comp.get('Auction', ''),
                'Delivery Year': comp.get('Delivery Year', '')
            })
        
        # Check for groups with different auction years
        for (cmu_id, location), years_list in grouped.items():
            if len(years_list) > 1:
                # Check if there are different values for auction or delivery year
                auctions = {y['Auction'] for y in years_list}
                delivery_years = {y['Delivery Year'] for y in years_list}
                
                if len(auctions) > 1 or len(delivery_years) > 1:
                    print(f"  Found: CMU ID={cmu_id}, Location={location}")
                    print(f"    Auctions: {sorted(auctions)}")
                    print(f"    Delivery Years: {sorted(delivery_years)}")
    
    except Exception as e:
        print(f"Error processing {json_file}: {e}")

# Get all component JSON files
json_files = glob.glob(os.path.join(json_dir, 'components_*.json'))

# Check each file
print("Checking for components with same CMU ID and location but different auction/delivery years...\n")
for json_file in sorted(json_files):
    check_auction_years(json_file)

print("\nCheck complete. If you see results above, those are components with different auction/delivery years.")
print("If you see no results, it means the exact match filter correctly preserved different auction years.") 