import json
import os

json_dir = os.path.join(os.getcwd(), 'json_data')
t_file = os.path.join(json_dir, 'components_T.json')

with open(t_file, 'r') as f:
    data = json.load(f)

cmu_ids = ['TS17_1', 'TS17_2', 'TS17_3', 'TS17_4', 'TS17_5']

print(f"Checking CMU IDs: {cmu_ids}")

for cmu in cmu_ids:
    components = data.get(cmu, [])
    print(f"\n{cmu}: {len(components)} components")
    
    for i, comp in enumerate(components):
        location = comp.get('Location and Post Code', 'Unknown')
        if location and 'Trostre Works' in location:
            print(f"  Component {i+1}: {location}")
    
# Check data service to see if it's creating a fake TS17 entry
print("\nChecking for places where we might be combining all TS17_x into TS17:")
import re
with open('checker/services/data_access.py', 'r') as f:
    content = f.read()
    matches = re.findall(r'["\']TS17["\']', content)
    print(f"Found {len(matches)} direct references to 'TS17' in data_access.py")

with open('checker/services/component_detail.py', 'r') as f:
    content = f.read()
    matches = re.findall(r'["\']TS17["\']', content)
    print(f"Found {len(matches)} direct references to 'TS17' in component_detail.py") 