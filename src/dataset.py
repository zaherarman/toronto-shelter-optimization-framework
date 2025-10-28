import requests
import zipfile

from config import RAW_DATA_DIR

base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

# Homeless Shelter Locations
url = base_url + "/api/3/action/package_show"
params = {"id": "hostel-services-homeless-shelter-locations"}
package = requests.get(url, params=params).json()

for idx, resource in enumerate(package["result"]["resources"]):
    if not resource["datastore_active"]:
        url = base_url + "/api/3/action/resource_show?id=" + resource["id"]
        resource_metadata = requests.get(url).json()

        file_url = resource_metadata["result"]["url"]
        file_name = resource_metadata["result"]["name"].replace("/", "_") + "." + resource_metadata["result"]["format"].lower()
        
        print(f"Downloading {file_name} from {file_url}")
        response = requests.get(file_url)

        file_path = RAW_DATA_DIR / file_name
        with open(file_path, "wb") as f:
            f.write(response.content)

        print(f"Saved {file_path}")

# Path to the incorret file name
bad = RAW_DATA_DIR / "shelter-locations-wgs84.shp"

# What it should be
good = RAW_DATA_DIR / "shelter-locations-wgs84.zip"

# Rename it only if it’s really a .zip file 
if bad.exists() and zipfile.is_zipfile(bad):
    bad.rename(good)
    print(f"Renamed to {good.name}")

# Extract it
with zipfile.ZipFile(good, "r") as z:
    z.extractall(RAW_DATA_DIR)
    print(f"Extracted files: {z.namelist()}")

# Delete .zip
good.unlink()

# Daily Shelter & Overnight Service Occupancy & Capacity
url = base_url + "/api/3/action/package_show" 
params = { "id": "daily-shelter-overnight-service-occupancy-capacity"}
package = requests.get(url, params = params).json()
		
for idx, resource in enumerate(package["result"]["resources"]):	
    name = resource["name"].lower()
    
    if "2024" in name and resource["format"].lower() == "csv":
        if not resource["datastore_active"]:
            url = base_url + "/api/3/action/resource_show?id=" + resource["id"]
            resource_metadata = requests.get(url).json()
            print(resource_metadata)
            
            file_url = resource_metadata["result"]["url"]
            file_name = resource_metadata["result"]["name"].replace("/", "_")
            
            print(f"Downloading {file_name} from {file_url}")
            response = requests.get(file_url)

            file_path = RAW_DATA_DIR / file_name
            with open(file_path, "wb") as f:
                f.write(response.content)

            print(f"Saved {file_path}")

# Toronto Shelter System Flow
url = base_url + "/api/3/action/package_show" 
params = { "id": "toronto-shelter-system-flow"}
package = requests.get(url, params = params).json()

for idx, resource in enumerate(package["result"]["resources"]):
    if not resource["datastore_active"]:
        url = base_url + "/api/3/action/resource_show?id=" + resource["id"]
        resource_metadata = requests.get(url).json()

        file_url = resource_metadata["result"]["url"]
        file_name = resource_metadata["result"]["name"].replace("/", "_")
        
        print(f"Downloading {file_name} from {file_url}")
        response = requests.get(file_url)

        file_path = RAW_DATA_DIR / file_name
        with open(file_path, "wb") as f:
            f.write(response.content)

        print(f"Saved {file_path}")

# Neighbourhood Profiles
url = base_url + "/api/3/action/package_show" 
params = { "id": "neighbourhood-profiles"}
package = requests.get(url, params = params).json()

