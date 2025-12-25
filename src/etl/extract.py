import os
import requests
import logging
import zipfile
import regex as re
from typing import List, Dict
from datetime import datetime

import pandas as pd


def format_filename(name: str, ext: str) -> str:
    '''
    standardizes filenames for shelter and neighbourhood census datasets.

    param name (str):   name of resource
    param ext (str):    file format of resource

    returns (str): formatted filename and extension
    '''
    basename = name.split('.')[0].lower().replace(' ', '-')
    year = (re.search(r"\b(\d{4})\b", basename).group(1) if re.search(r"\b(\d{4})\b", basename) else datetime.now().year)
    if "shelter" in basename:
        slug = f"toronto-shelter-occupancy-{year}"
    elif "neighbourhood" in basename:
        slug = f"toronto-neighbourhood-profiles-{year}"
    filename = f"{slug}.{ext.lower()}"
    return filename

def get_ckan_metadata(ckan_base_url: str, dataset_id: str) -> List[Dict[str, str]]:
    '''
    retrieves metadata for a given dataset from the CKAN API and returns resources in a formatted list

    param ckan_base_url (str):    base URL of the CKAN instance
    param dataset_id (str):       dataset identifier

    returns: list of dictionaries containing url, name, and extenstion, metadata for each resource
    '''
    out = []
    pkg = requests.get(f'{ckan_base_url}/api/3/action/package_show', params = { "id": dataset_id}).json()
    for res in pkg["result"]["resources"]:
        name = res['name']
        ext = res['format']
        if ext in ('XLSX', 'CSV') and ext.lower() not in name.split('.'):
            out.append({'url': res['url'],'name': name,'ext': ext})
    return out

def download_resource(meta: Dict[str, str], dest: str) -> None:
    '''
    downloads a CKAN resource in CSV or Excel formats to the local disk
    
    param meta (Dict[str, str]):    resource metadata
    param dest (str):               output folder
    '''
    os.makedirs(dest, exist_ok=True)
    filename = format_filename(meta['name'], meta['ext'])
    filepath = os.path.join(dest, filename)
    response = requests.get(meta['url'])
    if os.path.exists(filepath):
        return
    if response.status_code == 200:
        with open(filepath, 'wb') as f:
            f.write(response.content)
    else:
        logging.error(f"failed: {filename} ({response.status_code})")

def extract_shelter_locations(raw_dir: str, dataset_key: str, ckan_base_url: str) -> None:
    """
    Download and extract the 'hostel-services-homeless-shelter-locations' dataset.
    Handles the known issue where the download may be a ZIP disguised as .shp.
    """
    logging.info("  fetching CKAN metadata for dataset: shelter_locations")

    pkg = requests.get(
        f"{ckan_base_url}/api/3/action/package_show",
        params={"id": "hostel-services-homeless-shelter-locations"}
    ).json()

    dest = os.path.join(raw_dir, dataset_key)
    os.makedirs(dest, exist_ok=True)

    for res in pkg["result"]["resources"]:
        if res.get("datastore_active"):
            continue

        res_meta = requests.get(
            f"{ckan_base_url}/api/3/action/resource_show",
            params={"id": res["id"]}
        ).json()["result"]

        file_url = res_meta["url"]
        ext = (res_meta.get("format") or "").lower()
        name = (res_meta.get("name") or "resource").replace("/", "_")

        # prevent double extension like ".zip.zip"
        filename = name if name.lower().endswith(f".{ext}") else f"{name}.{ext}"
        path = os.path.join(dest, filename)

        if os.path.exists(path):
            continue

        logging.info("      downloading resource: %s", os.path.basename(path))
        r = requests.get(file_url)
        r.raise_for_status()
        with open(path, "wb") as f:
            f.write(r.content)

        # Rename shp->zip if it is actually a zip file
        if path.lower().endswith(".shp") and zipfile.is_zipfile(path):
            zip_path = path[:-4] + ".zip"
            os.rename(path, zip_path)
            path = zip_path

        # Extract zip if applicable, then delete (matches your script behavior)
        if path.lower().endswith(".zip") and zipfile.is_zipfile(path):
            with zipfile.ZipFile(path, "r") as z:
                z.extractall(dest)
            os.remove(path)

def get_nightly_weather_data(meteo_base_url: str, lat=43.6532, lon=-79.3832,) -> pd.DataFrame:
    '''
    retrieves historical weather data in Toronto since 2017 from the Meteo API

    param meteo_base_url (str): base URL of the Meteo instance
    '''
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": "2017-01-01",
        "end_date": datetime.now().strftime("%Y-%m-%d"),
        "hourly": ",".join([
            "temperature_2m", "apparent_temperature", "precipitation",
            "snowfall", "snow_depth", "cloud_cover", "windspeed_10m",
            "windgusts_10m", "relative_humidity_2m", "weathercode", "visibility"
        ]),
        "timezone": "America/Toronto"
        }
    response = requests.get(meteo_base_url, params=params)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data["hourly"])
    df["time"] = pd.to_datetime(df["time"])
    return df

def run(*, dataset_ids: dict[str, str], raw_dir: str, ckan_base_url: str, meteo_base_url: str) -> None:
    '''
    runs the extract.py file by extracting all needed CKAN and Meteo API data and then downloading to the raw data folder.

    param dataset_ids (dict[str, str]): dictionary of required CKAN datasets and its identifiers
    param raw_dir (str):                directory where all data should be extracted to
    param ckan_base_url (str):          base url of CKAN instance
    param meteo_base_url (str):         base url of Meteo instance
    '''
    extract_shelter_locations(raw_dir, dataset_key="shelter_locations", ckan_base_url=ckan_base_url)

    for id, dataset in dataset_ids.items():
        if id == "shelter_locations":
            continue
        subfolder = os.path.join(raw_dir, id)
        os.makedirs(subfolder, exist_ok=True)
        meta = get_ckan_metadata(ckan_base_url, dataset)
        logging.info("  fetching CKAN metadata for dataset: %s", id)
        for item in meta:
            logging.info("      downloading resource: %s", item["name"])
            download_resource(item, subfolder)
    
    logging.info("  fetching nightly weather data from Meteo API...")
    weather_dir = os.path.join(raw_dir, "weather")
    os.makedirs(weather_dir, exist_ok=True)
    weather_file = os.path.join(weather_dir, "nightly_weather_toronto.csv")
    get_nightly_weather_data(meteo_base_url).to_csv(weather_file, index=False)
    logging.info("  weather data saved to %s", weather_file)