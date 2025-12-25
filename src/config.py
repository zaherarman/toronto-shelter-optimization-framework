import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Paths
PROJ_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJ_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

#secrets
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if GOOGLE_API_KEY is None:
    raise RuntimeError("GOOGLE_API_KEY environment variable not set")

SERVICE_ACCOUNT_CRED_PATH = os.getenv("SERVICE_ACCOUNT_CRED_PATH") 
if SERVICE_ACCOUNT_CRED_PATH is None:
    raise RuntimeError("SERVICE_ACCOUNT_CRED_PATH environment variable not set")

PROJECT_ID = os.getenv("PROJECT_ID")

#directories
RAW_DIR       = os.getenv("RAW_DIR", "data/raw")
CACHE_DIR     = os.getenv("CACHE_DIR", "data/cache")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "data/processed")

#ensuring the folders exist
for d in (RAW_DIR, CACHE_DIR, PROCESSED_DIR):
    os.makedirs(d, exist_ok=True)

#geocode cache (CSV file)
cache_file = os.path.join(CACHE_DIR, "geocoded_addresses.csv")

#geocode cache helper
def empty_cache() -> pd.DataFrame:
    """
    Return an empty geocode-cache DataFrame whose index
    is 'shelter_address' and whose columns are ['lat', 'lon'].
    """
    df = pd.DataFrame(columns=["shelter_address", "lat", "lon"])
    return df.set_index("shelter_address")

if os.path.exists(cache_file):
    try:
        tmp = pd.read_csv(cache_file)
        if "shelter_address" in tmp.columns:
            GEOCODE_CACHE = tmp.set_index("shelter_address")
        else:
            GEOCODE_CACHE = empty_cache()
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        GEOCODE_CACHE = empty_cache()
else:
    GEOCODE_CACHE = empty_cache()

#constants

BASE_CKAN_URL  = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
BASE_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

DATASET_IDS = {
    "shelter_locations": "hostel-services-homeless-shelter-locations",
    "shelters_2021_to_present": "daily-shelter-overnight-service-occupancy-capacity",
    "shelters_2017_to_2020"   : "daily-shelter-occupancy",
    "neighbourhood_profiles"  : "neighbourhood-profiles",
    "shelter_system_flow": "toronto-shelter-system-flow"
}

PRE_2021_SHELTERS_RENAME = {
    'OCCUPANCY_DATE': 'occupancy_date',
    'SHELTER_NAME': 'shelter_name',
    'PROGRAM_NAME': 'program_name',
    'SECTOR': 'sector',
    'OCCUPANCY': 'occupancy_beds',
    'CAPACITY': 'capacity_beds',
    'SHELTER_ADDRESS': 'shelter_address',
    'SHELTER_CITY': 'shelter_city',
    'SHELTER_POSTAL_CODE': 'shelter_postal_code'
}

POST_2021_SHELTERS_RENAME = {
    'OCCUPANCY_DATE': 'occupancy_date',
    'LOCATION_NAME': 'shelter_name',
    'PROGRAM_NAME': 'program_name',
    'SECTOR': 'sector',
    'CAPACITY_ACTUAL_BED': 'capacity_beds',
    'OCCUPIED_BEDS': 'occupancy_beds',
    'CAPACITY_ACTUAL_ROOM': 'capacity_rooms',
    'OCCUPIED_ROOMS': 'occupancy_rooms',
    'LOCATION_ADDRESS': 'shelter_address',
    'LOCATION_CITY': 'shelter_city',
    'LOCATION_POSTAL_CODE': 'shelter_postal_code',
    'CAPACITY_TYPE': 'capacity_type'
}

BASE_COLUMNS = [
    'occupancy_date', 'shelter_name', 'program_name', 
    'sector', 'capacity_beds', 'occupancy_beds', 
    'capacity_rooms', 'occupancy_rooms', 'shelter_address', 
    'shelter_city', 'shelter_postal_code', 'data_schema',
    'year', 'capacity_type'
]

CENSUS_COLS = [
    'area_name',
    'total_age_groups_of_the_population_25%_sample_data',
    'average_age_of_the_population',
    'median_age_of_the_population',
    'total_persons_in_private_households_25%_sample_data',
    'number_of_persons_in_private_households',
    'total_income_statistics_in_2020_for_the_population_aged_15_years_and_over_in_private_households_25%_sample_data',
    'median_total_income_in_2020_among_recipients_($)',
    'average_total_income_in_2020_among_recipients_($)',
    'total_lim_lowincome_status_in_2020_for_the_population_in_private_households_25%_sample_data',
    'in_low_income_based_on_the_lowincome_measure,_after_tax_(limat)',
    'total_private_households_by_household_size_25%_sample_data',
    'average_household_size'
]