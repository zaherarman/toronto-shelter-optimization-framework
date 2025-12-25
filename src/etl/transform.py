import os
import requests
import logging
import warnings
from datetime import datetime
from typing import List, Dict

import numpy as np
import pandas as pd

import geopandas as gpd
from shapely.geometry import shape, Point


warnings.simplefilter(action='ignore', category=FutureWarning)

def load_local_files(raw_folder: str, subfolder: str) -> dict[str, str]:
    '''
    loads raw data files from a given subfolder within data\raw

    param raw_folder (str): base directory containing the raw data
    param subfolder (str):  subfolder containing specific raw dataset files

    returns (dict[str,str]): metadata of all resources in the subfolder
    '''
    folder = os.path.join(raw_folder, subfolder) if subfolder else raw_folder
    files = [file for file in os.listdir(folder) if not file.startswith('.')]
    if len(files) == 1:
        file = files[0]
        path = os.path.join(folder, file)
        return pd.read_csv(path) if file.lower().endswith('.csv') else pd.read_excel(path)

    data = {}
    for file in files:
        path = os.path.join(folder, file)
        ext = file.split('.')[-1]
        if ext == 'csv':
            df = pd.read_csv(path)
        elif ext == 'xlsx':
            df = pd.read_excel(path)
        key = int(file.split('-')[-1].split('.')[0])
        data.update({key:df})
    return data

def save_geocode_cache(cache_dir: str, geocode_cache_df: pd.DataFrame) -> None:
    '''
    saves DataFrame containing latitude and longitude for a list of addresses to the cache directory

    param cache_dir (str):          base directory of data cache
    param geocode_cache_df (str):   directory of cached geocode data
    '''
    logging.info('  saving to geocode cache')
    path = os.path.join(cache_dir, "geocoded_addresses.csv")
    geocode_cache_df.reset_index().to_csv(path, index=False)


def rename_and_standardize(
        pre_2021_shelters_rename: list[dict[str,str]], 
        post_2021_shelters_rename: list[dict[str,str]], 
        base_columns: list[str], 
        df: pd.DataFrame, 
        year: int
        ) -> pd.DataFrame:
    '''
    standardizes column names and schemas for shelter data across pre-2021 and post-2021 formats

    param pre_2021_shelters_rename (dict[str, str]): mapping of original to standardized column names for data before 2021.
    param post_2021_shelters_rename (dict[str, str]): mapping of original to standardized column names for data after 2021.
    param base_columns (list[str]): list of columns to keep after renaming.
    param df (pd.DataFrame): shelter dataset for a single year.
    param year (int): year of the dataset.

    returns (pd.DataFrame): shelter DataFrame with standardized columns and added schema/year metadata.
    '''
    df.columns = [col.upper() for col in df.columns]
    if year <= 2020:
        df = df.rename(columns = pre_2021_shelters_rename)
        df['data_schema'] = 'pre_2021'
        for col in reversed(['capacity_rooms', 'occupancy_rooms', 'capacity_type']):
            df.insert(1, col, None)  
    else:
        df = df.rename(columns = post_2021_shelters_rename)
        df['data_schema'] = 'post_2021'
    df['year'] = year
    return df[base_columns]

def engineer_occupancy(df: pd.DataFrame) -> pd.DataFrame:
    '''
    calculates occupancy rate metrics for room-based, bed-based, and returns combined capacities

    param df (pd.DataFrame): shelter DataFrame without occupancy rates

    returns (pd.DataFrame): shelter DataFrame with combined occupancy rates and removed bed-based and occupancy-based specific columns
    '''
    df = df.copy()
    df['occupancy_rate_beds'] = df['occupancy_beds'] / df['capacity_beds']
    df['occupancy_rate_rooms'] = df['occupancy_rooms'] / df['capacity_rooms']
    df['capacity_combined'] = df['capacity_beds'].fillna(df['capacity_rooms'])
    df['occupancy_combined'] = df['occupancy_beds'].fillna(df['occupancy_rooms'])
    df['occupancy_rate_combined'] = (df['occupancy_combined'] / df['capacity_combined'])

    df = df.replace([np.inf, -np.inf], np.nan)

    df['capacity_combined'] = df['capacity_combined'].fillna(0)
    df['occupancy_rate_combined'] = df['occupancy_rate_combined'].fillna(0)
    return df

def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    '''
    standardizes date formats for the shelter datasets to be follow ISO 8601 and converts to datetime objects

    param df (pd.DataFrame): shelter dataset containing raw date strings and year column

    returns (pd.DataFrame): DataFrame with parsed and standardized occupancy dates
    '''
    mask_2020 = df['year'] == 2020
    mask_2021_2022 = df['year'].isin([2021, 2022])
    mask_2024_2025 = df['year'].isin([2024, 2025])
    mask_standard = df['year'].isin([2017, 2018, 2019, 2023])
    #CKAN 2020 shelter files use MM/DD/YYYY
    df.loc[mask_2020, 'occupancy_date'] = pd.to_datetime(df.loc[mask_2020, 'occupancy_date'], format='%m/%d/%Y', errors='coerce')
    #CKAN 2021-2022 shelter files use YY-MM-DD
    df.loc[mask_2021_2022, 'occupancy_date'] = pd.to_datetime(df.loc[mask_2021_2022, 'occupancy_date'], format='%y-%m-%d', errors='coerce')
    #CKAN 2024-2025 shelter files use YYYY-MM-DD
    df.loc[mask_2024_2025, 'occupancy_date'] = pd.to_datetime(df.loc[mask_2024_2025, 'occupancy_date'], format='%Y-%m-%d', errors='coerce')
    #CKAN 2017-2019 and 2023 shelter files use ISO-8601
    df.loc[mask_standard, 'occupancy_date'] = pd.to_datetime(df.loc[mask_standard, 'occupancy_date'], errors='coerce')
    #adds 4 hours so all timestamps reflect daily count taken at 4AM EST
    df['occupancy_date'] = df['occupancy_date'] + pd.Timedelta(hours=4)
    df['occupancy_date'] = pd.to_datetime(df['occupancy_date'], errors='coerce')
    return df


def add_calendar_columns(df: pd.DataFrame) -> pd.DataFrame:
    '''
    adds calendar-based columns derived from the occupancy date

    param df (pd.DataFrame): dataset with 'occupancy_date' as a datetime column

    returns (pd.DataFrame): DataFrame with month, day, weekday, weekend columns.
    '''
    df = df.copy()
    df["month"] = df["occupancy_date"].dt.month
    df["day"] = df["occupancy_date"].dt.day
    df["weekday"] = df["occupancy_date"].dt.day_name()
    df["is_weekend"] = df["weekday"].isin(["Saturday", "Sunday"])
    return df

def impute_addresses(df: pd.DataFrame) -> pd.DataFrame:
    '''
    fills missing shelter names, addresses, cities, and postal codes using mapping from known values

    param df (pd.DataFrame): shelter dataset with missing location fields

    returns (pd.DataFrame): imputed shelter dataset
    '''
    program_to_shelter = df.dropna(subset=['program_name', 'shelter_name']).drop_duplicates(subset=['program_name']).set_index('program_name')['shelter_name'].to_dict()
    df["shelter_name"] = df["shelter_name"].fillna(df["program_name"].map(program_to_shelter))

    shelter_to_city = df.dropna(subset=['shelter_name', 'shelter_city']).drop_duplicates(subset=['shelter_name']).set_index('shelter_name')['shelter_city'].to_dict()
    df["shelter_address"] = df["shelter_address"].fillna(df["shelter_name"].map(shelter_to_city))

    shelter_to_address = df.dropna(subset=['shelter_name',  'shelter_address']).drop_duplicates(subset=['shelter_name']).set_index('shelter_name')['shelter_address'].to_dict()
    df["shelter_city"] = df["shelter_city"].fillna(df["shelter_name"].map(shelter_to_address))

    address_to_postal = df.dropna(subset=['shelter_address', 'shelter_postal_code']).drop_duplicates(subset=['shelter_address']).set_index('shelter_address')['shelter_postal_code'].to_dict()
    df["shelter_postal_code"] = df["shelter_postal_code"].fillna(df["shelter_address"].map(address_to_postal))
    return df

def filter_critical(df: pd.DataFrame) -> pd.DataFrame:
    '''
    drops rows missing critical information

    param df (pd.DataFrame): shelter dataset with possibly missing rows in critical columns

    returns (pd.DataFrame): filtered DataFrame
    '''
    critical_columns = ['program_name', 'shelter_name', 'shelter_address', 'shelter_city', 'shelter_postal_code', 'capacity_combined']
    return df.dropna(subset=critical_columns)



def geocode_address_cached(geocode_cache_df: pd.DataFrame, google_api_key: str, address: str) -> pd.Series:
    '''
    retrieves latitude and longitude for an address, using a cache to reduce API calls

    param geocode_cache_df (pd.DataFrame): DataFrame containing cached geocoded addresses, indexed by address
    param google_api_key (str): Google Maps Geocoding API key
    param address (str): street address to geocode

    returns (pd.Series): latitude and longitude as a Series with keys 'lat' and 'lon'
    '''
    if address in geocode_cache_df.index:
        return geocode_cache_df.loc[address]

    response = requests.get('https://maps.googleapis.com/maps/api/geocode/json',params={'address': address, 'key': google_api_key})
    result = response.json()
    if result['status'] == 'OK':
        location = result['results'][0]['geometry']['location']
        coords = pd.Series({'lat': location['lat'], 'lon': location['lng']})
    else:
        coords = pd.Series({'lat': None, 'lon': None})

    #update the cache
    geocode_cache_df.loc[address] = coords
    return coords

def geocode_and_join_neighbourhoods(geocode_cache_df, google_api_key, df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    geocodes unique shelter addresses and spatially join them with neighbourhood boundaries

    param geocode_cache_df (pd.DataFrame): cached geocode results
    param google_api_key (str): Google Maps Geocoding API key
    param df (pd.DataFrame): shelter dataset containing 'shelter_address'

    returns (gpd.GeoDataFrame): GeoDataFrame with geocoded points and neighbourhood attributes
    """
    unique_addresses = df['shelter_address'].dropna().unique()
    geocoded_df = pd.DataFrame(unique_addresses, columns=['shelter_address'])
    geocoded_df[['lat', 'lon']] = (geocoded_df['shelter_address'].apply(lambda addr: geocode_address_cached(geocode_cache_df, addr, google_api_key)).apply(pd.Series))
    df = df.merge(geocoded_df, on='shelter_address', how='left')

    df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['lon'], df['lat']), crs='EPSG:4326')

    gdf_neigh = gpd.read_file("data/raw/neighbourhoods_4326.geojson")
    df = gpd.sjoin(df, gdf_neigh, how='left', predicate='within')
    df.columns = df.columns.str.lower()
    return df

def transform_shelter_data(google_api_key, pre_2021_shelters_rename, post_2021_shelters_rename, base_columns, geocode_cache_df, raw: dict[int, pd.DataFrame]) -> gpd.GeoDataFrame:
    """
    applies full transformation pipeline to shelter data

    param google_api_key (str): Google Maps Geocoding API key
    param pre_2021_shelters_rename (dict[str, str]): column rename mapping for pre-2021 data
    param post_2021_shelters_rename (dict[str, str]): column rename mapping for post-2021 data
    param base_columns (list[str]): columns to retain after renaming
    param geocode_cache_df (pd.DataFrame): cached geocode results
    param raw (dict[int, pd.DataFrame]): dictionary of year to raw shelter DataFrames

    returns (gpd.GeoDataFrame): fully transformed and geocoded shelter dataset.
    """
    
    frames = [rename_and_standardize(pre_2021_shelters_rename, post_2021_shelters_rename, base_columns, df, year=yr) for yr, df in raw.items()]
    df = pd.concat(frames, ignore_index=True)

    #dates / occupancy
    df = engineer_occupancy(df)
    df = parse_dates(df)
    df = add_calendar_columns(df)

    #impute + filter
    df = impute_addresses(df)
    df["capacity_type"] = df["capacity_type"].fillna("Bed Based Capacity")
    df = df.rename(columns = {'occupancy_date':'date_time'})
    df = filter_critical(df)

    #geocode
    df = geocode_and_join_neighbourhoods(geocode_cache_df, google_api_key, df)

    #final column drops
    #1) removing unneeded capacity/occupancy columns
    df = df.drop(columns = ['occupancy_beds','occupancy_rooms','capacity_beds','capacity_rooms','occupancy_rate_beds','occupancy_rate_rooms'], errors="ignore")

    #2) removing unneeded geocode columns
    df = df.drop(columns = ['lat','lon','index_right','_id','area_id','area_attr_id','parent_area_id','area_short_code','area_long_code','area_desc','classification','classification_code','objectid'], errors="ignore")
    return df

def transform_neighbourhood_census_data(census_cols: list[str], raw: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """
    transforms neighbourhood census data into a standardized tabular format

    param census_cols (list[str]): list of census column names to retain
    param raw (dict[int, pd.DataFrame]): dictionary of year to raw census DataFrames

    returns (pd.DataFrame): transformed census dataset with selected columns and year field
    """
    frames = []
    for year in [2021]:
        df_raw = raw.get(year)
        if df_raw is None:
            raise ValueError(f"No data found for year {year}")

        #moves first column values into row index
        indicators = df_raw.iloc[:, 0]
        data_only = df_raw.iloc[1:].copy()
        data_only.index = indicators[1:]

        #transpose so indicators bcome columns
        df_transposed = data_only.transpose()
        df_transposed.columns = df_transposed.iloc[0]
        df_transposed = df_transposed.drop(df_transposed.index[0])
        df_transposed = df_transposed.reset_index().rename(columns={'index': 'area_name'})

        #standardizing column names
        df_transposed.columns = df_transposed.columns.astype(str).str.strip().str.lower().str.replace(' ', '_', regex=False).str.replace('-','', regex=False).str.replace('($)','', regex=False).str.replace('(limat)','', regex=False)

        #keeping only relevant columns
        available_cols = [col for col in census_cols if col in df_transposed.columns]
        df_filtered = df_transposed[available_cols].copy()
        df_filtered["year"] = year

        frames.append(df_filtered)
    
    return pd.concat(frames, ignore_index=True)

def transform_weather_data(raw: pd.DataFrame) -> pd.DataFrame:
    """
    cleans weather dataset by dropping unused columns and filling missing values

    param raw (pd.DataFrame): raw weather dataset

    returns (pd.DataFrame): cleaned weather dataset
    """
    df = raw.drop(columns='visibility', errors='ignore')
    df['snow_depth'] = df['snow_depth'].fillna(0)
    df = df.rename(columns={'time':'date_time'})
    df = df.dropna()
    return df

def run(*, google_api_key: str,
        raw_files: list[str],
        processed_dir: str,
        cache_dir: str,
        geocode_cache: pd.DataFrame,
        pre_2021_shelters_rename: list[dict[str,str]],
        post_2021_shelters_rename: list[dict[str,str]], 
        base_columns: list[str],
        census_cols: list[str]) -> None:
    '''
    runs the transformation of shelter, census, and weather datasets. Loads raw files, applies transformations, and saves processed outputs

    param google_api_key (str): Google Maps Geocoding API key
    param raw_files (str): path to the raw data directory
    param processed_dir (str): Directory where processed datasets will be stored
    param cache_dir (str): directory for storing the geocode cache
    param geocode_cache pd.DataFrame: Cached geocode results
    param pre_2021_shelters_rename (dict[str, str]): column rename mapping for pre-2021 data
    param post_2021_shelters_rename (dict[str, str]): column rename mapping for post-2021 data
    param base_columns (list[str]): columns to retain in the final shelter dataset
    param census_cols (list[str]): columns to retain in the transformed census dataset
    '''

    #__________transforming shelter data__________
    logging.info('  processing shelter occupancy data ...')
    logging.info("      loading shelter files ...")
    raw = {}
    raw.update(load_local_files(raw_files, 'shelters_2017_to_2020'))
    raw.update(load_local_files(raw_files, 'shelters_2021_to_present'))
    logging.info("      shelter files loaded: %d yearly frames", len(raw))
    logging.info("      transforming shelter data ...")
    gdf = transform_shelter_data(google_api_key, pre_2021_shelters_rename, post_2021_shelters_rename, base_columns, geocode_cache, raw)
    gdf.drop(columns=["geometry"], inplace=False).to_csv(os.path.join(processed_dir, "shelter_occupancy_processed.csv"), index=False)
    save_geocode_cache(cache_dir, geocode_cache)
    logging.info("      shelter data saved to %s", os.path.join(processed_dir, "shelter_occupancy_processed.csv"))

    #__________transforming neighbourhood census data__________
    logging.info('  processing neighbourhood census data ...')
    logging.info("      loading neighbourhood census data ...")
    census_raw = load_local_files(raw_files, 'neighbourhood_profiles')
    logging.info("      transforming neighbourhood census data ...")
    census_data = transform_neighbourhood_census_data(census_cols, census_raw)
    census_data.to_csv(os.path.join(processed_dir, "neighbourhood_census_2016_2021.csv"), index=False)
    logging.info("      census data saved %s", os.path.join(processed_dir, "neighbourhood_census_2016_2021.csv"))

    #__________transforming weather data__________
    logging.info('  processing weather data ...')
    logging.info("      loading weather data ...")
    weather_raw = load_local_files(raw_files, 'weather')
    logging.info("      transforming weather data ...")
    weather_data = transform_weather_data(weather_raw)
    weather_data.to_csv(os.path.join(processed_dir, "weather_since_2017.csv"), index=False)
    logging.info("      weather data saved to %s", os.path.join(processed_dir, "weather_since_2017.csv"))