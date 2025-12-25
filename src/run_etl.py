import os
import logging
import pandas as pd
from datetime import datetime

#local modules
from src.config import (
    RAW_DIR,
    CACHE_DIR,
    PROCESSED_DIR,
    GEOCODE_CACHE,
    DATASET_IDS,
    BASE_CKAN_URL,
    BASE_METEO_URL,
    GOOGLE_API_KEY,
    SERVICE_ACCOUNT_CRED_PATH,
    PROJECT_ID,
    PRE_2021_SHELTERS_RENAME,
    POST_2021_SHELTERS_RENAME, 
    BASE_COLUMNS,
    CENSUS_COLS
)

from src.etl import extract
from src.etl import transform
from src.etl import load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)

def main() -> None:
    start_time = datetime.now()
    logging.info("starting ETL pipeline")


    logging.info("extract phase ...")
    raw_files = extract.run(
        dataset_ids=DATASET_IDS,
        raw_dir=RAW_DIR,
        ckan_base_url=BASE_CKAN_URL,
        meteo_base_url=BASE_METEO_URL)
    logging.info("extract complete")


    logging.info("transform phase ...")
    transformed_files = transform.run(
        google_api_key = GOOGLE_API_KEY,
        raw_files = RAW_DIR,
        processed_dir=PROCESSED_DIR,
        cache_dir = CACHE_DIR,
        geocode_cache=GEOCODE_CACHE,
        pre_2021_shelters_rename=PRE_2021_SHELTERS_RENAME,
        post_2021_shelters_rename=POST_2021_SHELTERS_RENAME, 
        base_columns=BASE_COLUMNS,
        census_cols=CENSUS_COLS)
    logging.info("transform complete")


    logging.info("load phase ...")
    load.run(
        creds_path=SERVICE_ACCOUNT_CRED_PATH,
        project_id=PROJECT_ID,
        processed_dir=PROCESSED_DIR)
    logging.info("load phase complete")


    logging.info("pipeline finished successfully")
    execution_time = datetime.now() - start_time
    logging.info(f"total execution time (hh:mm:ss.ms) {execution_time}")

if __name__ == "__main__":
    main()

