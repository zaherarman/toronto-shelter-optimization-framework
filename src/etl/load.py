import os
import logging

import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict


def create_bigquery_client(*, creds_path: str, project_id:str) -> bigquery.Client:
    '''
    creates an authenticated BigQuery client from a Service Account key

    param creds_path (str): path to JSON credential in local storage
    param project_id (str): Google Cloud project identifier

    returns (bigquery.Client): authenticated client
    '''
    creds  = service_account.Credentials.from_service_account_file(creds_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return bigquery.Client(project=project_id, credentials=creds)

def create_dataset(*, client: str, project_id: str, dataset_name ='shelter_project'):
    '''
    ensures the dataset exists and returns the dataset reference
    '''
    logging.info("  checking for existing datasets")
    dataset_ref = f"{project_id}.{dataset_name}"
    dataset     = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    try:
        client.create_dataset(dataset)
        logging.info(f" none found; created dataset {dataset_ref}")
    except Conflict:
        logging.info("  dataset already exists; skipping.")
    return dataset_ref

def initialize_bigquery(*, creds_path: str, project_id: str):
    '''
    initialize BQ client, creates dataset, and creates job configuration

    param creds_path (str): path to the Google Cloud Service Account JSON key file
    param project_id (str): Google Cloud project identifier

    returns (bigquery.Client, str, bigquery.LoadJobConfig): client instance, dataset reference, and load job configuration
    '''
    logging.info('  initializing BigQuery client')
    client = create_bigquery_client(creds_path = creds_path, project_id = project_id)
    dataset_ref = create_dataset(client = client, project_id = project_id)
    job_config = bigquery.LoadJobConfig(
        source_format      = bigquery.SourceFormat.CSV,
        skip_leading_rows  = 1,
        autodetect         = True,
        write_disposition  = "WRITE_TRUNCATE"
    )
    return client, dataset_ref, job_config

def load_to_bigquery(client, dataset_ref, job_config, processed_dir):
    files = [file for file in os.listdir(processed_dir) if not file.startswith('.')]
    '''
    loads all processed CSV files in the processed directory into a BigQuery table
    '''

    for file in files:
        file_path = f"data/processed/{file}"
        table_id = f"{dataset_ref}.{file.split('.')[0]}"
        with open(file_path, "rb") as f:
            load_job = client.load_table_from_file(f, table_id, job_config=job_config)

        load_job.result()
        table = client.get_table(table_id)
        logging.info("  loaded file %s to BigQuery successfully (%d rows, %d columns)", file.split('.')[-2], table.num_rows, len(table.schema))

def run(*, creds_path: str, project_id: str, processed_dir: str):
    '''
    runs the BigQuery loading for processed CSV files

    param creds_path (str):     path to the Google Cloud Service Account JSON key file
    param project_id (str):     Google Cloud project identifier
    param processed_dir (str):  folder containing processed data
    '''
    client, dataset_ref, job_config = initialize_bigquery(
        creds_path=creds_path,
        project_id=project_id,
    )
    load_to_bigquery(client, dataset_ref, job_config, processed_dir)