import os, json, shutil
import base64
import re
import numpy as np
# import pygsheets
import pandas as pd
import datetime
import sys
import argparse
import requests
import tenacity
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

dir_path = os.path.dirname(os.path.realpath(__file__))
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = dir_path+"/"+"f3brand-service-account.json"
project_id = "f3brand"
dataset_id = "api_extracted_data"
bucket_name = "f3brand_extracted_data"

def clear_directory(folder_directory):
    print("Function___clear_directory:"+folder_directory)
    for filename in os.listdir(folder_directory):
        file_path = os.path.join(folder_directory, filename)
        try:
            print("Deleting__ "+file_path)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

    return 

def get_gcs_client():
    SCOPES = [
            "https://www.googleapis.com/auth/devstorage.read_write",
        ]
    SERVICE_ACCOUNT_FILE = dir_path+"/"+"f3brand-service-account.json"
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gcs_client = storage.Client(credentials=credentials, project=project_id)
    return gcs_client

def get_bq_client():
    SCOPES = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
    ]
    SERVICE_ACCOUNT_FILE = dir_path+"/"+"f3brand-service-account.json"
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)    
    bq_client = bigquery.Client(credentials=credentials,project=project_id)
    return bq_client

@tenacity.retry(stop=tenacity.stop_after_attempt(10), wait=tenacity.wait_fixed(5))
def update_source_table(query_string, table_name):
    bq_client = get_bq_client()
    print("__QueryString__ "+query_string)
    table_id = f"f3brand.api_extracted_data.{table_name}"
    job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition="WRITE_TRUNCATE",allow_large_results=True)
    query_job = bq_client.query(query_string, job_config=job_config)
    res = query_job.result()
    print("Total rows:"+str(res.total_rows))
    return True

def upload_blob(file_directory,source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = get_gcs_client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_directory+source_file_name)
    print("File {} uploaded to bucket.".format(source_file_name))

def load_gcs_to_bq(uri, dataset_id, table_name, schema=None, overwrite=None):
    client = get_bq_client()
    table_ref = client.dataset(dataset_id).table(table_name)
    job_config = bigquery.LoadJobConfig()
    if not overwrite:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    else:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    if not schema:
        job_config.autodetect = True
    else:
        job_config.schema = schema
    
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    # uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)  # API request
    print("Starting job {}".format(load_job.job_id))
    load_job.result()  # Waits for table load to complete.
    print("Job finished.")
    destination_table = client.get_table(table_ref)

    print("Loaded {} rows.".format(destination_table.num_rows))    

# /Users/apple/BQ-Sid/data-export-api/data-extraction-api/test_20210619_2600.csv
# load_gcs_to_bq(f"gs://{bucket_name}/wufoo_data/{tday}/{temp_wufoo_form_data_table}_{tday}_*", dataset_id, "test_wuffoo", schema=None, overwrite=None)
# upload_blob("/Users/apple/BQ-Sid/data-export-api/data-extraction-api/","test_20210619_100.csv", "20210620/test_20210619_200.csv")