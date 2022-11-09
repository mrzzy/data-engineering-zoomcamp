#
# data-engineering-zoomcamp
# Week 3 - Homework
# Ingest Yellow Taxi Data to GCP
#


import os
from collections.abc import Callable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryDeleteTableOperator

import requests
from pyarrow import csv, parquet
from airflow.decorators import task, task_group
from airflow.providers.google.cloud.hooks.gcs import GCSHook

BUCKET = "dtc_data_lake_mrzzy-data-eng-zoomcamp"
BQ_DATASET = "nyc_tlc"
GCP_PROJECT = "mrzzy-data-eng-zoomcamp"
GITHUB_DATASET_URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

def download_file(url: str, path: str, unpack: Callable[[bytes], bytes] = lambda b: b):
    """Download the file at the given URL and writes its contents at path."""
    with requests.get(url) as r, open(path, "wb") as f:
        data = unpack(r.content)
        f.write(data)

def to_parquet_csv(csv_path: str, pq_path):
    """Convert the CSV at the given path to a Parquet file at the given path."""
    table = csv.read_csv(csv_path)
    os.remove(csv_path)
    parquet.write_table(
        table,
        where=pq_path,
        compression="snappy",
    )

@task
def convert_parquet(csv_path: str) -> str:
    """
    Converts the CSV file at the given file to Parquet.
        Returns the path to the converted Parquet File.
    """
    # rewrite csv as parquet file
    pq_path = csv_path.replace("csv", "pq")
    to_parquet_csv(csv_path, pq_path)
    return pq_path

@task
def upload_gcs(src_path: str, prefix: str, bucket: str) -> str:
    """
    Upload the file at the given path to the GCS with the given destination prefix.
            Returns fully qualified GCS path to the file
    """
    gcs, dest_path = GCSHook(), f"{prefix}/{src_path}"
    gcs.upload(
        bucket_name=bucket,
        object_name=dest_path,
        filename=src_path,
    )
    os.remove(src_path)
    return f"gs://{bucket}/{dest_path}"

@task_group
def register_external_bq_table(
    gs_path: str,
    bq_dataset: str,
    table_name: str,
):
    """
    Register the given Parquet file on the GCS Bucket into BigQuery as a external table.
    Returns the dataset-qualified ID for the registered BigQuery table.
    """
    # fully replace existing table with new table if it already exists
    remove_existing = BigQueryDeleteTableOperator(
        task_id="remove_existing_table",
        deletion_dataset_table=f"{bq_dataset}.{table_name}",
        ignore_if_missing=True,
    )

    register_bq = BigQueryCreateExternalTableOperator(
        task_id="ingest_parquet_bq_table",
        table_resource={
            "tableReference": {
                "datasetId": bq_dataset,
                "tableId": table_name,
            },
            "externalDataConfiguration": {
                "sourceUris": [gs_path],
                "sourceFormat": "PARQUET",
            },
        },
    )

    remove_existing >> register_bq  # type: ignore
