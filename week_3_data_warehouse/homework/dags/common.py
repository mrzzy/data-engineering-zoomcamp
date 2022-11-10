#
# data-engineering-zoomcamp
# Week 3 - Homework
# Ingest Yellow Taxi Data to GCP
#


import json
import os
from typing import Callable, Dict, Iterable, Optional
from airflow.models import Connection
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteTableOperator,
)
from google.cloud.bigquery import TableReference

import requests
from pyarrow import csv, parquet
from airflow.decorators import task, task_group
from airflow.providers.google.cloud.hooks.gcs import GCSHook

BUCKET = "dtc_data_lake_mrzzy-data-eng-zoomcamp"
BQ_DATASET = "nyc_tlc"
GCP_PROJECT = "mrzzy-data-eng-zoomcamp"
GITHUB_DATASET_URL_PREFIX = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
)


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


def build_load_bq_job_config(
    source_urls: Iterable[str],
    dest_table: TableReference,
    partition_by: Optional[str] = None,
    source_format: str = "PARQUET",
) -> Dict:
    """Build the BigQuery Job configuration for a truncate & load Job.

    Args:
        source_urls:
            Pass to configuration as 'sourceUris'.
        dest_table:
            Reference to the destination BigQuery table to load to.
        partition_by:
            Optional. If specified, will configure the destination table to be
            time-partitioned (day granularity) with 'partition_by' as the date key.
        source_format:
            Optional. If left unspecified, would assume that the data source is
            encoded in Parquet files.
    """
    config = {
        "jobType": "LOAD",
        "writeDisposition": "WRITE_TRUNCATE",
        "load": {
            "sourceUris": source_urls,
            "sourceFormat": source_format,
            "destinationTable": dest_table.to_api_repr(),
        },
    }
    if partition_by is not None:
        config["timePartitioning"] = {
            "type": "DAY",
            "field": partition_by,
        }

    return config
