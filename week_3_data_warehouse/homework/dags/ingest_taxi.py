#
# data-engineering-zoomcamp
# Week 2 - Homework
# Ingest Yellow Taxi Data to GCP
#

import gzip
import os
from enum import Enum
from typing import Callable, Optional

import requests
from airflow.decorators import dag, task, task_group
from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteTableOperator,
)
from pendulum import datetime
from pendulum.datetime import DateTime
from pendulum.tz.timezone import UTC
from pyarrow import csv, parquet

BUCKET = "dtc_data_lake_mrzzy-data-eng-zoomcamp"
BQ_DATASET = "nyc_tlc"
GCP_PROJECT = "mrzzy-data-eng-zoomcamp"


class NYTaxiDatasetType(Enum):
    """NY Taxi Dataset variant types"""

    Yellow = "yellow"
    Green = "green"
    ForHire = "fhv"
    Zone = "zone"


def download_file(url: str, path: str, unpack: Callable[[bytes], bytes] = lambda b: b):
    """Download the file at the given URL and writes its contents at path."""
    with requests.get(url) as r, open(path, "wb") as f:
        data = unpack(r.content)
        f.write(data)


def download_gzip(url: str, path: str):
    """Download the GZIP at the given URL and writes its decompressed form at path."""
    download_file(url, path, unpack=gzip.decompress)


def to_parquet_csv(csv_path: str, pq_path):
    """Convert the CSV at the given path to a Parquet file at the given path."""
    table = csv.read_csv(csv_path)
    os.remove(csv_path)
    parquet.write_table(
        table,
        where=pq_path,
        compression="snappy",
    )


def build_dag(
    dataset_type: NYTaxiDatasetType,
    gcp_project: str = GCP_PROJECT,
    bucket: str = BUCKET,
    bq_dataset: str = BQ_DATASET,
) -> DAG:
    """
    Build an Airflow DAG to ingest the NYTaxi Dataset into BigQuery.

    Args:
        dataset_type:
            Variant of the NYTaxi to ingest.
        gcp_project:
            GCP project containing the supporting infrastructure.
        bucket:
            Name of then GCS bucket to use as staging area for BigQuery ingestion.
        bq_dataset
            Name of the BigQuery dataset to ingest to.
    """
    # while other dataset types are partitioned by year-month, the taxi
    # is special in that it only needs to be ingested oncea
    schedule_params = (
        {
            "start_date": datetime(2019, 1, 2, tz=UTC),
            "end_date": datetime(2019, 1, 2, tz=UTC),
        }
        if dataset_type == NYTaxiDatasetType.Zone
        else {
            "start_date": datetime(2019, 1, 2, tz=UTC),
            "end_date": datetime(2021, 8, 2, tz=UTC),
            "schedule_interval": "0 3 2 * *",  # 3am on the 2nd of every month
            "catchup": True,
        }
    )

    @dag(
        dag_id=f"ingest-nyc-tlc-{dataset_type.value}",
        # TODO(mrzzy): pushdown params to specific tasks
        params={
            "project_id": gcp_project,
            "bq_dataset": bq_dataset,
            "retries": 3,
            "retry_delay": 60.0,
            "retry_exponential_backoff": True,
        },
        **schedule_params,
    )
    def build():
        f"""
        Ingest NY Taxi Data ({dataset_type.value}) into BigQuery.

        ## Prerequisites
        Expects a Google Cloud Platform connection configured under the id:
        `google_cloud_default` with GCS & BigQuery IAM permissions.

        Within the GCP project, expects the following infrastructure to be deployed:
        - GCS Bucket: `{bucket}`
        - BigQuery Dataset: `{bq_dataset}`
        """

        @task
        def download(
            dataset_type: NYTaxiDatasetType,
            data_interval_start: Optional[DateTime] = None,
        ) -> str:
            """
            Download & Uncompress Data CSV.
            Returns the path to the downloaded CSV File.
            """
            # download data from github
            dataset_prefix = (
                f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
            )
            if dataset_type == NYTaxiDatasetType.Zone:
                csv_path = "taxi_zone_lookup.csv"
                download_file(f"{dataset_prefix}/misc/taxi_zone_lookup.csv", csv_path)
                return csv_path

            partition = data_interval_start.strftime("%Y-%m")  # type: ignore
            csv_path = f"{dataset_type.value}_tripdata_{partition}.csv"
            download_gzip(
                url=f"{dataset_prefix}/{dataset_type.value}/{dataset_type.value}_tripdata_{partition}.csv.gz",
                path=csv_path,
            )
            return csv_path

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
        def upload_gcs(src_path: str, prefix: str, bucket: str = bucket) -> str:
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
            dataset_type: NYTaxiDatasetType,
            gs_path: str,
        ):
            """
            Register the given Parquet file on the GCS Bucket into BigQuery as a external table.
            """
            # fully replace existing table with new table if it already exists
            table_name = dataset_type.value + (
                ""
                if dataset_type == NYTaxiDatasetType.Zone
                else "_{{ data_interval_start.strftime('%Y_%m') }}"
            )

            remove_existing = BigQueryDeleteTableOperator(
                task_id="remove_existing_table",
                deletion_dataset_table="{{ params.bq_dataset }}.%s" % table_name,
                ignore_if_missing=True,
            )

            register_bq = BigQueryCreateExternalTableOperator(
                task_id="ingest_parquet_bq_table",
                table_resource={
                    "tableReference": {
                        "datasetId": "{{ params.bq_dataset }}",
                        "tableId": table_name,
                    },
                    "externalDataConfiguration": {
                        "sourceUris": [gs_path],
                        "sourceFormat": "PARQUET",
                    },
                },
            )

            remove_existing >> register_bq  # type: ignore

        # define dag
        csv_path = download(dataset_type)
        pq_path = convert_parquet(csv_path)
        gs_path = upload_gcs(pq_path, prefix=f"nyc_tlc/{dataset_type.value}/raw")
        register_external_bq_table(dataset_type, gs_path)

    return build()


yellow_dag = build_dag(NYTaxiDatasetType.Yellow)
for_hire_dag = build_dag(NYTaxiDatasetType.ForHire)
zone_dag = build_dag(NYTaxiDatasetType.Zone)
