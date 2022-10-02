#
# data-engineering-zoomcamp
# Week 2 - Homework
# Ingest Yellow Taxi Data to GCP
#

import gzip
from pendulum.tz.timezone import UTC
import requests

from typing import Optional

from airflow.decorators import dag, task, task_group
from pendulum import datetime
from pendulum.datetime import DateTime
from pyarrow import csv, parquet
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteTableOperator,
)

BUCKET = "dtc_data_lake_mrzzy-data-eng-zoomcamp"
DATASET = "nyc_tlc"
GCP_PROJECT = "mrzzy-data-eng-zoomcamp"

@dag(
    dag_id="ingest-yellow-taxi",
    start_date=datetime(2019, 1, 2, tz=UTC),
    end_date=datetime(2021, 8, 2, tz=UTC),
    schedule_interval="0 3 2 * *",  # 3am on the 2nd of every month
    catchup=False,
    params={
        "project_id": GCP_PROJECT,
        "dataset": DATASET,
    },
)
def build_dag():
    """
    Ingest NY Taxi Yellow Cab Data into BigQuery.
    Expects a Google Cloud Platform connection configured under the id:
    `google_cloud_default` with GCS & BigQuery IAM permissions
    """

    @task
    def download(data_interval_start: Optional[DateTime] = None) -> str:
        """
        Download & Uncompress Yellow Cab Data CSV.
        Returns the path to the downloaded CSV File.
        """
        # download gzipped data into buffer
        partition = data_interval_start.strftime("%Y-%m")  # type: ignore
        csv_path = f"yellow_tripdata_{partition}.csv"
        with requests.get(
            f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{partition}.csv.gz"
        ) as r, open(csv_path, "wb") as f:
            csv = gzip.decompress(r.content)
            f.write(csv)

        return csv_path

    @task
    def convert_parquet(csv_path: str) -> str:
        """
        Converts the CSV file at the given file to Parquet.
        Returns the path to the converted Parquet File.
        """
        # rewrite csv as parquet file
        table = csv.read_csv(csv_path)
        pq_path = csv_path.replace("csv", "pq")
        parquet.write_table(
            table,
            where=pq_path,
            compression="snappy",
        )
        return pq_path

    @task
    def upload_gcs(src_path: str, bucket: str = BUCKET, prefix: str = DATASET) -> str:
        """
        Upload the file at the given path to the GCS with the given destination prefix.
        Returns fully qualified GCS path to the file
        """
        gcs, dest_path = GCSHook(), f"{prefix}/{src_path}"
        gcs.upload(
            bucket_name=bucket,
            object_name=dest_path,
            filename=src_path,
            chunk_size=8 * 1024 * 1024, # 8MB
        )
        return f"gs://{bucket}/{dest_path}"

    @task_group
    def register_bq_table(
        gs_path: str,
    ):
        """
        Register the given Parquet file on the GCS Bucket into BigQuery as a external table.
        """
        # fully replace existing table with new table if it already exists
        table_name = "yellow_{{ data_interval_start.strftime('%Y_%m') }}"

        remove_existing = BigQueryDeleteTableOperator(
            task_id="remove_existing_table",
            deletion_dataset_table="{{ params.dataset }}.%s" % table_name,
            ignore_if_missing=True,
        )

        register_bq = BigQueryCreateExternalTableOperator(
            task_id="ingest_parquet_bq_table",
            table_resource={
                "tableReference": {
                    "datasetId": "{{ params.dataset }}",
                    "tableId": table_name,
                },
                "externalDataConfiguration": {
                    "sourceUris": [gs_path],
                    "sourceFormat": "PARQUET",
                },
            }
        )

        remove_existing >> register_bq  # type: ignore

    # define dag
    csv_path = download()
    pq_path = convert_parquet(csv_path)
    gs_path = upload_gcs(pq_path, prefix=f"{DATASET}/raw")

    register_bq = register_bq_table(gs_path)
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset", dataset_id=DATASET, exists_ok=True,
        location="europe-west6",
    )
    create_dataset >> register_bq # type: ignore


dag = build_dag()
