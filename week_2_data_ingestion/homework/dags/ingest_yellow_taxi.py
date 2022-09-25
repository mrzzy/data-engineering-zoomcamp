#
# data-engineering-zoomcamp
# Week 2 - Homework
# Ingest Yellow Taxi Data to GCP
#

import gzip
import requests

from typing import Optional

from airflow.decorators import dag, task
from pendulum import datetime
from pendulum.datetime import DateTime
from pyarrow import csv, parquet
from airflow.providers.google.cloud.hooks.gcs import GCSHook

NEW_YORK_TIMEZONE = "US/Eastern"
BUCKET = "dtc_data_lake_mrzzy-data-eng-zoomcamp"
DATASET_PREFIX = "nyc_tlc"


@dag(
    dag_id="ingest-yellow-taxi",
    start_date=datetime(2019, 1, 1, tz=NEW_YORK_TIMEZONE),
    end_date=datetime(2021, 7, 1, tz=NEW_YORK_TIMEZONE),
    schedule_interval="0 3 2 * *",  # 3am on the 2nd of every month
)
def build_dag():
    """
    Ingest NY Taxi Yellow Cab Data into BigQuery.
    Expects a Google Cloud Platform connection configured under the id:
    `google_cloud_default` with GCS & BigQuery IAM permissions
    """

    @task(pool="github_api")
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
            compresssion="snappy",
        )
        return pq_path

    @task
    def upload_gcs(
        src_path: str, bucket: str = BUCKET, prefix: str = DATASET_PREFIX
    ) -> str:
        """
        Upload the file at the given path to the GCS with the given destination prefix.
        Returns location the file was uploaded to in GCS as a 'gs://' URL.
        """
        gcs, dest_path = GCSHook(), f"{prefix}/{src_path}"
        gcs.upload(
            bucket_name=bucket,
            object_name=dest_path,
            filename=src_path,
        )
        return f"gs://{bucket}/{dest_path}"

    # define dag
    csv_path = download()
    pq_path = convert_parquet(csv_path)
    gs_path = upload_gcs(pq_path)
