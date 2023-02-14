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

NEW_YORK_TIMEZONE = "US/Eastern"

@dag(
    dag_id="ingest-yellow-taxi",
    start_date=datetime(2019, 1, 1, tz=NEW_YORK_TIMEZONE), 
    end_date=datetime(2021, 7, 1, tz=NEW_YORK_TIMEZONE), 
    schedule_interval="0 3 2 * *", # 3am on the 2nd of every month
)
def build_dag():
    """
    Ingest NY Taxi Yellow Cab Data into BigQuery.
    """
    @task(
        pool="github_api"
    )
    def download(data_interval_start: Optional[DateTime] = None) -> str:
        """
        Download & Uncompress Yellow Cab Data CSV.
        Returns the path to the downloaded CSV File.
        """
        # download gzipped data into buffer
        partition = data_interval_start.strftime('%Y-%m') # type: ignore
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

    convert_parquet(download())
