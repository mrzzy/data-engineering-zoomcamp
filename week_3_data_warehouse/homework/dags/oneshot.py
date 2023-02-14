#
# data-engineering-zoomcamp
# Week 3 - Homework
# Ingest Oneshot Taxi Data to GCP
#

from airflow.models.dag import DAG
from airflow.decorators import dag, task
from pendulum import datetime
from pendulum.tz.timezone import UTC

from common import (
    BQ_DATASET,
    BUCKET, 
    GITHUB_DATASET_URL_PREFIX, 
    convert_parquet,
    download_file,
    upload_gcs,
    register_external_bq_table
)

def build_dag(
    bucket: str = BUCKET,
    bq_dataset: str = BQ_DATASET,
) -> DAG:
    """
    Build an oneshot Airflow DAG to ingest the NYTaxi zone lookup table into BigQuery.

    Args:
        dataset_type:
            Variant of the NYTaxi to ingest, only supports oneshot ingest variants.
        bucket:
            Name of then GCS bucket to use as staging area for BigQuery ingestion.
        bq_dataset
            Name of the BigQuery dataset to ingest to.
    """
    @dag(
        dag_id=f"ingest-nyc-tlc-zone",
        start_date=datetime(2019, 1, 2, tz=UTC),
        end_date=datetime(2019, 1, 2, tz=UTC),
        params={
            "retries": 3,
            "retry_delay": 60.0,
            "retry_exponential_backoff": True,
        },
    )
    def build():
        f"""
        Ingest NY Taxi Data (Zone lookup) into BigQuery.

        ## Prerequisites
        Expects a Google Cloud Platform connection configured under the id:
        `google_cloud_default` with GCS & BigQuery IAM permissions.

        Within the GCP project, expects the following infrastructure to be deployed:
        - GCS Bucket: `{bucket}`
        - BigQuery Dataset: `{bq_dataset}`
        """

        @task
        def download() -> str:
            """
            Download & Uncompress Data CSV.
            Returns the path to the downloaded CSV File.
            """
            # download data from github
            csv_path = f"taxi_zone_lookup.csv"
            download_file(f"{GITHUB_DATASET_URL_PREFIX}/misc/taxi_zone_lookup.csv", csv_path)
            return csv_path

        # define dag
        csv_path = download()
        pq_path = convert_parquet(csv_path)
        gs_path = upload_gcs(
            pq_path,
            prefix=f"nyc_tlc/zone/raw",
            bucket=bucket
        )
        register_external_bq_table(
            gs_path,
            bq_dataset,
            table_name="zone"
        )
    return build()

zone_dag = build_dag()
