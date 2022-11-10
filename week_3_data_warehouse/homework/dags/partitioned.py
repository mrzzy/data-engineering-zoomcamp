#
# data-engineering-zoomcamp
# Week 3 - Homework
# Ingest Partitioned Taxi Data to GCP
#

from enum import Enum
import gzip
from typing import Optional

from airflow.decorators import dag, task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud.bigquery import TableReference

from pendulum import datetime
from pendulum.datetime import DateTime
from pendulum.tz.timezone import UTC

from common import (
    BQ_DATASET,
    BUCKET,
    GCP_PROJECT,
    GITHUB_DATASET_URL_PREFIX,
    build_load_bq_job_config,
    convert_parquet,
    download_file,
    upload_gcs,
)


class PartitionedDatasetType(Enum):
    Yellow = "yellow"
    ForHire = "fhv"

    def __str__(self) -> str:
        return self.value


PARTITION_KEY = {
    PartitionedDatasetType.Yellow.value: "tpep_pickup_datetime",
    PartitionedDatasetType.ForHire.value: "pickup_datetime",
}


def build_dag(
    dataset_type: str,
    github_url_prefix: str = GITHUB_DATASET_URL_PREFIX,
    gcp_project_id: str = GCP_PROJECT,
    bucket: str = BUCKET,
    bq_dataset: str = BQ_DATASET,
) -> DAG:
    """
    Build an Airflow DAG to ingest the partitioned NYTaxi Dataset into BigQuery.

    Args:
        dataset_type:
            Variant of the NYTaxi to ingest, only supports partitioned variants.
        github_url_prefix:
            URL prefix used to build URLs to retrieve dataset files from Github.
        gcp_project_id:
            ID specifying the GCP project the GCS Bucket & BigQuery dataset reside in.
        bucket:
            Name of then GCS bucket to use as staging area for BigQuery ingestion.
        bq_dataset
            Name of the BigQuery dataset to ingest to.
    """

    @dag(
        dag_id=f"ingest-nyc-tlc-{dataset_type}",
        start_date=datetime(2019, 1, 2, tz=UTC),
        end_date=datetime(2021, 8, 2, tz=UTC),
        schedule_interval="0 3 2 * *",  # 3am on the 2nd of every month
        catchup=True,
        params={
            "retries": 3,
            "retry_delay": 60.0,
            "retry_exponential_backoff": True,
        },
    )
    def build():
        f"""
        Ingest NY Taxi Data ({dataset_type}) into BigQuery.

        ## Prerequisites
        Expects a Google Cloud Platform connection configured under the id:
        `google_cloud_default` with GCS & BigQuery IAM permissions.

        Within the GCP project, expects the following infrastructure to be deployed:
        - GCS Bucket: `{bucket}`
        - BigQuery Dataset: `{bq_dataset}`
        """

        @task
        def download(
            dataset_type: str,
            data_interval_start: Optional[DateTime] = None,
        ) -> str:
            """
            Download & Uncompress Data CSV.
            Returns the path to the downloaded CSV File.
            """
            # download data from github
            partition = data_interval_start.strftime("%Y-%m")  # type: ignore
            csv_path = f"{dataset_type}_tripdata_{partition}.csv"
            download_file(
                url=f"{github_url_prefix}/{dataset_type}/{dataset_type}_tripdata_{partition}.csv.gz",
                path=csv_path,
                unpack=gzip.decompress,
            )
            return csv_path

        # define dag
        csv_path = download(dataset_type)
        pq_path = convert_parquet(csv_path)
        gcs_path = upload_gcs(
            pq_path, prefix=f"nyc_tlc/{dataset_type}/raw", bucket=bucket
        )
        # load data in GCS as a partitioned table in BigQuery.
        load_table = BigQueryInsertJobOperator(
            task_id="load_bq_table",
            configuration=build_load_bq_job_config(
                source_urls=[f"gs://{bucket}/nyc_tlc/{dataset_type}/raw/*.pq"],
                dest_table=TableReference.from_string(
                    f"{gcp_project_id}.{bq_dataset}.{dataset_type}"
                ),
                partition_by=PARTITION_KEY[dataset_type],
            ),
        )
        gcs_path >> load_table  # type: ignore

    return build()


yellow_dag = build_dag(PartitionedDatasetType.Yellow.value)
for_hire_dag = build_dag(PartitionedDatasetType.ForHire.value)
