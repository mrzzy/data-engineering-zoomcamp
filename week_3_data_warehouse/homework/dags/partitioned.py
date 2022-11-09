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

from pendulum import datetime
from pendulum.datetime import DateTime
from pendulum.tz.timezone import UTC

from common import (
    BQ_DATASET,
    BUCKET,
    GITHUB_DATASET_URL_PREFIX,
    convert_parquet,
    download_file,
    register_external_bq_table,
    upload_gcs,
)


class PartitionedDatasetType(Enum):
    Yellow = "yellow"
    ForHire = "fhv"

PARTITION_KEY = {
    PartitionedDatasetType.Yellow.value: "tpep_pickup_datetime",
    PartitionedDatasetType.ForHire.value: "pickup_datetime",
}

def build_dag(
    dataset_type: PartitionedDatasetType,
    bucket: str = BUCKET,
    bq_dataset: str = BQ_DATASET,
) -> DAG:
    """
    Build an Airflow DAG to ingest the partitioned NYTaxi Dataset into BigQuery.

    Args:
        dataset_type:
            Variant of the NYTaxi to ingest, only supports partitioned variants.
        bucket:
            Name of then GCS bucket to use as staging area for BigQuery ingestion.
        bq_dataset
            Name of the BigQuery dataset to ingest to.
    """
    @dag(
        dag_id=f"ingest-nyc-tlc-{dataset_type.value}",
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
            dataset_type: PartitionedDatasetType,
            data_interval_start: Optional[DateTime] = None,
        ) -> str:
            """
            Download & Uncompress Data CSV.
            Returns the path to the downloaded CSV File.
            """
            # download data from github
            partition = data_interval_start.strftime("%Y-%m")  # type: ignore
            csv_path = f"{dataset_type.value}_tripdata_{partition}.csv"
            download_file(
                url=f"{GITHUB_DATASET_URL_PREFIX}/{dataset_type.value}/{dataset_type.value}_tripdata_{partition}.csv.gz",
                path=csv_path,
                unpack=gzip.decompress
            )
            return csv_path



        # define dag
        csv_path = download(dataset_type)
        pq_path = convert_parquet(csv_path)
        gs_path = upload_gcs(
            pq_path,
            prefix=f"nyc_tlc/{dataset_type.value}/raw",
            bucket=bucket
        )
        register_table = register_external_bq_table(
            gs_path,
            bq_dataset,
            table_name=dataset_type.value + "_{{ data_interval_start.strftime('%Y_%m') }}",
        )
        # rewrite table name partitioned external tables into a bigquery native partitioned table.
        partition_table = BigQueryInsertJobOperator(
            task_id="partition_bq_table",
            params={"table_id": f"{bq_dataset}.{dataset_type.value}"},
            configuration={
                "jobType": "QUERY",
                "query": {
                    "query":
                    "CREATE OR REPLACE TABLE {{ params.table_id }} "
                    "PARTITION BY DATE(CONCAT(REPLACE(_TABLE_SUFFIX, '_', '-'), '-01')) "
                    # wildcard select from all date paritioned external table names
                    "AS SELECT * FROM `{{ params.table_id }}*`",
                    "useLegacySql": False,
                }
            }
        )
        register_table >> partition_table # type: ignore
    return build()


yellow_dag = build_dag(PartitionedDatasetType.Yellow)
for_hire_dag = build_dag(PartitionedDatasetType.ForHire)
