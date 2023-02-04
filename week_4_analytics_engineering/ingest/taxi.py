#
# Data Engineering Zoomcamp
# Taxi Data pipeline
# Yellow & ForHire Taxi records
#

import os.path as path
import pyarrow as pa
import pyarrow.parquet as pq


# TODO(mrzzy): consolidate logging in @flow
from datetime import date
from enum import Enum
from typing import List
from google.cloud import bigquery
from google.cloud.bigquery.job import LoadJobConfig, SourceFormat, WriteDisposition
from google.cloud.bigquery.table import TableReference

from prefect import flow, get_run_logger, task

from util import load_url_gcs


class TaxiVariant(Enum):
    Yellow = "yellow"
    ForHire = "fhv"


@task
def load_taxi_gcs(bucket: str, variant: TaxiVariant, partition: date) -> str:
    """Load a variant of of the NYC Taxi Dataset into the given GCS Bucket

    Args:
        bucket:
            Name of the GCS bucket to load the partition.
        variant:
            Variant of the the NYC Taxi Dataset to laod.
        date:
            Date specifying the month, year of the partition of the dataset to
            load. Ignores the day of month passed.
    Returns:
        GCS URL of of the uploaded partition of data in within the bucket.
    """
    # upload partition data as blob to GCS
    return load_url_gcs(
        url=(
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/{variant.value}_tripdata_"
            f"{partition:%Y-%m}.parquet"
        ),
        bucket=bucket,
        path=f"nyc_taxi/{variant.value}/{partition:%Y-%m}.pq",
    )


@task
def load_parquet_bq(
    table_id: str, partition_urls: List[str], schema_json: str, truncate: bool = False
):
    """Load the data on the Parquet partitions stored on GCS to a BigQuery table.

    Args:
        table_id:
            ID of the BigQuery table to load data to the format
            <PROJECT_ID>.<DATASET_ID>.<TABLE>
        partition_urls:
            List of GCS URLs referencing Parquet partitions on GCS to load.
        schema_json
            Path to a JSON schema file used to define the schema of the loaded table
        truncate:
            Whether to truncate the table if it exists before writing.
    """
    bq = bigquery.Client()

    # ingest partitions into bigquery
    bq.load_table_from_uri(
        source_uris=partition_urls,
        destination=TableReference.from_string(table_id),
        job_config=LoadJobConfig(
            source_format=SourceFormat.PARQUET,
            schema=bq.schema_from_json(schema_json),
            write_disposition=(
                WriteDisposition.WRITE_APPEND
                if truncate
                else WriteDisposition.WRITE_APPEND
            ),
        ),
    ).result()

    log = get_run_logger()
    log.info(f"Loaded {len(partition_urls)} partitions to {table_id}")


@flow
def ingest_taxi(
    variant: TaxiVariant, bucket: str, table_id: str, partition: date, truncate: bool
):
    """Ingest the variant of the NYC Taxi dataset into the BQ Table with id.

    Stages partition data in a GCS Bucket before ingesting into BigQuery.

    Args:
        variant:
            Variant of thne NYC Taxi dataset to ingest.
        bucket:
            Name of the GCS Bucket used to stage ingested data.
        table_id:
            ID of the BigQuery table to ingesto data to, the format
            <PROJECT_ID>.<DATASET_ID>.<TABLE>
        partition:
            Date of partition to ingest. Since partitions are monthly sized,
            the day of month is disregarded if passed.
        truncate:
            Whether to truncate the table if it exists before writing.
    """
    gs_url = load_taxi_gcs(bucket, variant, partition)
    load_parquet_bq(
        table_id=table_id,
        partition_urls=[gs_url],
        schema_json=path.join(
            path.dirname(__file__), "schemas", f"{variant.value}.json"
        ),
        truncate=truncate,
    )
