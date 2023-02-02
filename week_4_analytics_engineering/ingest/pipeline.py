#
# Data Engineering Zoomcamp
# Yellow Taxi Data Pipeline
#

import logging
from os.path import join, basename
from typing import List, cast
from google.cloud.bigquery.job import LoadJobConfig
from prefect import flow, get_run_logger, task
from datetime import date, timedelta
from enum import Enum
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import (
    SourceFormat,
    TableReference,
    DatasetReference,
    WriteDisposition,
)
from pyarrow import Schema
import requests
import pyarrow as pa
import pyarrow.parquet as pq


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
    # download partition data from NYC's S3
    response = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/{variant.value}_tripdata_"
        f"{partition:%Y-%m}.parquet"
    )
    data = response.content

    # upload partition data as blob to GCS
    gcs = storage.Client()
    blob = gcs.bucket(bucket).blob(f"nyc_taxi/{variant.value}/{partition:%Y-%m}.pq")
    blob.upload_from_string(data)

    log = get_run_logger()
    log.info(f"Loaded {response.url} to {blob.name}")

    return f"gs://{bucket}/{blob.name}"


@task
def load_gcs_bq(table_id: str, partition_urls: List[str]):
    """Load the data on the Parquet partitions stored on GCS to a BigQuery table.
    Appends data to the table before loading.

    Args:
        table_id:
            ID of the BigQuery table to load data to the format
            <PROJECT_ID>.<DATASET_ID>.<TABLE>
        partition_urls:
            List of GCS URLs referencing Parquet partitions on GCS to load.
    """
    bq = bigquery.Client()

    # ingest partitions into bigquery
    bq.load_table_from_uri(
        source_uris=partition_urls,
        destination=TableReference.from_string(table_id),
        job_config=LoadJobConfig(
            source_format=SourceFormat.PARQUET,
            write_disposition=WriteDisposition.WRITE_APPEND,
        ),
    ).result()

    log = get_run_logger()
    log.info(f"Loaded {len(partition_urls)} partitions to {table_id}")

@task
def fix_yellow_taxi_type(gs_url: str) -> str:
    """Fix type inconsistency on Yellow variant NYC Taxi partition.
    
    Args:
        gs_url: URL pointing to the Yellow NYC taxi partition to fix.
    
    Returns:
        URL pointing at the rectified partition.
    """
    yellow = pq.read_table(gs_url) 

    # fix type inconsistency in 'airport_fee' column
    bad_column, schema = "airport_fee", yellow.schema 
    schema.set(
        schema.get_field_index(bad_column),
        schema.field(bad_column).with_type(pa.float32())
    )
    fixed = yellow.cast(schema)

    fixed_gs_url = f"nyc_taxi/{TaxiVariant.Yellow.value}_fixed/{basename(gs_url)}"
    pq.write_table(fixed, fixed_gs_url)
    return fixed_gs_url


@flow
def ingest_yellow_taxi(bucket: str, table_id: str, partition: date):
    """Ingest the Yellow variant of the NYC Taxi dataset into the BQ Table with id.

    Stages partition data in a GCS Bucket before ingesting into BigQuery.

    Args:
        bucket:
            Name of the GCS Bucket used to stage ingested data.
        table_id:
            ID of the BigQuery table to ingesto data to, the format
            <PROJECT_ID>.<DATASET_ID>.<TABLE>
        partition:
            Date of partition to ingest. Since partitions are monthly sized,
            the day of month is disregarded if passed.
    """
    gs_url = load_taxi_gcs(bucket, TaxiVariant.Yellow, partition)
    gs_url = fix_yellow_taxi_type(gs_url)
    load_gcs_bq(partition_urls=[gs_url], table_id=table_id)

@flow
def ingest_fhv_taxi(bucket: str, table_id: str, partition: date):
    """Ingest the ForHire variant of the NYC Taxi dataset into the BQ Table with id.

    Stages partition data in a GCS Bucket before ingesting into BigQuery.

    Args:
        bucket:
            Name of the GCS Bucket used to stage ingested data.
        table_id:
            ID of the BigQuery table to ingesto data to, the format
            <PROJECT_ID>.<DATASET_ID>.<TABLE>
        partition:
            Date of partition to ingest. Since partitions are monthly sized,
            the day of month is disregarded if passed.
    """
    gs_url = load_taxi_gcs(bucket, TaxiVariant.ForHire, partition)
    load_gcs_bq(partition_urls=[gs_url], table_id=table_id)

def monthly_range(begin: date, end: date) -> List[date]:
    """Create a date range with begin & end with dates on monthly interval.
    Args:
        begin:
            Start of the date range.
        end:
            End (inclusive) of the date range.
    Returns:
        List of dates: start of each month between begin & end.
    """
    # discard day of month by resetting begin & end start of month
    begin, end = date(begin.year, begin.month, 1), date(end.year, end.month, 1)
    # +1: inclusive of end month
    n_months = (end.year - begin.year) * 12 + end.month - begin.month + 1
    if n_months < 1:
        raise ValueError(
            "Expected begin & end to delimit a date range of at least 1 month"
        )
    return [
        date(
            year=begin.year + i // 12,
            month=begin.month + i % 12,
            day=1,
        )
        for i in range(n_months)
    ]


if __name__ == "__main__":
    ingest_yellow_taxi(
        bucket="mrzzy-data-eng-zoomcamp-nytaxi",
        table_id=f"mrzzy-data-eng-zoomcamp.nytaxi.{TaxiVariant.Yellow.value}",
        partition=date(2019, 8, 1),
    )
