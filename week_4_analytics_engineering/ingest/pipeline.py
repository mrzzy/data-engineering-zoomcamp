#
# Data Engineering Zoomcamp
# Yellow Taxi Data Pipeline
#

import logging
from typing import List
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
import requests


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
            Date specifying the month, year of the parition of the dataset to
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
    Truncates the table before loading.

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
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
        ),
    ).result()

    log = get_run_logger()
    log.info(f"Loaded {len(partition_urls)} partitions to {table_id}")


@flow
def ingest_taxi(
    bucket: str, table_id: str, variant: TaxiVariant, begin: date, end: date
):
    """Ingest the given variant of NYC Taxi dataset into a BQ Table.

    Uses GCS Bucket as a staging area before ingesting into BigQuery.
    Ingest data in month-sized partitions.

    Day of month passed to begin or end arguments are ignored.

    Args:
        bucket:
            Name of the GCS Bucket used to stage ingeted data..
        table_id:
            ID of the BigQuery table to ingesto data to, the format
            <PROJECT_ID>.<DATASET_ID>.<TABLE>
        variant:
            Variant of the taxi dataset to ingest.
        begin:
            First month of the date range of data to ingest.
        end:
            Last month of the date range of data to ingest.
    """
    # discard day of month by resetting begin & end start of month
    begin, end = date(begin.year, begin.month, 1), date(end.year, end.month, 1)
    n_months = (end.year - begin.year) * 12 + end.month - begin.month
    if n_months < 1:
        raise ValueError(
            "Expected begin & end to delimit a date range of at least 1 month"
        )

    # download partitions in date range.
    gs_paths = [
        load_taxi_gcs(
            bucket,
            variant,
            # calculate parition date i months from begin date
            date(
                year=begin.year + i // 12,
                month=begin.month + i % 12,
                day=1,
            ),
        )
        for i in range(n_months)
    ]
    # ingest partitions to bigquery
    load_gcs_bq(partition_urls=gs_paths, table_id=table_id)

if __name__ == "__main__":
    ingest_taxi(
        bucket="mrzzy-data-eng-zoomcamp-nytaxi",
        table_id=f"mrzzy-data-eng-zoomcamp.nytaxi.{TaxiVariant.Yellow.value}",
        variant=TaxiVariant.Yellow,
        begin=date(2019, 1, 1),
        end=date(2020, 12, 1),
    )
    ingest_taxi(
        bucket="mrzzy-data-eng-zoomcamp-nytaxi",
        table_id=f"mrzzy-data-eng-zoomcamp.nytaxi.{TaxiVariant.ForHire.value}",
        variant=TaxiVariant.ForHire,
        begin=date(2019, 1, 1),
        end=date(2019, 12, 1),
    )
