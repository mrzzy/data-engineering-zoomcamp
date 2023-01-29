#
# Data Engineering Zoomcamp
# Yellow Taxi Data Pipeline
#

import asyncio
import logging
from typing import List
from google.cloud.bigquery.job import LoadJobConfig
from prefect import flow, get_run_logger, task
from datetime import date
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


@task
async def load_taxi_gcs(bucket: str, variant: TaxiVariant, partition: date) -> str:
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
async def load_gcs_bq(table_id: str, partition_urls: List[str]):
    """Load the data on the Parquet partitions stored on GCS to a BigQuery table.
    Truncates the table before loading.

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
async def ingest_taxi(bucket: str, table_id: str, variant: TaxiVariant):
    gs_path = await load_taxi_gcs(bucket, variant, date(2009, 1, 1))
    return await load_gcs_bq(partition_urls=[gs_path], table_id=table_id)


if __name__ == "__main__":
    asyncio.run(
        ingest_taxi(
            bucket="mrzzy-data-eng-zoomcamp-nytaxi",
            table_id=f"mrzzy-data-eng-zoomcamp.nytaxi.{TaxiVariant.Yellow.value}",
            variant=TaxiVariant.Yellow,
        ) # type: ignore
    )  
