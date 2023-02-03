#
# Data Engineering Zoomcamp
# Taxi Data pipeline
# Yellow & ForHire Taxi records
#

import pyarrow as pa
import pyarrow.parquet as pq


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
def load_gcs_bq(table_id: str, partition_urls: List[str], truncate: bool = False):
    """Load the data on the Parquet partitions stored on GCS to a BigQuery table.

    Args:
        table_id:
            ID of the BigQuery table to load data to the format
            <PROJECT_ID>.<DATASET_ID>.<TABLE>
        partition_urls:
            List of GCS URLs referencing Parquet partitions on GCS to load.
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
            write_disposition=(
                WriteDisposition.WRITE_APPEND
                if truncate
                else WriteDisposition.WRITE_APPEND
            ),
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
    schema = schema.set(
        schema.get_field_index(bad_column),
        schema.field(bad_column).with_type(pa.float32()),
    )
    fixed = yellow.cast(schema)

    fixed_gs_url = gs_url.replace(
        f"/{TaxiVariant.Yellow.value}/", f"/{TaxiVariant.Yellow.value}_fixed/"
    )
    pq.write_table(fixed, fixed_gs_url)
    return fixed_gs_url


@flow
def ingest_yellow_taxi(bucket: str, table_id: str, partition: date, truncate: bool):
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
        truncate:
            Whether to truncate the table if it exists before writing.
    """
    gs_url = load_taxi_gcs(bucket, TaxiVariant.Yellow, partition)
    gs_url = fix_yellow_taxi_type(gs_url)
    load_gcs_bq(partition_urls=[gs_url], table_id=table_id, truncate=truncate)


@flow
def ingest_fhv_taxi(bucket: str, table_id: str, partition: date, truncate: bool):
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
        truncate:
            Whether to truncate the table if it exists before writing.
    """
    gs_url = load_taxi_gcs(bucket, TaxiVariant.ForHire, partition)
    load_gcs_bq(partition_urls=[gs_url], table_id=table_id, truncate=truncate)
