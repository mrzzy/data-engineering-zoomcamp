#
# Data Engineering Zoomcamp
# Taxi Data pipeline
# Yellow & ForHire Taxi records
#

import os.path as path
from prefect.tasks import task_input_hash
import pyarrow as pa
import pyarrow.parquet as pq


# TODO(mrzzy): consolidate logging in @flow
from datetime import date
from enum import Enum
from typing import Any, Dict, List
from google.cloud import bigquery
from google.cloud.bigquery.job import LoadJobConfig, SourceFormat, WriteDisposition
from google.cloud.bigquery.table import TableReference

from prefect import flow, get_run_logger, task

from util import load_url_gcs


class TaxiVariant(Enum):
    Yellow = "yellow"
    Green = "green"
    ForHire = "fhv"


@task(cache_key_fn=task_input_hash)
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


@task(cache_key_fn=task_input_hash)
def fix_taxi_type(gs_url: str, to_fix: Dict[str, Any]):
    """Fix type inconsistency on NYC Taxi partition.

    Args:
        gs_url: URL pointing to the NYC taxi partition to fix.
        to_fix: Map of column to PyArrow type to cast to.
    Returns:
        URL pointing at the rectified partition.
    """
    partition = pq.read_table(gs_url)
    # fix type inconsistencies
    for bad_column, cast_type in to_fix.items():
        schema = partition.schema
        schema = schema.set(
            schema.get_field_index(bad_column),
            schema.field(bad_column).with_type(cast_type),
        )
        partition = partition.cast(schema)

    # write fixed partition back to GCS
    fixed_gs_url = gs_url.replace(
        f"/{TaxiVariant.Yellow.value}/", f"/{TaxiVariant.Yellow.value}_fixed/"
    )
    pq.write_table(partition, fixed_gs_url)
    return fixed_gs_url


@task(cache_key_fn=task_input_hash)
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

    # fix type inconsistencies
    if variant == TaxiVariant.Yellow:
        to_fix = {"airport_fee": pa.float32()}
    elif variant == TaxiVariant.ForHire:
        to_fix = {
            "SR_Flag": pa.int8(),
            "PUlocationID": pa.int64(),
            "DOlocationID": pa.int64(),
        }
    elif variant == TaxiVariant.Green:
        to_fix = {"ehail_fee": pa.float64()}
    else:
        raise ValueError(f"Unsupported Taxi Variant: {variant.value}")
    gs_url = fix_taxi_type(gs_url, to_fix)

    load_parquet_bq(
        table_id=table_id,
        partition_urls=[gs_url],
        schema_json=path.join(
            path.dirname(__file__), "schemas", f"{variant.value}.json"
        ),
        truncate=truncate,
    )
