#
# Data Engineering Zoomcamp
# Taxi Data pipeline
# Zone Lookup Table
#

from typing import List
from google.cloud import bigquery
from google.cloud.bigquery.enums import WriteDisposition
from google.cloud.bigquery.job import LoadJobConfig, SourceFormat
from google.cloud.bigquery.table import TableReference
from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash

from util import load_url_gcs


@task
def load_zone_gcs(bucket: str) -> str:
    """Load the Zone lookup table CSV into the given GCS Bucket.


    Args:
        bucket:
            Name of the GCS bucket to load the Zone CSV.
    Returns:
        GCS URL of the uploaded CSV.
    """
    return load_url_gcs(
        url="https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv",
        bucket=bucket,
        path="nyc_taxi/zone.csv",
    )


@task
def load_csv_bq(table_id: str, csv_urls: List[str]):
    """Load the data on the CSV partitions stored on GCS to a BigQuery table.

    Args:
        table_id:
            ID of the BigQuery table to load data to the format
            <PROJECT_ID>.<DATASET_ID>.<TABLE>
        csv_urls:
            List of GCS URLs referencing CSV partitions on GCS to load.
        truncate:
            Whether to truncate the table if it exists before writing.
    """

    bq = bigquery.Client()

    # ingest partitions into bigquery
    bq.load_table_from_uri(
        source_uris=csv_urls,
        destination=TableReference.from_string(table_id),
        job_config=LoadJobConfig(
            source_format=SourceFormat.CSV,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            # first row is header, autodetect schema
            skip_leading_rows=1,
            autodetect=True,
        ),
    ).result()


@flow
def ingest_zone(bucket: str, table_id: str):
    """Ingest the Zone Lookup table of the NYC Taxi dataset into the BQ table with id.

    Stages CSV data in a GCS Bucket before ingesting into BigQuery.

    Args:
        bucket:
            Name of the GCS Bucket used to stage ingested data.
        table_id:
            ID of the BigQuery table to ingesto data to, the format
            <PROJECT_ID>.<DATASET_ID>.<TABLE>
    """
    log = get_run_logger()
    gs_url = load_zone_gcs(bucket)
    log.info(f"Load Zone CSV to GCS")
    load_csv_bq(table_id, csv_urls=[gs_url])
    log.info(f"Ingested Zone CSV into BigQuery")
