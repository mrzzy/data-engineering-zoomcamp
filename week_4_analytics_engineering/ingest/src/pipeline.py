#
# Data Engineering Zoomcamp
# NYC Taxi Data Pipeline
#

import asyncio
import logging
from os import path
from os.path import join, basename
from typing import List, cast
from datetime import date, datetime, timedelta
from google.cloud.bigquery import (
    SourceFormat,
    TableReference,
    DatasetReference,
    WriteDisposition,
)
from prefect.orion.schemas import responses
from prefect.deployments import run_deployment
from pyarrow import Schema

from taxi import TaxiVariant, ingest_taxi
from util import apply_bq_table, monthly_range
from zone import ingest_zone


async def main():
    # create or replace BQ tables
    await run_deployment("apply-table/apply_table")

    gcs_bucket = "mrzzy-data-eng-zoomcamp-nytaxi"
    run_futures = []
    # ingest yellow taxi records from 2019 jan to 2020 dec
    for i, partition in enumerate(monthly_range(date(2019, 1, 1), date(2020, 12, 1))):
        run_futures.append(
            run_deployment(
                "ingest-taxi/ingest_taxi",
                parameters={
                    "variant": TaxiVariant.Yellow,
                    "bucket": gcs_bucket,
                    "table_id": f"mrzzy-data-eng-zoomcamp.nytaxi.{TaxiVariant.Yellow.value}",
                    "partition": datetime.combine(partition, datetime.min.time()),
                },
            )
        )

    # ingest green taxi records from 2019 jan to 2020 dec
    for i, partition in enumerate(monthly_range(date(2019, 1, 1), date(2020, 12, 1))):
        run_futures.append(
            run_deployment(
                "ingest-taxi/ingest_taxi",
                parameters={
                    "variant": TaxiVariant.Green,
                    "bucket": gcs_bucket,
                    "table_id": f"mrzzy-data-eng-zoomcamp.nytaxi.{TaxiVariant.Green.value}",
                    "partition": datetime.combine(partition, datetime.min.time()),
                },
            )
        )

    # ingest for hire taxi records from 2019 jan to 2019 dec
    for i, partition in enumerate(monthly_range(date(2019, 1, 1), date(2019, 12, 1))):
        run_futures.append(
            run_deployment(
                "ingest-taxi/ingest_taxi",
                parameters={
                    "variant": TaxiVariant.ForHire,
                    "bucket": gcs_bucket,
                    "table_id": f"mrzzy-data-eng-zoomcamp.nytaxi.{TaxiVariant.ForHire.value}",
                    "partition": datetime.combine(partition, datetime.min.time()),
                },
            )
        )

    # ingest taxi zone lookup table
    run_futures.append(
        run_deployment(
            "ingest-zone/ingest_zone",
            parameters={
                "bucket": gcs_bucket,
                "table_id": f"mrzzy-data-eng-zoomcamp.nytaxi.zone",
            },
        )
    )
    await asyncio.gather(*run_futures)  # type: ignore


if __name__ == "__main__":
    asyncio.run(main())
