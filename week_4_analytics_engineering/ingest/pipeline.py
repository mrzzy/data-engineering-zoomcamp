#
# Data Engineering Zoomcamp
# NYC Taxi Data Pipeline
#

import logging
from os.path import join, basename
from typing import List, cast
from datetime import date, timedelta
from google.cloud.bigquery import (
    SourceFormat,
    TableReference,
    DatasetReference,
    WriteDisposition,
)
from prefect.orion.schemas import responses
from pyarrow import Schema

from taxi import TaxiVariant, ingest_taxi
from util import monthly_range
from zone import ingest_zone

if __name__ == "__main__":
    gcs_bucket = "mrzzy-data-eng-zoomcamp-nytaxi"
    # ingest green taxi records from 2019 jan to 2019 dec
    for i, partition in enumerate(monthly_range(date(2019, 9, 1), date(2019, 12, 1))):
        ingest_taxi(
            variant=TaxiVariant.Green,
            bucket=gcs_bucket,
            table_id=f"mrzzy-data-eng-zoomcamp.nytaxi.{TaxiVariant.Green.value}",
            partition=partition,
            truncate=i == 0,
        )
