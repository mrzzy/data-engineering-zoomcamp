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

from taxi import TaxiVariant, ingest_fhv_taxi, ingest_yellow_taxi
from util import monthly_range

if __name__ == "__main__":
    for i, partition in enumerate(monthly_range(date(2019, 1, 1), date(2020, 12, 1))):
        ingest_yellow_taxi(
            bucket="mrzzy-data-eng-zoomcamp-nytaxi",
            table_id=f"mrzzy-data-eng-zoomcamp.nytaxi.{TaxiVariant.Yellow.value}",
            partition=partition,
            truncate=i == 0,
        )

    for i, partition in enumerate(monthly_range(date(2019, 1, 1), date(2019, 12, 1))):
        ingest_fhv_taxi(
            bucket="mrzzy-data-eng-zoomcamp-nytaxi",
            table_id=f"mrzzy-data-eng-zoomcamp.nytaxi.{TaxiVariant.ForHire.value}",
            partition=partition,
            truncate=i == 0,
        )
