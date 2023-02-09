#
# Data Engineering Zoomcamp
# NYC Taxi Data Pipeline
# Apply Tables
#

from os import path
from prefect.flows import flow

from taxi import TaxiVariant
from util import apply_bq_table


@flow
def apply_table():
    """Create or Replace BQ tables for ingesting NYC Taxi data."""
    for variant in [TaxiVariant.Yellow, TaxiVariant.Green, TaxiVariant.ForHire]:
        apply_bq_table(
            table_id=f"mrzzy-data-eng-zoomcamp.nytaxi.{variant.value}",
            schema_path=path.join(
                path.dirname(__file__), "schemas", f"{variant.value}.json"
            ),
        )
