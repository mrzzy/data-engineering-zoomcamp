#
# data-engineering-zoomcamp
# Week 1 - Homework
# Ingest Pipeline
#

import os
import pandas as pd
from pathlib import Path
from argparse import ArgumentParser
from sqlalchemy import create_engine

if __name__ == "__main__":
    # parse pipeline arguments
    parser = ArgumentParser(
        description="Data Pipeline to ingest NYTaxi dataset into a Postgres DB",
        usage="""Expects the Postgres DB password to be passed via
the POSTGRES_PASSWORD environment variable.
""",
    )
    parser.add_argument_group("Input Files")
    parser.add_argument(
        "cab_csv", type=Path, help="Path to the Yellow Cab Taxi CSV file to import."
    )
    parser.add_argument(
        "zone_csv", type=Path, help="Path to the Taxi Zone CSV file to import."
    )
    parser.add_argument(
        "db_host", type=str, help="Hostname, port of Postgres DB to connect to"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100000,
        help="Size of each batch of Yellow Cab Taxi data imported into the Postgres DB",
    )
    parser.add_argument(
        "--db-schema",
        type=str,
        default="ny_taxi",
        help="Name of the Postgres DB schema to write tables to.",
    )
    parser.add_argument(
        "--db-user", type=str, default="postgres",
        help="Username of the Postgres DB user used to log into the DB."
    )
    parser.parse_args()
    args = parser.parse_args()

    # connect to the Postgres DB
    db_password = os.environ["POSTGRES_PASSWORD"]
    db = create_engine(
        f"postgres+psycopg2://{args.db_user}:{db_password}@{args.db_host}/{args.db_schema}"
    )

    # import taxi zone lookup data into Postgres
    zone_df = pd.read_csv(args.zone_csv)
