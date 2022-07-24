#
# data-engineering-zoomcamp
# Week 1 - Homework
# Ingest Pipeline
#

import logging as log
import os
from argparse import ArgumentParser
from datetime import timedelta
from pathlib import Path
from time import sleep, time
from typing import cast

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


def count_lines(path: str) -> int:
    """Count the number of the lines in the text file of the given path."""
    buf_size = 1024 * 1024  # 1MB
    n_lines = 0
    with open(path, "rb", buffering=0) as f:
        buf = b"non empty"
        while len(buf) > 0:
            buf = f.read(buf_size)
            n_lines += buf.count(b"\n")
    return n_lines


if __name__ == "__main__":
    log.basicConfig(level=log.DEBUG)
    # parse pipeline arguments
    parser = ArgumentParser(
        description="Data Pipeline to ingest NYTaxi dataset into a Postgres DB",
        usage="""Expects the Postgres DB password to be passed via
the POSTGRES_PASSWORD environment variable.
""",
    )
    parser.add_argument_group("Input Files")
    parser.add_argument(
        "cab_csv",
        type=Path,
        help="Path to the Yellow Cab Taxi Trips CSV file to import.",
    )
    parser.add_argument(
        "zone_csv", type=Path, help="Path to the Taxi Zone CSV file to import."
    )
    parser.add_argument(
        "db_host", type=str, help="Hostname, port of Postgres DB to connect to"
    )
    parser.add_argument(
        "--db-database",
        type=str,
        default="NYTaxi",
        help="Name of the Postgres DB database to write tables to.",
    )
    parser.add_argument(
        "--db-user",
        type=str,
        default="postgres",
        help="Username of the Postgres DB user used to log into the DB.",
    )
    parser.parse_args()
    args = parser.parse_args()

    # connect to the Postgres DB with retries
    db_password = os.environ["POSTGRES_PASSWORD"]
    db = create_engine(
        f"postgresql+psycopg2://{args.db_user}:{db_password}@{args.db_host}/{args.db_database}"
    )
    for i in range(10):
        try:
            log.info(f"Connecting to DB: Retry {i+1}")
            db.connect()
            break
        except OperationalError as e:
            log.error(e)
        sleep(1)
    else:
        raise RuntimeError("Failed to connect to database despite retries")

    # read dataframes from csv & set data types
    zone_table, zone_df = "PickupZones", cast(pd.DataFrame, pd.read_csv(args.zone_csv))
    # as TaxiTrips is a large dataset, we will only use pandas to create a table
    # to store the data into the database. Hence no rows are actually read.
    trip_table, trip_df = "TaxiTrips", cast(
        pd.DataFrame,
        pd.read_csv(
            args.cab_csv,
            converters={
                "tpep_pickup_datetime": pd.to_datetime,
                "tpep_dropoff_datetime": pd.to_datetime,
            },
            dtype={
                "VendorID": "Int64",
                "passenger_count": "Int64",
                "trip_distance": "float",
                "RatecodeID": "Int64",
                "PULocationID": "Int64",
                "DOLocationID": "Int64",
                "payment_type": "Int64",
                "fare_amount": "float",
                "extra": "float",
                "mta_tax": "float",
                "tip_amount": "float",
                "tolls_amount": "float",
                "improvement_surcharge": "float",
                "total_amount": "float",
                "congestion_surcharge": "float",
            },
            nrows=0,
        ),
    )

    # apply table schema only to into postgres don't write any rows
    with db.begin():
        zone_df.head(0).to_sql(zone_table, db, if_exists="replace")
        trip_df.head(0).to_sql(trip_table, db, if_exists="replace")
    log.info("Applied table schema to DB.")

    # Ingest Data
    # ingest taxi zone lookup table oneshot
    zone_df.to_sql(zone_table, db, if_exists="append")
    log.info("Ingested Taxi Zone Lookup table to DB.")

    # ingest taxi trip data using Postgres's COPY FROM statement
    log.debug("Ingesting Taxi Trips Data table to DB...")
    begin = time()
    with open(args.cab_csv, "r") as cab_csv:
        # skip first header line from ingestion
        next(cab_csv)

        conn = db.raw_connection()
        cursor = conn.cursor()
        cursor.copy_from(  # type: ignore
            cab_csv, trip_table, sep=",", columns=trip_df.columns, null=""
        )
        conn.commit()

        # clean up reosurces
        cursor.close()
        conn.close()
    elapsed = timedelta(seconds=time() - begin)
    log.info(f"Ingested Taxi Trips Data table to DB: took {elapsed}")
