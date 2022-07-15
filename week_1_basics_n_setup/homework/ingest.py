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
    """
    Count the number of the lines in the text file of the given path.
    """
    buf_size = 1024 * 1024 # 1MB
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
        "--batch-size",
        type=int,
        default=100000,
        help="Size of each batch of Yellow Cab Taxi data imported into the Postgres DB",
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
    for i in range(5):
        try:
            log.info(f"Connecting to DB: Retry {i+1}")
            db.connect()
            break
        except OperationalError as e:
            log.error(e)
        sleep(1)
    else:
        raise RuntimeError("Failed to connect to database despite retries")

    # read data from csv & set data types
    zone_table, zone_df = "PickupZones", cast(pd.DataFrame, pd.read_csv(args.zone_csv))
    trip_table, trip_df = "TaxiTrips", pd.read_csv(
        args.cab_csv,
        converters={
            "tpep_pickup_datetime": pd.to_datetime,
            "tpep_dropoff_datetime": pd.to_datetime,
        },
        dtype={
            "VendorID": "int",
            "passenger_count": "int",
            "trip_distance": "float",
            "RatecodeID": "int",
            "PULocationID": "int",
            "DOLocationID": "int",
            "payment_type": "int",
            "fare_amount": "float",
            "extra": "float",
            "mta_tax": "float",
            "tip_amount": "float",
            "tolls_amount": "float",
            "improvement_surcharge": "float",
            "total_amount": "float",
            "congestion_surcharge": "float",
        },
        iterator=True,
        chunksize=args.batch_size,
    )

    # apply table schema only to into postgres don't write any rows
    with db.begin():
        zone_df.head(0).to_sql(zone_table, db, if_exists="replace")
        trip_df.get_chunk(0).head(0).to_sql(trip_table, db, if_exists="replace")
    log.info("Applied table schema to DB.")

    # Ingest Data
    # ingest taxi zone lookup table oneshot
    zone_df.to_sql(zone_table, db, if_exists="append")
    log.info("Ingested Taxi Zone Lookup table to DB.")

    # count taxi trip records needed to be ingested
    n_trips = count_lines(args.cab_csv)
    n_batches = n_trips // args.batch_size + 1

    # ingest taxi trip data in chunks
    for i, chunk_df in enumerate(trip_df):
        begin = time()
        chunk_df.to_sql(trip_table, db, if_exists="append")
        elapsed = timedelta(seconds=time() - begin)
        log.info(f"Ingested Taxi Trips Chunk {i}/{n_batches}: took {elapsed} ...")
    log.info("Ingested Taxi Trips Data table to DB.")
