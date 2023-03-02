#
# DE Zoomcamp
# Week 5
# Homework Solution
#

from datetime import datetime
import json
import os
from pyspark.sql import DataFrame, SparkSession, Window
from argparse import ArgumentParser

from pyspark.sql.types import StructType
from pyspark.sql import functions as F


def read_schema(json_path: str) -> StructType:
    with open(json_path, "r") as f:
        return StructType.fromJson(json.load(f))


if __name__ == "__main__":
    # parse command line argumetns
    parser = ArgumentParser("Week 5 homework")
    parser.add_argument(
        "fhv_data", help="Path the the FHV 2021-06 Data to analyze.", type=str
    )
    parser.add_argument(
        "fhv_schema", help="Path the the JSON Schema used to read FHV Data.", type=str
    )
    parser.add_argument(
        "zone_lookup", help="Path the the Taxi Zone Lookup table.", type=str
    )
    parser.add_argument(
        "zone_schema",
        help="Path the the JSON Schema used to read Zone Lookup.",
        type=str,
    )
    parser.add_argument(
        "out_dir", help="Path to the directory to write output files.", type=str
    )
    args = parser.parse_args()

    # create local spark development cluster
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("homework")
        # allow non-localhost conections to access spark ui
        .config("spark.driver.bindAddress", "0.0.0.0")
        # increase memory so that we can cache data
        .config("spark.driver.memory", "12g")
        .getOrCreate()
    )
    # Qns 1
    print(f"Q1: {spark.version}")
    #
    # Qns 2
    # df = spark.read.option("header", True).csv(
    #     args.fhv_data, read_schema(args.fhv_schema)
    # )
    # df.repartition(24).write.parquet(args.out_dir, mode="overwrite")
    df = spark.read.parquet("/tmp/out")
    # cache to speed up subsquent queries on trips data
    df.cache()
    print("Q2:")
    os.system(f"du -h {args.out_dir}")

    # Qns 3
    # gonna assume that they meant "June" instead of feb
    print(
        "Q3:",
        df.filter(
            (df.pickup_datetime >= datetime(2021, 6, 15))
            & (df.pickup_datetime < datetime(2021, 6, 16))
        ).count(),
    )

    # Qns 4
    # compute of duration of trip: dropoff_datetime - pickup_datetime
    df = df.withColumn("duration", df.dropoff_datetime - df.pickup_datetime).withColumn(
        "pickup_date", F.date_trunc("day", df.pickup_datetime)
    )
    print("Q4:", df.select(F.max_by(df.pickup_date, df.duration)).first()[0])  # type: ignore

    # Qns 5
    n_trips_by_base = df.groupBy(df.dispatching_base_num).count()
    print(
        "Q5:",
        n_trips_by_base.select(F.max_by("dispatching_base_num", "count")).first()[0],
    )

    # Qns 6
    # find top pickup-dropoff locations
    top_pickup_dropoff = (
        df.groupBy([df.PULocationID, df.DOLocationID])
        .count()
        .sort(F.desc("count"))
        .limit(1)
    )  # type: DataFrame

    zone_df = spark.read.option("header", True).csv(
        args.zone_lookup, read_schema(args.zone_schema)
    )
    # zone_df has to be joined twice, so we cache it here
    zone_df.cache()
    print(
        "Q6: ",
        top_pickup_dropoff.join(
            zone_df, zone_df.LocationID == df.PULocationID, how="left"
        )
        .drop("LocationID")
        .withColumnRenamed("zone", "pickup_zone")
        .join(zone_df, zone_df.LocationID == df.DOLocationID, how="left")
        .drop("LocationID")
        .withColumnRenamed("zone", "dropoff_zone")
        .select(
            F.format_string(
                "%s/%s",
                F.coalesce("pickup_zone", F.lit("Unknown")),
                F.coalesce("dropoff_zone", F.lit("Unknown")),
            )
        )
        .collect()[0][0],
    )
