"""Run the Spark smoke check or analyze supplied TLC Parquet files."""

import argparse
import json

from pyspark.sql import functions as F

from src.clean import clean_trips, quality_counts
from src.ingest import read_trips
from src.queries import QUERIES, register_views
from src.runtime import start_spark


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="*", help="TLC Yellow Taxi Parquet files")
    parser.add_argument("--smoke", action="store_true", help="check that all four Spark tasks ran")
    parser.add_argument("--query", choices=QUERIES, default="hourly_pickups")
    parser.add_argument("--zones", help="optional TLC zone lookup CSV")
    args = parser.parse_args()
    if not args.smoke and not args.inputs:
        parser.error("provide Parquet files or --smoke")

    spark = start_spark()
    try:
        if args.smoke:
            partitions = (
                spark.range(1000, numPartitions=4)
                .groupBy(F.spark_partition_id().alias("partition"))
                .agg(F.count("*").alias("rows"), F.sum("id").alias("total"))
                .collect()
            )
            if len(partitions) != 4 or sum(row.rows for row in partitions) != 1000 or (
                sum(row.total for row in partitions) != 499500
            ):
                raise RuntimeError(f"Spark smoke check returned unexpected results: {partitions}")
            print(f"Spark {spark.version} on {spark.sparkContext.master}: 4 tasks, 1,000 rows, sum 499,500")
            return
        raw = read_trips(spark, args.inputs)
        print(json.dumps(quality_counts(raw), indent=2))
        register_views(spark, clean_trips(raw), args.zones)
        spark.sql(QUERIES[args.query]).show(200, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
