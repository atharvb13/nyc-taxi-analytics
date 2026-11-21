"""Shared local Spark configuration."""

import os
import sys

from pyspark.sql import SparkSession


def start_spark(master: str = "local[2]", shuffle_partitions: int = 8) -> SparkSession:
    if master == "local" or master.startswith("local["):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName("NYC Taxi Analytics")
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
