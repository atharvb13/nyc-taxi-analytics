"""Shared local Spark configuration."""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def start_spark(
    master: str = "local[2]", shuffle_partitions: int = 8, event_dir: Path | None = None,
) -> SparkSession:
    if master == "local" or master.startswith("local["):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName("NYC Taxi Analytics")
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if event_dir:
        builder.config("spark.eventLog.enabled", "true")
        builder.config("spark.eventLog.dir", event_dir.as_uri())
        builder.config("spark.eventLog.rolling.enabled", "false")
        builder.config("spark.eventLog.compress", "false")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
