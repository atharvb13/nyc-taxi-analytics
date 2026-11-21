"""Read monthly TLC Parquet files and normalize the analysis columns."""

from collections.abc import Sequence
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import NumericType, TimestampNTZType, TimestampType

TLC_COLUMNS = {
    "tpep_pickup_datetime": ("pickup_at", "timestamp_ntz"),
    "tpep_dropoff_datetime": ("dropoff_at", "timestamp_ntz"),
    "PULocationID": ("pickup_zone_id", "int"),
    "DOLocationID": ("dropoff_zone_id", "int"),
    "trip_distance": ("trip_distance", "double"),
    "fare_amount": ("fare_amount", "double"),
    "passenger_count": ("passenger_count", "double"),
}


def normalize_trips(raw: DataFrame, source: str) -> DataFrame:
    columns = {field.name: field.dataType for field in raw.schema}
    missing = sorted(TLC_COLUMNS.keys() - columns.keys())
    if missing:
        raise ValueError(f"{source}: missing required columns: {', '.join(missing)}")

    selected = []
    for name, (alias, kind) in TLC_COLUMNS.items():
        supported = (TimestampType, TimestampNTZType) if kind == "timestamp_ntz" else (NumericType,)
        if not isinstance(columns[name], supported):
            raise ValueError(f"{source}: unsupported type for {name}: {columns[name].simpleString()}")
        selected.append(F.col(name).try_cast(kind).alias(alias))
    return raw.select(*selected)


def read_trips(spark: SparkSession, paths: Sequence[str]) -> DataFrame:
    if not paths:
        raise ValueError("Provide at least one TLC Parquet file")
    # Cast each month's physical schema before unioning. For example, passenger
    # count is stored as an integer in some Parquet schemas and a double in others.
    normalized = [normalize_trips(spark.read.parquet(path), path) for path in paths]
    return reduce(DataFrame.unionByName, normalized)
