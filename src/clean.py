"""Apply the milestone cleaning rules and count each stage."""

from pyspark.sql import Column, DataFrame, functions as F


def nonnegative_finite(value: Column) -> Column:
    return value.isNotNull() & ~F.isnan(value) & (value >= 0) & (value < float("inf"))


def _stages() -> dict[str, Column]:
    duration = F.timestamp_diff("MICROSECOND", "pickup_at", "dropoff_at")
    valid_timestamps = (
        F.col("pickup_at").isNotNull()
        & F.col("dropoff_at").isNotNull()
        & (duration > 0)
        & (duration <= 24 * 60 * 60 * 1_000_000)
    )
    valid_distance = valid_timestamps & nonnegative_finite(F.col("trip_distance"))
    return {
        "after_timestamps": valid_timestamps,
        "after_distance": valid_distance,
        "after_fare": valid_distance & nonnegative_finite(F.col("fare_amount")),
    }


def clean_trips(trips: DataFrame) -> DataFrame:
    return trips.where(_stages()["after_fare"])


def quality_counts(trips: DataFrame) -> dict[str, int]:
    counts = [F.count("*").alias("input_rows")]
    counts.extend(F.count_if(predicate).alias(stage) for stage, predicate in _stages().items())
    return trips.agg(*counts).first().asDict()
