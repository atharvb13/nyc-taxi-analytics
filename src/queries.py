"""Bounded Spark SQL workloads over the cleaned trip view."""

from pyspark.sql import DataFrame, SparkSession, functions as F

QUERIES = {
    "hourly_pickups": """
        SELECT dayofweek(pickup_at) AS day_of_week,
               hour(pickup_at) AS pickup_hour,
               count(*) AS trips
        FROM trips
        GROUP BY dayofweek(pickup_at), hour(pickup_at)
        ORDER BY day_of_week, pickup_hour
    """,
    "top_zones": """
        WITH zone_counts AS (
            SELECT 'pickup' AS direction, pickup_zone_id AS zone_id, count(*) AS trips
            FROM trips
            GROUP BY pickup_zone_id
            UNION ALL
            SELECT 'dropoff' AS direction, dropoff_zone_id AS zone_id, count(*) AS trips
            FROM trips
            GROUP BY dropoff_zone_id
        ),
        ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY direction ORDER BY trips DESC, zone_id ASC NULLS LAST
            ) AS zone_rank
            FROM zone_counts
        )
        SELECT ranked.direction, ranked.zone_id, zones.zone_name, ranked.trips
        FROM ranked
        LEFT JOIN zones USING (zone_id)
        WHERE ranked.zone_rank <= 10
        ORDER BY ranked.direction, ranked.zone_rank
    """,
    "trip_stats_by_time": """
        WITH time_buckets AS (
            SELECT floor(hour(pickup_at) / 6) * 6 AS start_hour,
                   timestampdiff(MICROSECOND, pickup_at, dropoff_at) / 60000000.0
                       AS duration_minutes,
                   trip_distance
            FROM trips
        )
        SELECT start_hour, count(*) AS trips,
               avg(duration_minutes) AS mean_duration_minutes,
               percentile_approx(duration_minutes, 0.5, 10000) AS median_duration_minutes,
               avg(trip_distance) AS mean_distance_miles,
               percentile_approx(trip_distance, 0.5, 10000) AS median_distance_miles
        FROM time_buckets
        GROUP BY start_hour
        ORDER BY start_hour
    """,
}


def register_views(spark: SparkSession, trips: DataFrame, zone_lookup: str | None = None) -> None:
    trips.createOrReplaceTempView("trips")
    zones = spark.createDataFrame([], "zone_id int, zone_name string")
    if zone_lookup:
        zones = (
            spark.read.option("header", True).option("mode", "FAILFAST").csv(zone_lookup)
            .select(F.col("LocationID").try_cast("int").alias("zone_id"), F.col("Zone").alias("zone_name"))
            .where(F.col("zone_id").isNotNull())
            .groupBy("zone_id").agg(F.min("zone_name").alias("zone_name"))
        )
    zones.createOrReplaceTempView("zones")
