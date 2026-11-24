import csv
import io
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src.benchmark import METRIC_FIELDS, begin_action, end_action, run_case
from src.clean import clean_trips, quality_counts
from src.ingest import normalize_trips, read_trips
from src.queries import QUERIES, register_views
from src.runtime import start_spark
from src.task_metrics import write_stages

MONDAY = datetime(2024, 1, 1, 8)
SCHEMA = """
    tpep_pickup_datetime timestamp_ntz, tpep_dropoff_datetime timestamp_ntz,
    PULocationID long, DOLocationID long, trip_distance double,
    fare_amount double, passenger_count int
"""


def trip(at=MONDAY, minutes=10, pickup=1, dropoff=2, distance=2.0, fare=8.0):
    return at, at + timedelta(minutes=minutes), pickup, dropoff, distance, fare, 1


class MilestoneTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = start_spark(shuffle_partitions=2)
        cls.fixture = cls.spark.createDataFrame([
            trip(),
            trip(minutes=20, distance=3.0),
            trip(minutes=30, pickup=999, distance=4.0),
            trip(at=MONDAY.replace(hour=13), minutes=40, dropoff=1, distance=5.0),
            trip(at=MONDAY + timedelta(days=1), minutes=10, distance=2.0),
            (None, *trip()[1:]),
            trip(minutes=-1),
            trip(minutes=24 * 60 + 1),
            trip(distance=-1.0),
            trip(distance=float("nan")),
            trip(fare=-1.0),
            trip(fare=float("inf")),
        ], SCHEMA)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def cleaned_fixture(self):
        return clean_trips(normalize_trips(self.fixture, "fixture"))

    def test_schema_errors_identify_file_and_field(self):
        with self.assertRaisesRegex(ValueError, "january.*missing.*fare_amount"):
            normalize_trips(self.fixture.drop("fare_amount"), "january")
        bad = self.fixture.selectExpr("*", "cast(fare_amount as string) as invalid_fare")
        bad = bad.drop("fare_amount").withColumnRenamed("invalid_fare", "fare_amount")
        with self.assertRaisesRegex(ValueError, "february.*fare_amount.*string"):
            normalize_trips(bad, "february")

    def test_mixed_parquet_schemas_normalize_before_union(self):
        with TemporaryDirectory() as directory:
            paths = [str(Path(directory) / name) for name in ("integer", "double")]
            row = self.fixture.limit(1)
            row.write.parquet(paths[0])
            row.withColumn("passenger_count", row.passenger_count.cast("double")).write.parquet(paths[1])
            counts = read_trips(self.spark, paths).groupBy("passenger_count").count().collect()
        self.assertEqual([tuple(row) for row in counts], [(1.0, 2)])

    def test_cleaning_counts_each_stage(self):
        raw = normalize_trips(self.fixture, "fixture")
        self.assertEqual(quality_counts(raw), {
            "input_rows": 12, "after_timestamps": 9, "after_distance": 7, "after_fare": 5,
        })
        self.assertEqual(clean_trips(raw).count(), 5)
        self.assertEqual(quality_counts(raw.limit(0)), dict.fromkeys(quality_counts(raw), 0))

    def test_hourly_counts_stable_when_repartitioned_and_cached(self):
        trips = self.cleaned_fixture()
        register_views(self.spark, trips)
        rows = self.spark.sql(QUERIES["hourly_pickups"]).collect()
        self.assertEqual([tuple(row) for row in rows], [(2, 8, 3), (2, 13, 1), (3, 8, 1)])
        cached = trips.repartition(3).cache()
        try:
            cached.count()
            register_views(self.spark, cached)
            self.assertEqual(self.spark.sql(QUERIES["hourly_pickups"]).collect(), rows)
        finally:
            cached.unpersist()

    def test_top_zones_preserves_unknown_ids_and_deduplicates_lookup(self):
        with TemporaryDirectory() as directory:
            lookup = Path(directory) / "zones.csv"
            lookup.write_text("LocationID,Zone\n1,Alpha\n1,Alternate\n2,Beta\n", encoding="utf-8")
            register_views(self.spark, self.cleaned_fixture(), str(lookup))
            rows = self.spark.sql(QUERIES["top_zones"]).collect()
        self.assertEqual([tuple(row) for row in rows], [
            ("dropoff", 2, "Beta", 4), ("dropoff", 1, "Alpha", 1),
            ("pickup", 1, "Alpha", 4), ("pickup", 999, None, 1),
        ])

    def test_trip_stats_by_time(self):
        register_views(self.spark, self.cleaned_fixture())
        morning, afternoon = self.spark.sql(QUERIES["trip_stats_by_time"]).collect()
        self.assertEqual(tuple(morning), (6, 4, 17.5, 10, 2.75, 2.0))
        self.assertEqual(tuple(afternoon), (12, 1, 40, 40, 5.0, 5.0))

    def test_fare_per_mile_filters_short_trips_and_averages_each_ratio(self):
        short = self.spark.createDataFrame([trip(distance=0.02, fare=20.0)], SCHEMA)
        raw = normalize_trips(self.fixture.unionByName(short), "fixture")
        register_views(self.spark, clean_trips(raw))
        rows = self.spark.sql(QUERIES["fare_by_pickup_zone"]).collect()
        self.assertEqual([tuple(row) for row in rows], [
            (1, None, 4, 8.0, 3.07),
            (999, None, 1, 8.0, 2.0),
        ])

    def test_routes_rank_within_each_hour_and_keep_unmatched_zones(self):
        with TemporaryDirectory() as directory:
            lookup = Path(directory) / "zones.csv"
            lookup.write_text("LocationID,Zone\n1,Alpha\n2,Beta\n", encoding="utf-8")
            register_views(self.spark, self.cleaned_fixture(), str(lookup))
            rows = self.spark.sql(QUERIES["routes_by_hour"]).collect()
        self.assertEqual([tuple(row) for row in rows], [
            (8, 1, 1, "Alpha", 2, "Beta", 3),
            (8, 2, 999, None, 2, "Beta", 1),
            (13, 1, 1, "Alpha", 1, "Alpha", 1),
        ])

    def test_benchmark_records_timed_and_failed_trials(self):
        settings = {"query": "hourly_pickups", "warmups": 1, "repetitions": 1}
        metrics = io.StringIO()
        writer = csv.DictWriter(metrics, fieldnames=METRIC_FIELDS)
        writer.writeheader()
        with TemporaryDirectory() as directory:
            output = Path(directory)
            path = str(output / "fixture.parquet")
            self.fixture.write.parquet(path)
            self.assertTrue(run_case(self.spark, "fixture", [path], settings, output, writer, "test"))
            self.assertIn("HashAggregate", (output / "fixture.plan.txt").read_text(encoding="utf-8"))
            case = {"inputs": [path], "cache": "memory_and_disk", "repartition": 3}
            self.assertTrue(run_case(self.spark, "cached", case, settings, output, writer, "test"))
            self.assertFalse(self.spark.table("trips").is_cached)
            with patch("src.benchmark.QUERIES", {"hourly_pickups": "SELECT * FROM missing_view"}):
                self.assertFalse(run_case(self.spark, "failed", [path], settings, output, writer, "test"))

        metrics.seek(0)
        reader = csv.DictReader(metrics)
        self.assertEqual(reader.fieldnames, list(METRIC_FIELDS))
        warmup, measured, buildup, cached_warmup, cached, failed_warmup, failed_trial = list(reader)
        self.assertEqual((warmup["phase"], measured["phase"]), ("warmup", "measured"))
        self.assertEqual((measured["status"], measured["clean_rows"], measured["result_rows"]), ("ok", "5", "3"))
        self.assertGreater(float(measured["runtime_seconds"]), 0)
        self.assertEqual((buildup["phase"], buildup["cached_partitions"]), ("cache_build", "3"))
        self.assertGreater(int(buildup["cache_memory_bytes"]), 0)
        self.assertEqual(cached_warmup["phase"], "warmup")
        self.assertEqual(cached["result_sha256"], measured["result_sha256"])
        self.assertEqual((failed_warmup["status"], failed_trial["status"]), ("failed", "failed"))
        self.assertIn("missing_view", failed_trial["error"])


class StageMetricsTests(unittest.TestCase):
    def test_event_log_keeps_stages_for_the_timed_action(self):
        with TemporaryDirectory() as directory:
            logs = Path(directory).resolve() / "logs"
            logs.mkdir()
            spark = start_spark(shuffle_partitions=2, event_dir=logs)
            try:
                spark.range(20, numPartitions=2).count()
                begin_action(spark, "fixture", "measured", 1)
                self.assertEqual(spark.range(10, numPartitions=2).count(), 10)
                end_action(spark)
            finally:
                spark.stop()
            output = logs.parent / "stages.csv"
            self.assertGreater(write_stages(logs, output), 0)
            with output.open(newline="", encoding="utf-8") as file:
                rows = list(csv.DictReader(file))
        self.assertEqual({(row["case"], row["phase"], row["repeat"]) for row in rows}, {
            ("fixture", "measured", "1"),
        })
        self.assertEqual({row["stage_name"] for row in rows}, {"count"})
        self.assertEqual(sum(int(row["input_rows"]) for row in rows), 10)
