"""Time Spark SQL across input, partition, and cache settings."""

import argparse
import csv
import json
import platform
import subprocess
from contextlib import redirect_stdout
from datetime import datetime, timezone
from hashlib import sha256
from io import StringIO
from os import environ
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from pyspark import StorageLevel

from src.clean import clean_trips, quality_counts
from src.ingest import read_trips
from src.queries import QUERIES, register_views
from src.runtime import start_spark
from src.task_metrics import write_stages

METRIC_FIELDS = (
    "run_id", "case", "query", "phase", "repeat", "input_rows", "clean_rows",
    "result_rows", "runtime_seconds", "input_rows_per_second", "status", "error",
    "experiment", "repartition", "cache", "result_sha256",
    "cache_memory_bytes", "cache_disk_bytes", "cached_partitions", "cache_partitions",
)
CACHE_MODES = ("off", "memory_and_disk")


def case_options(paths, settings):
    case = {"inputs": paths} if isinstance(paths, list) else paths
    return {
        "query": settings["query"],
        "experiment": "",
        "repartition": None,
        "cache": "off",
        "zones": None,
        **case,
    }


def begin_action(spark, case, phase, repeat):
    spark.sparkContext.setJobGroup(f"taxi:{case}:{phase}:{repeat}", f"{case} {phase} {repeat}")


def end_action(spark):
    spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
    spark.sparkContext.setLocalProperty("spark.job.description", None)


def run_case(spark, name, paths, settings, output, metrics, run_id):
    case = case_options(paths, settings)
    base = {
        "run_id": run_id, "case": name, "query": case["query"],
        "experiment": case["experiment"], "repartition": case["repartition"], "cache": case["cache"],
    }
    trips = None
    phase = "setup"
    started = None
    try:
        raw = read_trips(spark, case["inputs"])
        counts = quality_counts(raw)
        (output / f"{name}.quality.json").write_text(
            json.dumps(counts, indent=2) + "\n", encoding="utf-8"
        )
        base.update(input_rows=counts["input_rows"], clean_rows=counts["after_fare"])
        trips = clean_trips(raw)
        if case["repartition"]:
            trips = trips.repartition(case["repartition"])
        if case["cache"] != "off":
            phase = "cache_build"
            trips.persist(StorageLevel.MEMORY_AND_DISK_DESER)
            begin_action(spark, name, phase, 1)
            started = perf_counter()
            cache_rows = trips.count()
            elapsed = perf_counter() - started
            if cache_rows != counts["after_fare"]:
                raise RuntimeError("Cached row count differs from the cleaning count")
            blocks = spark.sparkContext._jsc.sc().getRDDStorageInfo()
            metrics.writerow({
                **base, "phase": phase, "repeat": 1, "status": "ok",
                "runtime_seconds": f"{elapsed:.6f}",
                "cache_memory_bytes": sum(block.memSize() for block in blocks),
                "cache_disk_bytes": sum(block.diskSize() for block in blocks),
                "cached_partitions": sum(block.numCachedPartitions() for block in blocks),
                "cache_partitions": sum(block.numPartitions() for block in blocks),
            })
        phase = "setup"
        started = None
        register_views(spark, trips, case["zones"])
    except Exception as error:
        record = {**base, "phase": phase, "status": "failed", "error": str(error)}
        if started is not None:
            record.update(repeat=1, runtime_seconds=f"{perf_counter() - started:.6f}")
        metrics.writerow(record)
        if trips is not None and case["cache"] != "off":
            trips.unpersist(blocking=True)
        return False
    finally:
        end_action(spark)

    try:
        return run_trials(spark, base, settings, output, metrics)
    finally:
        if case["cache"] != "off":
            trips.unpersist(blocking=True)


def run_trials(spark, base, settings, output, metrics):
    success = True
    for phase, repeats in (("warmup", settings["warmups"]), ("measured", settings["repetitions"])):
        for repeat in range(1, repeats + 1):
            record = {**base, "phase": phase, "repeat": repeat}
            started = None
            try:
                begin_action(spark, base["case"], phase, repeat)
                query = spark.sql(QUERIES[base["query"]])
                started = perf_counter()
                rows = query.collect()
                elapsed = perf_counter() - started
                record.update(
                    result_rows=len(rows),
                    runtime_seconds=f"{elapsed:.6f}",
                    input_rows_per_second=f"{base['input_rows'] / elapsed:.2f}",
                    result_sha256=sha256(json.dumps(rows, default=str).encode()).hexdigest(),
                    status="ok",
                )
            except Exception as error:
                if started is not None:
                    record["runtime_seconds"] = f"{perf_counter() - started:.6f}"
                record.update(status="failed", error=str(error))
                success = False
            finally:
                end_action(spark)
            metrics.writerow(record)

    if success:
        plan = StringIO()
        with redirect_stdout(plan):
            query.explain(mode="formatted")
        lines = [
            "Location: see case inputs in run.json" if line.startswith("Location:") else line.rstrip()
            for line in plan.getvalue().splitlines()
        ]
        (output / f"{base['case']}.plan.txt").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return success


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/baseline.json"))
    parser.add_argument("--output", type=Path, required=True, help="new directory for this run")
    parser.add_argument("--note", default="", help="machine or run conditions")
    parser.add_argument("--inputs", nargs="+", help="benchmark a single case with these inputs")
    parser.add_argument("--query", choices=QUERIES, help="override the query for each case")
    parser.add_argument("--repartition", type=int, help="override the explicit repartition count")
    parser.add_argument("--cache", choices=CACHE_MODES, help="override each case's cache mode")
    parser.add_argument("--repetitions", type=int, help="override the number of measured trials per case")
    args = parser.parse_args()
    settings = json.loads(args.config.read_text(encoding="utf-8"))
    if args.repetitions is not None:
        settings["repetitions"] = args.repetitions
    if args.inputs:
        settings["cases"] = {"custom": args.inputs}
    if args.query or args.repartition is not None or args.cache:
        overrides = {
            key: value for key, value in (
                ("query", args.query), ("repartition", args.repartition), ("cache", args.cache),
            ) if value is not None
        }
        settings["cases"] = {
            name: {**case_options(paths, settings), **overrides}
            for name, paths in settings["cases"].items()
        }
    if settings["warmups"] < 0 or settings["repetitions"] < 1 or settings["shuffle_partitions"] < 1:
        parser.error("provide nonnegative warmups, positive repetitions and positive shuffle partitions")
    if not settings["cases"] or any(not name.isidentifier() for name in settings["cases"]):
        parser.error("provide at least one case and use identifier-style case names")
    for name, paths in settings["cases"].items():
        case = case_options(paths, settings)
        if case["query"] not in QUERIES or case["cache"] not in CACHE_MODES or not case["inputs"]:
            parser.error(f"{name}: specify input files, a valid query, and a valid cache mode")
        if case["repartition"] is not None and (
            type(case["repartition"]) is not int or case["repartition"] < 1
        ):
            parser.error(f"{name}: repartition count must be a positive integer")
    if args.output.exists():
        parser.error("output directory already exists. Choose a new path for this run")
    args.output.mkdir(parents=True)
    if settings.get("task_metrics"):
        with TemporaryDirectory(prefix="taxi-spark-events-") as temporary:
            event_dir = Path(temporary).resolve()
            status = execute(settings, args.output, args.note, event_dir)
            if write_stages(event_dir, args.output / "stages.csv") == 0 and status == 0:
                raise RuntimeError("Spark did not write stage metrics for the benchmark actions")
            return status
    return execute(settings, args.output, args.note)


def execute(settings, output, note, event_dir=None) -> int:
    spark = start_spark(settings["master"], settings["shuffle_partitions"], event_dir)
    run_id = output.name
    try:
        java_home = environ.get("JAVA_HOME")
        java_command = str(Path(java_home) / "bin" / "java") if java_home else "java"
        java = subprocess.run([java_command, "-version"], text=True, capture_output=True, check=True)
        environment = {
            "run_id": run_id,
            "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "spark_version": spark.version,
            "python_version": platform.python_version(),
            "java_version": (java.stderr or java.stdout).splitlines()[0],
            "system": platform.platform(),
            "spark_master": spark.sparkContext.master,
            "spark_conf": {
                key: spark.conf.get(key) for key in (
                    "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
                    "spark.sql.session.timeZone",
                )
            },
            "cache_modes": sorted({case_options(paths, settings)["cache"] for paths in settings["cases"].values()}),
            "spark_driver_memory": spark.sparkContext.getConf().get("spark.driver.memory", "1g (default)"),
            "timed_action": "collect",
            "note": note,
            "settings": settings,
        }
        (output / "run.json").write_text(
            json.dumps(environment, indent=2) + "\n", encoding="utf-8"
        )
        success = True
        with (output / "metrics.csv").open("w", newline="", encoding="utf-8") as file:
            metrics = csv.DictWriter(file, fieldnames=METRIC_FIELDS, lineterminator="\n")
            metrics.writeheader()
            for name, paths in settings["cases"].items():
                print(f"Running {name}", flush=True)
                success = run_case(spark, name, paths, settings, output, metrics, run_id) and success
                file.flush()
        return 0 if success else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
