"""Benchmark the same Spark SQL workload at multiple input sizes."""

import argparse
import csv
import json
import platform
import subprocess
from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from os import environ
from pathlib import Path
from time import perf_counter

from src.clean import clean_trips, quality_counts
from src.ingest import read_trips
from src.queries import QUERIES, register_views
from src.runtime import start_spark

METRIC_FIELDS = (
    "run_id", "case", "query", "phase", "repeat", "input_rows", "clean_rows",
    "result_rows", "runtime_seconds", "input_rows_per_second", "status", "error",
)


def run_case(spark, name, paths, settings, output, metrics, run_id):
    base = {"run_id": run_id, "case": name, "query": settings["query"]}
    try:
        raw = read_trips(spark, paths)
        counts = quality_counts(raw)
        (output / f"{name}.quality.json").write_text(
            json.dumps(counts, indent=2) + "\n", encoding="utf-8"
        )
        base.update(input_rows=counts["input_rows"], clean_rows=counts["after_fare"])
        register_views(spark, clean_trips(raw))
    except Exception as error:
        metrics.writerow({**base, "phase": "setup", "status": "failed", "error": str(error)})
        return False

    success = True
    for phase, repeats in (("warmup", settings["warmups"]), ("measured", settings["repetitions"])):
        for repeat in range(1, repeats + 1):
            record = {**base, "phase": phase, "repeat": repeat}
            started = None
            try:
                query = spark.sql(QUERIES[settings["query"]])
                started = perf_counter()
                rows = query.collect()
                elapsed = perf_counter() - started
                record.update(
                    result_rows=len(rows),
                    runtime_seconds=f"{elapsed:.6f}",
                    input_rows_per_second=f"{counts['input_rows'] / elapsed:.2f}",
                    status="ok",
                )
            except Exception as error:
                if started is not None:
                    record["runtime_seconds"] = f"{perf_counter() - started:.6f}"
                record.update(status="failed", error=str(error))
                success = False
            metrics.writerow(record)

    if success:
        plan = StringIO()
        with redirect_stdout(plan):
            query.explain(mode="formatted")
        lines = [
            "Location: see case inputs in run.json" if line.startswith("Location:") else line.rstrip()
            for line in plan.getvalue().splitlines()
        ]
        (output / f"{name}.plan.txt").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return success


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/baseline.json"))
    parser.add_argument("--output", type=Path, required=True, help="new directory for this run")
    parser.add_argument("--note", default="", help="machine or run conditions")
    args = parser.parse_args()
    settings = json.loads(args.config.read_text(encoding="utf-8"))
    if settings["query"] not in QUERIES:
        parser.error(f"unknown query: {settings['query']}")
    if settings["warmups"] < 0 or settings["repetitions"] < 1 or settings["shuffle_partitions"] < 1:
        parser.error("provide nonnegative warmups, positive repetitions and positive shuffle partitions")
    if not settings["cases"] or any(not name.isidentifier() for name in settings["cases"]):
        parser.error("provide at least one case and use identifier-style case names")
    if args.output.exists():
        parser.error("output directory already exists. Choose a new path for this run")
    args.output.mkdir(parents=True)

    spark = start_spark(settings["master"], settings["shuffle_partitions"])
    run_id = args.output.name
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
            "spark_cache": False,
            "timed_action": "collect",
            "note": args.note,
            "settings": settings,
        }
        (args.output / "run.json").write_text(
            json.dumps(environment, indent=2) + "\n", encoding="utf-8"
        )
        success = True
        with (args.output / "metrics.csv").open("w", newline="", encoding="utf-8") as file:
            metrics = csv.DictWriter(file, fieldnames=METRIC_FIELDS, lineterminator="\n")
            metrics.writeheader()
            for name, paths in settings["cases"].items():
                success = run_case(spark, name, paths, settings, args.output, metrics, run_id) and success
                file.flush()
        return 0 if success else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
