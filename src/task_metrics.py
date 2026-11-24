"""Summarize Spark task events for jobs started by the benchmark."""

import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import median

STAGE_FIELDS = (
    "case", "phase", "repeat", "stage_id", "stage_name", "tasks",
    "median_task_ms", "max_task_ms", "input_bytes", "input_rows",
    "shuffle_read_bytes", "shuffle_write_bytes", "memory_spilled_bytes", "disk_spilled_bytes",
)


def write_stages(event_dir: Path, output: Path) -> int:
    groups = defaultdict(set)
    attempts = {}
    tasks = defaultdict(list)
    for path in event_dir.iterdir():
        if not path.is_file() or path.name.startswith("."):
            continue
        with path.open(encoding="utf-8") as log:
            for line in log:
                event = json.loads(line)
                kind = event["Event"]
                if kind == "SparkListenerJobStart":
                    group = (event.get("Properties") or {}).get("spark.jobGroup.id")
                    if group and group.startswith("taxi:"):
                        for stage_id in event["Stage IDs"]:
                            groups[stage_id].add(tuple(group.split(":")[1:]))
                elif kind == "SparkListenerStageCompleted":
                    info = event["Stage Info"]
                    if not info.get("Failure Reason"):
                        name = info["Stage Name"].split(" at ", 1)[0]
                        attempts[info["Stage ID"]] = (info["Stage Attempt ID"], name)
                elif kind == "SparkListenerTaskEnd" and event["Task End Reason"]["Reason"] == "Success":
                    key = (event["Stage ID"], event["Stage Attempt ID"])
                    tasks[key].append(event.get("Task Metrics") or {})

    stage_rows = 0
    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=STAGE_FIELDS, lineterminator="\n")
        writer.writeheader()
        for stage_id, labels in sorted(groups.items()):
            if stage_id not in attempts:
                continue
            attempt, name = attempts[stage_id]
            stage = tasks[(stage_id, attempt)]
            if not stage:
                continue
            durations = [task.get("Executor Run Time", 0) for task in stage]
            for case, phase, repeat in sorted(labels):
                writer.writerow({
                    "case": case, "phase": phase, "repeat": repeat, "stage_id": stage_id,
                    "stage_name": name, "tasks": len(stage),
                    "median_task_ms": median(durations), "max_task_ms": max(durations),
                    "input_bytes": sum(task.get("Input Metrics", {}).get("Bytes Read", 0) for task in stage),
                    "input_rows": sum(task.get("Input Metrics", {}).get("Records Read", 0) for task in stage),
                    "shuffle_read_bytes": sum(
                        task.get("Shuffle Read Metrics", {}).get("Local Bytes Read", 0)
                        + task.get("Shuffle Read Metrics", {}).get("Remote Bytes Read", 0) for task in stage
                    ),
                    "shuffle_write_bytes": sum(
                        task.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0) for task in stage
                    ),
                    "memory_spilled_bytes": sum(task.get("Memory Bytes Spilled", 0) for task in stage),
                    "disk_spilled_bytes": sum(task.get("Disk Bytes Spilled", 0) for task in stage),
                })
                stage_rows += 1
    return stage_rows
