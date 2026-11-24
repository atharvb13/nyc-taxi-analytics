"""Build the offline experiment report from the benchmark CSV files."""

import argparse
import csv
import json
from collections import defaultdict
from html import escape
from math import ceil
from pathlib import Path
from statistics import median

import matplotlib

matplotlib.use("Agg")
from matplotlib import pyplot as plt

BLUE = "#17667d"
GOLD = "#bc6418"
MIB = 1024 ** 2
PLOTS = (
    ("size-runtime", "Query time by input size", "Dots show medians. Bars show the shortest and longest trials."),
    ("size-throughput", "Trips read per second", "Throughput uses input rows before cleaning."),
    ("partitions", "Route query time after repartitioning", "The SQL shuffle setting stays at eight."),
    ("partition-shuffle", "Shuffle volume by repartition count", "Local and remote shuffle bytes read, summed for each trial."),
    ("cache", "Route query time with and without caching", "Building the cache is a separate Spark action."),
)


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def time_stats(rows):
    times = [float(row["runtime_seconds"]) for row in rows]
    return {
        "trials": len(times), "median_s": median(times),
        "min_s": min(times), "max_s": max(times),
    }


def summarize(metrics, stages, settings):
    measured = [row for row in metrics if row["phase"] == "measured" and row["status"] == "ok"]
    if any(row["status"] != "ok" for row in metrics):
        raise ValueError("Review the failed rows in metrics.csv before building the comparison")
    by_case = defaultdict(list)
    by_mode = defaultdict(list)
    by_trial = defaultdict(list)
    for row in measured:
        by_case[row["case"]].append(row)
        if row["experiment"] == "cache":
            by_mode[row["cache"]].append(row)
    for stage in stages:
        if stage["phase"] == "measured":
            by_trial[(stage["case"], stage["repeat"])].append(stage)

    sizes = []
    partitions = []
    for case, rows in by_case.items():
        row = rows[0]
        if row["experiment"] == "size":
            sizes.append({
                "case": case, "months": len(settings["cases"][case]["inputs"]),
                "rows": int(row["input_rows"]), "clean_rows": int(row["clean_rows"]),
                "rows_per_s": median(float(trial["input_rows_per_second"]) for trial in rows),
                **time_stats(rows),
            })
        if row["experiment"] == "partitions":
            trials = [by_trial[(case, trial["repeat"])] for trial in rows]
            if not all(trials):
                raise ValueError(f"Missing task metrics for {case}")
            widest_stages = [
                max(trial, key=lambda stage: (int(stage["tasks"]), int(stage["shuffle_read_bytes"])))
                for trial in trials
            ]
            partitions.append({
                "case": case, "partitions": int(row["repartition"]),
                "shuffle_read_bytes": median(
                    sum(int(stage["shuffle_read_bytes"]) for stage in trial) for trial in trials
                ),
                "max_tasks": max(int(stage["tasks"]) for stage in widest_stages),
                "wide_stage_median_task_ms": median(float(stage["median_task_ms"]) for stage in widest_stages),
                "wide_stage_max_task_ms": median(float(stage["max_task_ms"]) for stage in widest_stages),
                "disk_spill_max_bytes": max(
                    sum(int(stage["disk_spilled_bytes"]) for stage in trial) for trial in trials
                ),
                **time_stats(rows),
            })

    builds = [row for row in metrics if row["phase"] == "cache_build"]
    uncached = time_stats(by_mode["off"])
    cached = time_stats(by_mode["memory_and_disk"])
    build = {
        **time_stats(builds),
        "memory_bytes": median(int(row["cache_memory_bytes"]) for row in builds),
        "disk_bytes": median(int(row["cache_disk_bytes"]) for row in builds),
    }
    saved = uncached["median_s"] - cached["median_s"]
    route_digests = {row["result_sha256"] for row in measured if row["query"] == "routes_by_hour"}
    if len(route_digests) != 1 or "" in route_digests:
        raise ValueError("Route results differ across the partition and cache trials")
    return {
        "sizes": sorted(sizes, key=lambda row: row["months"]),
        "partitions": sorted(partitions, key=lambda row: row["partitions"]),
        "cache": {
            "off": uncached, "on": cached, "build": build,
            "break_even_queries": ceil(build["median_s"] / saved) if saved > 0 else None,
        },
        "routes_sha256": route_digests.pop(),
    }


def chart(plots, name, x, series, ylabel, *, xlabel="", ranges=None, bars=False):
    fig, ax = plt.subplots(figsize=(7.6, 4.15), layout="constrained")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.2)
    ax.set_axisbelow(True)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    if bars:
        marks = ax.bar(x, series, color=[BLUE, BLUE, GOLD] if name == "cache" else BLUE, width=0.55)
        ax.bar_label(marks, labels=[f"{value:.2f}" for value in series], padding=4)
        ax.margins(y=0.14)
    elif ranges:
        errors = [
            [value - row["min_s"] for value, row in zip(series, ranges, strict=True)],
            [row["max_s"] - value for value, row in zip(series, ranges, strict=True)],
        ]
        ax.errorbar(x, series, yerr=errors, color=BLUE, marker="o", capsize=5, linewidth=1.7)
    else:
        ax.plot(x, series, "o-", color=BLUE, linewidth=1.7)
    ax.set_ylim(bottom=0)
    fig.savefig(plots / f"{name}.png", dpi=145)
    plt.close(fig)


def write_charts(plots, summary):
    sizes = summary["sizes"]
    partitions = summary["partitions"]
    cache = summary["cache"]
    size_labels = [f"{row['rows'] / 1_000_000:.2f}" for row in sizes]
    partition_labels = [str(row["partitions"]) for row in partitions]
    chart(plots, "size-runtime", size_labels, [row["median_s"] for row in sizes],
          "Runtime (seconds)", xlabel="Input trips (millions)", ranges=sizes)
    chart(plots, "size-throughput", size_labels, [row["rows_per_s"] / 1_000_000 for row in sizes],
          "Million input trips per second", xlabel="Input trips (millions)")
    chart(plots, "partitions", partition_labels, [row["median_s"] for row in partitions],
          "Runtime (seconds)", xlabel="Explicit repartition count", ranges=partitions)
    chart(plots, "partition-shuffle", partition_labels,
          [row["shuffle_read_bytes"] / MIB for row in partitions], "Median shuffle read (MiB)",
          xlabel="Explicit repartition count", bars=True)
    chart(plots, "cache", ["Uncached query", "Cached query", "Cache build"],
          [cache[mode]["median_s"] for mode in ("off", "on", "build")], "Median time (seconds)",
          bars=True)


def write_html(run, summary, environment):
    size_rows = "".join(
        f"<tr><th>{row['months']}</th><td>{row['rows']:,}</td>"
        f"<td>{row['clean_rows']:,}</td><td>{row['median_s']:.3f}</td></tr>"
        for row in summary["sizes"]
    )
    partition_rows = "".join(
        f"<tr><th>{row['partitions']}</th><td>{row['median_s']:.3f}</td>"
        f"<td>{row['shuffle_read_bytes'] / MIB:.1f}</td><td>{row['max_tasks']}</td>"
        f"<td>{row['wide_stage_median_task_ms']:.0f} / {row['wide_stage_max_task_ms']:.0f}</td></tr>"
        for row in summary["partitions"]
    )
    figures = {
        name: f'<figure><figcaption><strong>{title}</strong><br>{note}</figcaption>'
              f'<img src="plots/{name}.png" alt="{title}"></figure>'
        for name, title, note in PLOTS
    }
    cache = summary["cache"]
    fastest = min(summary["partitions"], key=lambda row: row["median_s"])
    break_even = cache["break_even_queries"]
    cache_note = (f"At the measured medians, the build cost is recovered after about {break_even} queries."
                  if break_even is not None else "The cached query was no faster at the measured medians.")
    analytics_links = " · ".join(
        f'<a href="analytics/{path.name}">{path.stem.replace("_", " ").capitalize()}</a>'
        for path in sorted((run / "analytics").glob("*.csv"))
    )
    run_date = escape(environment["started_utc"][:10])
    machine = escape(environment["note"])
    page = f"""<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NYC Taxi Analytics | Spark benchmark</title>
<style>
  body {{ background: #f5f3eb; color: #192b31; font: 17px/1.55 system-ui, sans-serif; margin: 0; }}
  main {{ max-width: 1080px; margin: 0 auto; padding: 44px 26px 60px; }}
  h1 {{ font-size: clamp(34px, 5vw, 56px); line-height: 1.1; margin: 0; }}
  h2 {{ margin-top: 36px; border-top: 1px solid #a7b7b6; padding-top: 16px; }}
  a {{ color: #125e72; text-underline-offset: 3px; }}
  figure {{ margin: 0; background: white; padding: 16px; }}
  figcaption {{ margin: 0 0 9px; font-size: 15px; }}
  img {{ width: 100%; height: auto; display: block; }}
  .pair {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }}
  .table {{ overflow-x: auto; }}
  table {{ border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }}
  th, td {{ padding: 7px 12px; border-bottom: 1px solid #ccd3ce; text-align: right; }}
  thead th {{ background: #e3e9df; }}
  th:first-child {{ text-align: left; }}
  @media (max-width: 720px) {{ .pair {{ grid-template-columns: 1fr; }} }}
</style>
<main>
<h1>NYC Taxi Analytics</h1>
<p>NYC Yellow Taxi trips, January through April 2024. Spark benchmark started {run_date} (UTC).</p>
<p><a href="#size">Input size</a> · <a href="#partitions">Partitions</a> · <a href="#cache">Caching</a>
 · <a href="#files">Data and run files</a></p>
<h2 id="size">Input size</h2>
<p>The hourly pickup query uses the same settings as monthly files are added in order.</p>
<div class="table"><table><thead><tr><th>Months</th><th>Input rows</th><th>Clean rows</th>
<th>Median seconds</th></tr></thead><tbody>{size_rows}</tbody></table></div>
<div class="pair">{figures['size-runtime']}{figures['size-throughput']}</div>
<h2 id="partitions">Partitions</h2>
<p>The lowest median in this run was {fastest['median_s']:.3f} seconds at
{fastest['partitions']} partitions. The route query groups by pickup hour and both zone IDs,
ranks the most common routes, then joins the zone lookup twice.</p>
<div class="table"><table><thead><tr><th>Repartition count</th><th>Median seconds</th>
<th>Median shuffle read (MiB)</th><th>Peak stage tasks</th>
<th>Widest stage median / max task (ms)*</th></tr></thead><tbody>{partition_rows}</tbody></table></div>
<p><small>*Median across trials of the widest stage's median and maximum executor task times.
If stages have the same task count, this uses the one that read more shuffle data.</small></p>
<div class="pair">{figures['partitions']}{figures['partition-shuffle']}</div>
<h2 id="cache">Caching</h2>
<p>The route query took {cache['off']['median_s']:.3f} seconds uncached and
{cache['on']['median_s']:.3f} seconds cached. The cache took {cache['build']['median_s']:.3f}
seconds to build and held {cache['build']['memory_bytes'] / MIB:.1f} MiB in memory and
{cache['build']['disk_bytes'] / MIB:.1f} MiB on disk. {cache_note}</p>
{figures['cache']}
<p>Every measured route trial returned the same result hash across the partition and cache settings.</p>
<h2 id="files">Data and run files</h2>
<p>{machine}</p>
<p><a href="run.json">Exact settings</a> · <a href="metrics.csv">Trial timings</a> ·
<a href="stages.csv">Stage metrics</a> · <a href="tlc-manifest.json">TLC download manifest</a></p>
<p>{analytics_links}</p>
</main>
</html>
"""
    (run / "report.html").write_text(page, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True, type=Path, help="directory with completed experiment results")
    run = parser.parse_args().run
    environment = json.loads((run / "run.json").read_text(encoding="utf-8"))
    summary = summarize(read_csv(run / "metrics.csv"), read_csv(run / "stages.csv"), environment["settings"])
    (run / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    plots = run / "plots"
    plots.mkdir(exist_ok=True)
    write_charts(plots, summary)
    write_html(run, summary, environment)


if __name__ == "__main__":
    main()
