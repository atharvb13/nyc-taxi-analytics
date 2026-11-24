# NYC Taxi Analytics

COMPSCI 532 · Systems for Data Science

This project reads four months of NYC Yellow Taxi trips with PySpark. The SQL queries cover pickup demand, busy zones, trip length, fares, and routes. The benchmarks measure what happens when the input grows, when Spark repartitions the trips, and when repeated queries use a cache.

## Run locally

Use Python 3.12 and JDK 21. Run these commands from the repository root.

```sh
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
export SPARK_LOCAL_IP=127.0.0.1
python -m src.analytics --smoke
python -m unittest discover -s tests -v
```

The smoke check runs a sum across four Spark partitions. The tests use small fixtures and run without the TLC downloads. They check all five SQL queries, the cleaning rules, mixed monthly schemas, unknown zone IDs, and what gets logged when a benchmark query fails. They also check that repartitioning and caching preserve the result and that timed actions have stage metrics.

## Data and SQL queries

```sh
python scripts/download_data.py
python -m src.analytics --query all --zones data/raw/taxi_zone_lookup.csv \
  --output output/analytics data/raw/yellow_tripdata_2024-0[1-4].parquet
```

The downloader puts the January, February, March, and April 2024 Parquet files and the taxi zone lookup in `data/raw/`. It checks existing downloads against its manifest before reusing them. The committed manifest lists the exact filenames, source URLs, download dates, sizes, and SHA-256 hashes used for the final run. [TLC trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) · [Download manifest](results/final/tlc-manifest.json)

Spark checks the seven required fields in each file and casts them to common types before combining months. The cleaning pass drops missing timestamps, drop-offs at or before pickup, durations over 24 hours, and missing, negative, or non-finite distances and fares. It prints how many trips remain after each step. Pickup hours use New York local time.

| Query | Output |
| --- | --- |
| `hourly_pickups` | Pickups by day of week and hour. Sunday is day 1. |
| `top_zones` | The ten busiest pickup zones and the ten busiest drop-off zones. |
| `trip_stats_by_time` | Counts, means, and approximate medians of duration and distance, grouped in six-hour pickup windows. |
| `fare_by_pickup_zone` | Mean base fare and mean fare per mile for trips of at least 0.1 miles, by pickup zone. Spark averages the per-trip ratios. |
| `routes_by_hour` | The five busiest pickup-to-drop-off zone pairs in each hour, with zone names where the lookup has them. |

The command writes one CSV per query and a `quality.json` file. To preview a single result in the terminal, omit `--output` and pick a query. The [saved analytics](results/final/analytics/) use all four months.

## Reproduce the benchmarks

```sh
python -m pip install -r requirements-report.txt
python -m src.benchmark --config config/experiments.json --output output/benchmark
cp data/raw/manifest.json output/benchmark/tlc-manifest.json
cp -R output/analytics output/benchmark/analytics
python -m src.report --run output/benchmark
```

Open `output/benchmark/report.html` in a browser. The report works offline with the files in that directory. The [committed report](results/final/report.html) has five charts, and [the run directory](results/final/) has the timings, physical plans, exact settings, quality counts, and stage metrics. Choose a new output directory when rerunning the analytical or benchmark commands.

The runner also accepts `--inputs`, `--query`, `--repartition`, `--cache`, and `--repetitions` for a single comparison or a quick check. The config lists the file set and query for each final case. [Experiment config](config/experiments.json) · [Original milestone](results/milestone-1/)

### Measurement setup

The final run used Spark 4.0.1 on an Apple M4 with 16 GiB RAM, two local cores, eight SQL shuffle partitions, and adaptive query execution off. Each case had one warm-up and three measured trials. The timer covers `collect()` on the bounded SQL result, including scans, filtering, shuffles, and result transfer when those are part of the plan. Spark runs the quality counts and records the physical plan outside the measured action. Throughput uses input rows before cleaning.

Input size runs the same hourly query as months are added in order. Partitioning holds January and February fixed and changes the explicit round-robin repartition before the route query. Caching uses that same two-month route query without an explicit repartition. The run order was uncached, cached, cached, uncached. The cached cases time `count()` to build the cache before the query warm-up and record memory and disk use separately. Every timed route query produced the same result hash.

### Results and limits

| Input to hourly pickups | Raw trips | Clean trips | Median |
| --- | ---: | ---: | ---: |
| January | 2,964,624 | 2,926,295 | 0.772 s |
| January–February | 5,972,150 | 5,892,352 | 1.267 s |
| January–March | 9,554,778 | 9,415,372 | 2.264 s |
| January–April | 13,069,067 | 12,870,983 | 3.104 s |

The largest input had 4.4 times as many trips as January and took about four times as long. The hourly plan projects the fields used for filtering and grouping from Parquet. Each task can aggregate its own rows before Spark shuffles the partial counts.

For the two-month route query, the median times at 4, 8, 16, and 32 explicit partitions were 2.981, 2.944, 3.065, and 3.236 seconds. The trial ranges at 4 and 8 overlapped. As the count went from 4 to 32, median shuffle reads rose from 88.1 to 102.7 MiB and the largest stage grew from 8 to 32 tasks. The partition cases recorded no disk spill. With two local cores, the extra tasks and shuffle traffic are plausible costs. [Stage metrics](results/final/stages.csv)

The route query took 1.522 seconds without caching and 0.485 seconds after building the cache. The build took 2.603 seconds and used about 240 MiB of memory. At these medians, three repeated queries cover the build cost. The cached plan reads cleaned rows from `InMemoryTableScan` and still exchanges data for the route grouping and ranking. [Cached plan](results/final/cache_on_a.plan.txt)

All trials ran on one host and shared one Spark session. The OS file cache was left in place. More monthly files change the scan layout as well as the row count. The route result covers only five pairs per hour, and all trip records come from the same four-month window. The project measures local task parallelism.

## Demo and slides

```sh
python -m src.analytics --query top_zones --zones data/raw/taxi_zone_lookup.csv \
  data/raw/yellow_tripdata_2024-01.parquet
```

The command shows the cleaning counts and the zone ranking. Use [the route plan](results/final/partition_16.plan.txt) to point out the repartition and SQL exchanges, then open the [report](results/final/report.html) to compare the experiment timings. The [presentation](slides/nyc-taxi-analytics.pptx) includes the data flow and results.
