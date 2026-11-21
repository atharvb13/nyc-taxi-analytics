# NYC Taxi Analytics

COMPSCI 532 · Systems for Data Science

The first experiment times the same Spark SQL query on one month of NYC Yellow Taxi trips and then on two. The query groups pickups by day of week and hour.

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

The smoke check runs a sum across four Spark partitions. The tests use small fixtures, so you can run them before downloading the trip files. They check the cleaning rules, the SQL results, and what the benchmark logs when a query fails.

## Download data and run a query

```sh
python scripts/download_data.py
python -m src.analytics \
  data/raw/yellow_tripdata_2024-01.parquet data/raw/yellow_tripdata_2024-02.parquet \
  --query top_zones --zones data/raw/taxi_zone_lookup.csv
```

The script downloads `yellow_tripdata_2024-01.parquet`, `yellow_tripdata_2024-02.parquet`, and `taxi_zone_lookup.csv` into the Git-ignored `data/raw/` folder. The files for this run were downloaded on November 21, 2026. See the manifest for the exact URLs and checksums. [TLC data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) · [Download manifest](results/milestone-1/tlc-manifest.json)

The CLI checks the required columns and casts them to common types before combining the months. It drops trips with missing timestamps, drop-offs at or before pickup, or durations over 24 hours. It also drops missing, negative, or non-finite distances and fares. The CLI prints how many trips remain after each step. It uses New York local time.

| Query | Output |
| --- | --- |
| `hourly_pickups` | Pickups by weekday and hour. Sunday is day 1. |
| `top_zones` | Ten pickup zones and ten drop-off zones, ranked by trip count. The lookup adds names where available. |
| `trip_stats_by_time` | Trip counts, mean and approximate median duration, and mean and approximate median distance by six-hour pickup window. |

## First benchmark

```sh
python -m src.benchmark --config config/baseline.json --output output/baseline
```

| Input | Input rows | Clean rows | Median runtime | Median input rows/s |
| --- | ---: | ---: | ---: | ---: |
| January 2024 | 2,964,624 | 2,926,295 | 0.773 s | 3.84 M |
| January and February 2024 | 5,972,150 | 5,892,352 | 1.138 s | 5.25 M |

Both cases used Spark 4.0.1 on an Apple M4 with 16 GiB RAM, two local cores, and eight shuffle partitions. Adaptive query execution was off, and Spark ran without caching the trip data. Each case had one warm-up and three timed trials. The timer covers the `collect()` call, from scanning and filtering the files through returning 168 counts to Python. Throughput uses the row count before cleaning. [Benchmark settings](config/baseline.json) · [Timings, counts, and query plans](results/milestone-1/)

The two-month run read roughly twice as many rows and took about 47% longer. January ran first in the same Spark session, and the second case added another Parquet file. The JVM, OS file cache, and different scan layout could all affect that gap.

## AI use

OpenAI Codex helped write the code, tests, and README for this milestone, run the benchmarks, and set up the repository.
