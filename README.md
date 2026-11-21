# NYC Taxi Analytics

A PySpark project for COMPSCI 532 Systems for Data Science. It ingests monthly NYC Yellow Taxi records, runs Spark SQL mobility analyses, and measures how query execution changes with input size.

## Set up and verify Spark

Use Python 3.12 and JDK 21. These were the versions used for the first milestone. Run the commands from the repository root.

```sh
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
export SPARK_LOCAL_IP=127.0.0.1
python -m src.analytics --smoke
python -m unittest discover -s tests -v
```

The smoke check verifies that four Spark partitions process 1,000 rows and return a sum of 499,500. The fixture tests cover schema errors, cross-file type changes, each cleaning stage, all three SQL workloads, unknown zone IDs, result equivalence after repartitioning and caching, and benchmark error reporting. Tests use generated fixtures and run without TLC downloads.

## Download data and run a query

The [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) publishes the trip data. The January and February 2024 Yellow Taxi files and optional zone lookup below were downloaded on November 21, 2026. The downloader writes URLs, dates, byte sizes, and SHA-256 checksums to `data/raw/manifest.json`. The checksums for the benchmark inputs are in the [recorded manifest](results/milestone-1/tlc-manifest.json).

- [yellow_tripdata_2024-01.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet)
- [yellow_tripdata_2024-02.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet)
- [taxi_zone_lookup.csv](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)

```sh
python scripts/download_data.py
python -m src.analytics data/raw/yellow_tripdata_2024-0[12].parquet \
  --query top_zones --zones data/raw/taxi_zone_lookup.csv
```

Spark validates the seven required columns in each file and normalizes their types before combining months. The CLI prints cumulative row counts for each cleaning stage, followed by the query result. Cleaning retains trips with a positive wall-clock duration of up to 24 hours and finite, nonnegative distance and fare. Zero distance or fare is allowed. TLC timestamps are treated as New York local clock readings. Zone IDs and passenger counts are kept where available.

| Query | Output |
| --- | --- |
| `hourly_pickups` | Pickup counts by day of week and hour. Spark numbers Sunday as 1. |
| `top_zones` | Top 10 pickup and top 10 drop-off zones, with names when the lookup is supplied. Unmatched IDs remain in the results. |
| `trip_stats_by_time` | Trip counts, means, and approximate medians of duration and distance in six-hour pickup windows. |

## First milestone baselines

```sh
python -m src.benchmark --config config/baseline.json --output output/baseline
```

The [baseline configuration](config/baseline.json) runs the same hourly pickup aggregation on January alone and on January plus February. It uses Spark 4.0.1 with two local cores, eight SQL shuffle partitions, adaptive query execution disabled, and no explicit Spark caching. Each case has one warm-up and three measured trials. The timer wraps `collect()` on the 168-row aggregate, so it includes scanning, filtering, query execution, and transfer of the result to Python. The separate quality-count pass and plan capture are outside the timer. Throughput uses input rows before cleaning.

| Input | Input rows | Clean rows | Median measured runtime | Median input rows/s |
| --- | ---: | ---: | ---: | ---: |
| January 2024 | 2,964,624 | 2,926,295 | 0.773 s | 3.84 M |
| January and February 2024 | 5,972,150 | 5,892,352 | 1.138 s | 5.25 M |

The January case ran first on a single Apple M4 machine with 16 GiB RAM. Reading two files also changed the scan layout. JVM warm-up and OS file caching may contribute to the higher measured throughput for the larger case. These numbers describe this local run.

The [recorded results](results/milestone-1/) include trial-level timings, failed-trial status fields, the Spark configuration and machine details, cumulative cleaning counts, and formatted physical plans. Raw TLC files stay in the ignored `data/` directory.

## AI use

OpenAI Codex assisted with implementing the first milestone, writing tests and documentation, running the baseline experiments, and setting up GitHub.
