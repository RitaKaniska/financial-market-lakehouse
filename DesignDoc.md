# Financial Market Data Pipeline Design Doc

## 1. Project Overview

This project builds a small but complete data pipeline for 1-minute cryptocurrency market data. The goal is to ingest raw historical market files, store them in an S3-compatible data lake, transform them with Spark, and expose curated analytics-ready data for downstream querying and visualization.

The implementation is designed for a take-home data engineering assignment, so the focus is on:

- an end-to-end pipeline that can be run locally
- clear separation between raw and curated data zones
- reproducible orchestration with Airflow
- scalable transformation logic with Spark
- a lightweight serving layer for business analysis

## 2. Data Source

The dataset used in this project comes from Kaggle:

- Dataset: `BTC Price 1m`
- Author: `kaanxtr`
- Source: <https://www.kaggle.com/datasets/kaanxtr/btc-price-1m>

This dataset contains 1-minute OHLCV-style cryptocurrency market data for multiple trading pairs such as `BTCUSDT`, `ETHUSDT`, and `AAVEUSDT`. In the local project layout, each symbol is stored as a separate CSV file under the `data/` directory.

Example schema:

- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `close_time`
- `quote_asset_volume`
- `number_of_trades`
- `taker_buy_base_asset_volume`
- `taker_buy_quote_asset_volume`

This source is a good fit for the assignment because it is time-series based, large enough to justify distributed processing, and naturally supports partitioning by date.

## 3. Architecture Overview

The pipeline follows this high-level flow:

`Kaggle Dataset -> Airflow -> MinIO Raw Zone -> Spark -> MinIO Curated Zone -> DuckDB/Streamlit`

Component responsibilities:

- `Airflow` orchestrates ingestion, transformation, and data quality tasks.
- `MinIO` acts as a local S3-compatible object store for raw and curated data.
- `Spark` performs cleansing, aggregation, joins, and writes partitioned Parquet outputs.
- `DuckDB` reads curated Parquet data efficiently for analytics queries.
- `Streamlit` provides a simple UI for business-facing visualizations.

## 4. Initial Deployment Topology

The local environment is containerized with Docker Compose. The initial infrastructure includes:

- `postgres` for Airflow metadata
- `airflow-webserver`
- `airflow-scheduler`
- `minio`
- `spark-master`
- `spark-worker`

Current local access points:

- Airflow UI: `http://localhost:8080`
- Spark Master UI: `http://localhost:8081`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`

## 5. Data Zones

### Raw Zone

The raw zone stores source data with minimal modification. Its purpose is to preserve the original input so the pipeline can be rerun or reprocessed without downloading the dataset again.

Expected path pattern:

- `s3://raw-zone/<symbol>/...`

Typical characteristics:

- source-aligned structure
- minimal transformation
- useful for replay and debugging

### Curated Zone

The curated zone stores cleaned and analytics-ready datasets in columnar format.

Expected path pattern:

- `s3://curated-zone/fact_trades/date=YYYY-MM-DD/...`
- `s3://curated-zone/dim_symbol/...`
- `s3://curated-zone/dim_time/...`

Typical characteristics:

- cleaned and typed columns
- derived metrics such as VWAP or moving averages
- partitioned Parquet outputs for efficient reads

## 6. Data Modeling Plan

The serving model will follow a simple star schema.

### Fact Table

`fact_trades`

Candidate columns:

- `trade_timestamp`
- `date`
- `symbol`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `quote_asset_volume`
- `number_of_trades`
- derived metrics such as `vwap` and moving averages

### Dimension Tables

`dim_symbol`

- `symbol`
- `base_asset`
- `quote_asset`
- optional descriptive metadata

`dim_time`

- `timestamp`
- `date`
- `year`
- `month`
- `day`
- `hour`
- `minute`
- optional weekday fields

This model supports common analytical questions around price movement, trade volume, and time-based trends.

## 7. Transformation Plan

The Spark layer is expected to implement at least three non-trivial jobs:

1. Cleansing job
- parse timestamps
- cast numeric columns
- remove null or invalid rows
- filter out negative prices or volumes

2. Windowed aggregation job
- compute VWAP
- compute moving averages over time windows
- prepare trend-oriented metrics for downstream use

3. Join and modeling job
- enrich fact data with `dim_symbol`
- generate time-based dimensional attributes
- write curated Parquet outputs partitioned by `date`

## 8. Orchestration Plan

Airflow will manage the pipeline as a DAG with a simple dependency chain:

`ingest_raw -> spark_transform -> data_quality_checks`

Key orchestration goals:

- repeatable execution
- retry handling for transient failures
- idempotent reruns for historical dates
- clear separation between ingestion and transformation steps

## 9. Why This Tech Stack

### Airflow

Airflow is a strong fit for this assignment because it provides explicit workflow orchestration, retry policies, task dependencies, and observability through a web UI.

### MinIO

MinIO is used to simulate an S3-like data lake locally. This keeps the project lightweight while still following patterns that are close to production object storage workflows.

### Spark

Spark is appropriate because the dataset is large, time-series based, and requires transformations such as cleansing, partitioned writes, and window functions that are common in data engineering workloads.

### DuckDB and Streamlit

DuckDB offers a fast and simple way to query Parquet directly without provisioning a separate warehouse. Streamlit provides a lightweight interface to demonstrate business value from the curated data.

## 10. Risks and Constraints

Current known constraints:

- local machine resources may limit Spark parallelism
- Docker-based local development can introduce networking and image compatibility issues
- historical CSV files may contain schema inconsistencies across symbols

Expected mitigation:

- keep the first version small and reproducible
- use partitioned Parquet in curated outputs
- validate schema early during ingestion and transformation

## 11. Next Steps

### Day 2

- move raw source data into the MinIO raw zone
- define folder conventions for ingestion outputs
- add structured logging and failure handling

### Day 3

- implement Spark cleansing and metric computation
- generate star-schema-style curated outputs
- write Parquet files partitioned by `date`

### Day 4+

- connect the flow with an Airflow DAG
- add data quality checks
- build the serving layer with DuckDB and Streamlit

