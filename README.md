# financial-market-lakehouse
An engineering-grade, idempotent financial data lakehouse pipeline built with PySpark, Airflow, and MinIO for high-throughput market data analytics

## Data Source

The raw market data used in this project was obtained from the following public Kaggle dataset:

- Dataset: BTC Price 1m
- Author: kaanxtr
- Source: https://www.kaggle.com/datasets/kaanxtr/btc-price-1m
- Example file used in this project: `AAVEUSDT`

This dataset contains 1-minute OHLCV-style cryptocurrency market data for multiple trading pairs, organized as one CSV file per symbol.

Note:
- The data is used for educational and take-home assignment purposes only.
- Please refer to the original Kaggle dataset page for licensing, updates, and full metadata.

## Current Pipeline Scope

The current implementation covers these layers:

- local infrastructure with Airflow, Spark, MinIO, and Postgres
- raw ingestion from local CSV files into MinIO `raw-zone`
- Spark transformation logic for cleansing and window-based market metrics
- curated analytical outputs written back to MinIO as partitioned Parquet datasets

## Data Lake Layout

### Raw Zone

The raw zone keeps the source-aligned files uploaded from the local `data/` directory.

Current raw object key convention:

- `raw-zone/market_data/symbol=<SYMBOL>/<FILE>.csv`

Example:

- `raw-zone/market_data/symbol=BTCUSDT/BTCUSDT.csv`

### Curated Zone

The curated zone is not part of the original Kaggle source. It is the output area created by the Spark transformation job after raw data has been cleaned and modeled.

Current curated datasets:

- `curated-zone/fact_trades/date=YYYY-MM-DD/...`
- `curated-zone/dim_symbol/...`
- `curated-zone/dim_time/...`

This means:

- the source dataset only gives you raw CSV files
- the pipeline creates the curated zone as a downstream analytics-ready layer

## Transformation Outputs

The current Spark transformation logic is designed to:

- parse timestamps and cast numeric columns
- filter duplicate or invalid market rows
- compute `vwap`
- compute `moving_avg_5`
- write a fact table and lightweight dimension tables to the curated zone

## Airflow Orchestration Preview

The pipeline has been registered in Airflow and can be triggered manually from the Airflow UI during development.

![Airflow DAG UI](./First_DAG_onUI.png)

Example successful task execution from the Airflow UI:

![Airflow Task Success](./First_Task_Success.png)
