# financial-market-lakehouse
An engineering-grade, idempotent financial data lakehouse pipeline built with PySpark, Airflow, and MinIO for high-throughput market data analytics

# Data Source

The raw market data used in this project was obtained from the following public Kaggle dataset:

- Dataset: BTC Price 1m
- Author: kaanxtr
- Source: https://www.kaggle.com/datasets/kaanxtr/btc-price-1m
- Example file used in this project: `AAVEUSDT`

This dataset contains 1-minute OHLCV-style cryptocurrency market data for multiple trading pairs, organized as one CSV file per symbol.

Note:
- The data is used for educational and take-home assignment purposes only.
- Please refer to the original Kaggle dataset page for licensing, updates, and full metadata.
