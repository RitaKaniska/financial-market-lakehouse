from pathlib import Path
import sys

import pytest
from pyspark.sql import functions as F

from src.jobs.transform_data import (
    add_window_metrics,
    build_dim_symbol,
    build_dim_time,
    build_fact_trades,
    cleanse_market_data,
)


# =========================================================
# Helpers
# =========================================================

MARKET_COLUMNS = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
    "symbol",
]


CURATED_COLUMNS = [
    "symbol",
    "date",
    "event_timestamp",
    "close_timestamp",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
]


def cast_market_df(df):
    return (
        df.withColumn("date", F.col("date").cast("date"))
        .withColumn(
            "event_timestamp",
            F.col("event_timestamp").cast("timestamp"),
        )
        .withColumn(
            "close_timestamp",
            F.col("close_timestamp").cast("timestamp"),
        )
    )


# =========================================================
# cleanse_market_data
# =========================================================

def test_cleanse_market_data_filters_invalid_and_duplicate_rows(
    spark_session,
):
    data = [
        (
            "2024-01-01 00:00:00",
            "100",
            "110",
            "90",
            "105",
            "10",
            "2024-01-01 00:00:59.999",
            "1050",
            5,
            "5",
            "525",
            "0",
            "BTCUSDT",
        ),
        (
            "2024-01-01 00:00:00",
            "100",
            "110",
            "90",
            "105",
            "10",
            "2024-01-01 00:00:59.999",
            "1050",
            5,
            "5",
            "525",
            "0",
            "BTCUSDT",
        ),
        (
            "2024-01-01 00:01:00",
            "100",
            "110",
            "90",
            "-1",
            "10",
            "2024-01-01 00:01:59.999",
            "1000",
            5,
            "5",
            "500",
            "0",
            "BTCUSDT",
        ),
    ]

    df_input = spark_session.createDataFrame(data, MARKET_COLUMNS)

    df_output = cleanse_market_data(df_input)

    rows = df_output.collect()

    assert len(rows) == 1

    row = rows[0]

    assert row["symbol"] == "BTCUSDT"
    assert row["close_price"] == 105.0

    dtypes = dict(df_output.dtypes)

    assert dtypes["close_price"] == "double"
    assert dtypes["volume"] == "double"


# =========================================================
# add_window_metrics
# =========================================================

def test_add_window_metrics_computes_vwap_and_moving_average(
    spark_session,
):
    data = [
        (
            "BTCUSDT",
            "2024-01-01",
            "2024-01-01 00:00:00",
            "2024-01-01 00:00:59",
            100.0,
            110.0,
            90.0,
            100.0,
            10.0,
            1000.0,
            5,
            5.0,
            500.0,
        ),
        (
            "BTCUSDT",
            "2024-01-01",
            "2024-01-01 00:01:00",
            "2024-01-01 00:01:59",
            110.0,
            120.0,
            100.0,
            110.0,
            5.0,
            550.0,
            3,
            2.0,
            220.0,
        ),
    ]

    columns = CURATED_COLUMNS

    df_input = spark_session.createDataFrame(data, columns)

    df_output = (
        add_window_metrics(cast_market_df(df_input))
        .orderBy("event_timestamp")
    )

    rows = df_output.collect()

    assert rows[0]["vwap"] == pytest.approx(100.0, rel=1e-6)

    assert rows[1]["vwap"] == pytest.approx(
        (1000.0 + 550.0) / (10.0 + 5.0),
        rel=1e-6,
    )

    assert rows[1]["moving_avg_5"] == pytest.approx(
        105.0,
        rel=1e-6,
    )


def test_add_window_metrics_partitions_by_symbol(
    spark_session,
):
    data = [
        (
            "BTCUSDT",
            "2024-01-01",
            "2024-01-01 00:00:00",
            "2024-01-01 00:00:59",
            100.0,
            110.0,
            90.0,
            100.0,
            10.0,
            1000.0,
            5,
            5.0,
            500.0,
        ),
        (
            "ETHUSDT",
            "2024-01-01",
            "2024-01-01 00:01:00",
            "2024-01-01 00:01:59",
            200.0,
            210.0,
            190.0,
            200.0,
            20.0,
            4000.0,
            10,
            10.0,
            2000.0,
        ),
    ]

    df_input = spark_session.createDataFrame(
        data,
        CURATED_COLUMNS,
    )

    df_output = add_window_metrics(
        cast_market_df(df_input)
    )

    rows = {
        row["symbol"]: row
        for row in df_output.collect()
    }

    assert rows["BTCUSDT"]["vwap"] == pytest.approx(
        100.0,
        rel=1e-6,
    )

    assert rows["ETHUSDT"]["vwap"] == pytest.approx(
        200.0,
        rel=1e-6,
    )


# =========================================================
# build_dim_symbol
# =========================================================

def test_build_dim_symbol_splits_assets_correctly(
    spark_session,
):
    df_input = spark_session.createDataFrame(
        [
            ("BTCUSDT",),
            ("ETHUSDC",),
            ("SOLBUSD",),
            ("BTCFDUSD",),
        ],
        ["symbol"],
    )

    rows = {
        row["symbol"]: row
        for row in build_dim_symbol(df_input).collect()
    }

    assert rows["BTCUSDT"]["base_asset"] == "BTC"
    assert rows["BTCUSDT"]["quote_asset"] == "USDT"

    assert rows["ETHUSDC"]["base_asset"] == "ETH"
    assert rows["ETHUSDC"]["quote_asset"] == "USDC"

    assert rows["SOLBUSD"]["base_asset"] == "SOL"
    assert rows["SOLBUSD"]["quote_asset"] == "BUSD"

    assert rows["BTCFDUSD"]["quote_asset"] == "FDUSD"


# =========================================================
# build_dim_time
# =========================================================

def test_build_dim_time_derives_calendar_fields(
    spark_session,
):
    df_input = spark_session.createDataFrame(
        [
            ("2024-01-01 13:45:00", "2024-01-01"),
        ],
        ["event_timestamp", "date"],
    )

    df_input = cast_market_df(
        df_input.withColumnRenamed(
            "event_timestamp",
            "event_timestamp",
        ).withColumn(
            "close_timestamp",
            F.col("event_timestamp"),
        )
    )

    row = build_dim_time(df_input).collect()[0]

    assert row["year"] == 2024
    assert row["month"] == 1
    assert row["day"] == 1
    assert row["hour"] == 13
    assert row["minute"] == 45


# =========================================================
# build_fact_trades
# =========================================================

def test_build_fact_trades_keeps_expected_columns(
    spark_session,
):
    data = [
        (
            "BTCUSDT",
            "2024-01-01",
            "2024-01-01 00:00:00",
            "2024-01-01 00:00:59",
            100.0,
            110.0,
            90.0,
            105.0,
            10.0,
            1050.0,
            5,
            5.0,
            525.0,
            105.0,
            105.0,
        )
    ]

    columns = CURATED_COLUMNS + [
        "vwap",
        "moving_avg_5",
    ]

    df_input = spark_session.createDataFrame(
        data,
        columns,
    )

    df_output = build_fact_trades(
        cast_market_df(df_input)
    )

    expected_columns = {
        *CURATED_COLUMNS,
        "vwap",
        "moving_avg_5",
    }

    assert set(df_output.columns) == expected_columns

    rows = df_output.collect()

    assert len(rows) == 1

    assert rows[0]["vwap"] == 105.0
    assert rows[0]["moving_avg_5"] == 105.0
