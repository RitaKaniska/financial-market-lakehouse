from __future__ import annotations

import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from src.quality.staging import get_staging_root
from src.utils.env import load_env_file
from src.utils.spark_helper import configure_partition_overwrite, get_spark_session


LOGGER = logging.getLogger("market_transform")


RAW_MARKET_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("open", StringType(), True),
        StructField("high", StringType(), True),
        StructField("low", StringType(), True),
        StructField("close", StringType(), True),
        StructField("volume", StringType(), True),
        StructField("close_time", StringType(), True),
        StructField("quote_asset_volume", StringType(), True),
        StructField("number_of_trades", LongType(), True),
        StructField("taker_buy_base_asset_volume", StringType(), True),
        StructField("taker_buy_quote_asset_volume", StringType(), True),
        StructField("ignore", StringType(), True),
    ]
)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def cleanse_market_data(df: DataFrame) -> DataFrame:
    cleansed_df = (
        df.withColumn("event_timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("close_timestamp", F.to_timestamp("close_time", "yyyy-MM-dd HH:mm:ss.SSS"))
        .withColumn("open_price", F.col("open").cast(DoubleType()))
        .withColumn("high_price", F.col("high").cast(DoubleType()))
        .withColumn("low_price", F.col("low").cast(DoubleType()))
        .withColumn("close_price", F.col("close").cast(DoubleType()))
        .withColumn("volume", F.col("volume").cast(DoubleType()))
        .withColumn("quote_asset_volume", F.col("quote_asset_volume").cast(DoubleType()))
        .withColumn(
            "taker_buy_base_asset_volume",
            F.col("taker_buy_base_asset_volume").cast(DoubleType()),
        )
        .withColumn(
            "taker_buy_quote_asset_volume",
            F.col("taker_buy_quote_asset_volume").cast(DoubleType()),
        )
        .withColumn("date", F.to_date("event_timestamp"))
        .dropDuplicates(["symbol", "event_timestamp"])
        .filter(F.col("event_timestamp").isNotNull())
        .filter(F.col("symbol").isNotNull())
        .filter(F.col("open_price") > 0)
        .filter(F.col("high_price") > 0)
        .filter(F.col("low_price") > 0)
        .filter(F.col("close_price") > 0)
        .filter(F.col("volume") >= 0)
        .filter(F.col("quote_asset_volume") >= 0)
    )

    return cleansed_df.select(
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
    )


def add_window_metrics(df: DataFrame) -> DataFrame:
    ordered_window = (
        Window.partitionBy("symbol", "date")
        .orderBy("event_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    moving_avg_window = (
        Window.partitionBy("symbol", "date")
        .orderBy("event_timestamp")
        .rowsBetween(-4, 0)
    )

    return (
        df.withColumn("cum_quote_asset_volume", F.sum("quote_asset_volume").over(ordered_window))
        .withColumn("cum_volume", F.sum("volume").over(ordered_window))
        .withColumn(
            "vwap",
            F.when(F.col("cum_volume") > 0, F.col("cum_quote_asset_volume") / F.col("cum_volume")),
        )
        .withColumn("moving_avg_5", F.avg("close_price").over(moving_avg_window))
        .drop("cum_quote_asset_volume", "cum_volume")
    )


def build_fact_trades(df: DataFrame) -> DataFrame:
    return df.select(
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
        "vwap",
        "moving_avg_5",
    )


def build_dim_symbol(df: DataFrame) -> DataFrame:
    return (
        df.select("symbol")
        .distinct()
        .withColumn(
            "quote_asset",
            F.when(F.col("symbol").endswith("USDT"), F.lit("USDT"))
            .when(F.col("symbol").endswith("BUSD"), F.lit("BUSD"))
            .when(F.col("symbol").endswith("BTC"), F.lit("BTC"))
            .when(F.col("symbol").endswith("ETH"), F.lit("ETH"))
            .when(F.col("symbol").endswith("BNB"), F.lit("BNB"))
            .when(F.col("symbol").endswith("USD"), F.lit("USD"))
            .otherwise(F.lit("UNKNOWN")),
        )
        .withColumn(
            "base_asset",
            F.when(
                F.col("quote_asset") != "UNKNOWN",
                F.expr("substring(symbol, 1, length(symbol) - length(quote_asset))"),
            ).otherwise(F.col("symbol")),
        )
        .orderBy("symbol")
    )


def build_dim_time(df: DataFrame) -> DataFrame:
    return (
        df.select("event_timestamp", "date")
        .distinct()
        .withColumn("year", F.year("event_timestamp"))
        .withColumn("month", F.month("event_timestamp"))
        .withColumn("day", F.dayofmonth("event_timestamp"))
        .withColumn("hour", F.hour("event_timestamp"))
        .withColumn("minute", F.minute("event_timestamp"))
        .withColumn("weekday", F.date_format("event_timestamp", "E"))
        .orderBy("event_timestamp")
    )


def read_raw_market_data(spark, raw_path: str) -> DataFrame:
    return spark.read.option("header", "true").schema(RAW_MARKET_SCHEMA).csv(raw_path)


def write_staging_outputs(
    spark,
    fact_trades_df: DataFrame,
    dim_symbol_df: DataFrame,
    dim_time_df: DataFrame,
    staging_root: str,
) -> None:
    configure_partition_overwrite(spark)
    (
        fact_trades_df.write.mode("overwrite")
        .partitionBy("date")
        .parquet(f"{staging_root}/fact_trades")
    )
    dim_symbol_df.write.mode("overwrite").parquet(f"{staging_root}/dim_symbol")
    dim_time_df.write.mode("overwrite").parquet(f"{staging_root}/dim_time")


def run_job(
    raw_path: str | None = None,
    staging_root: str | None = None,
) -> None:
    configure_logging()
    load_env_file(Path(".env"))
    resolved_raw_path = raw_path or os.getenv("RAW_MARKET_PATH", "s3a://raw-zone/market_data")
    resolved_staging_root = staging_root or get_staging_root()

    LOGGER.info("Starting Spark transformation job")
    LOGGER.info("Resolved raw path: %s", resolved_raw_path)
    LOGGER.info("Resolved staging root: %s", resolved_staging_root)

    spark = get_spark_session("financial-market-curation")

    raw_df = read_raw_market_data(spark, resolved_raw_path)
    if "symbol" not in raw_df.columns:
        raw_df = raw_df.withColumn("symbol", F.lit(None).cast(StringType()))

    raw_count = raw_df.count()
    LOGGER.info("Loaded raw rows: %s", raw_count)

    cleansed_df = cleanse_market_data(raw_df)
    enriched_df = add_window_metrics(cleansed_df)

    fact_trades_df = build_fact_trades(enriched_df)
    dim_symbol_df = build_dim_symbol(enriched_df)
    dim_time_df = build_dim_time(enriched_df)

    fact_count = fact_trades_df.count()
    dim_symbol_count = dim_symbol_df.count()
    dim_time_count = dim_time_df.count()

    LOGGER.info(
        "Transformation summary | fact_trades=%s dim_symbol=%s dim_time=%s",
        fact_count,
        dim_symbol_count,
        dim_time_count,
    )

    write_staging_outputs(
        spark,
        fact_trades_df,
        dim_symbol_df,
        dim_time_df,
        resolved_staging_root,
    )
    LOGGER.info("Staging outputs written successfully; awaiting data quality promotion")
    spark.stop()


if __name__ == "__main__":
    run_job()
