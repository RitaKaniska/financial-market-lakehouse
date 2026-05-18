from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.quality.staging import get_staging_root, promote_staging_to_curated, read_staging_datasets
from src.utils.env import load_env_file
from src.utils.spark_helper import get_spark_session


LOGGER = logging.getLogger("curated_data_quality")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def get_min_fact_row_threshold() -> int:
    import os
    return int(os.getenv("MIN_CURATED_FACT_ROWS", "100"))


def check_fact_trades_has_rows(fact_trades_df: DataFrame, min_rows: int) -> None:
    row_count = fact_trades_df.count()
    if row_count < min_rows:
        raise ValueError(
            f"fact_trades row count {row_count} is below minimum threshold {min_rows}"
        )
    LOGGER.info("DQ passed | fact_trades row count = %s (min=%s)", row_count, min_rows)


def check_symbol_not_null(fact_trades_df: DataFrame) -> None:
    null_count = fact_trades_df.filter(F.col("symbol").isNull()).count()
    if null_count > 0:
        raise ValueError(f"fact_trades contains {null_count} rows with null symbol")
    LOGGER.info("DQ passed | symbol is non-null")


def check_close_price_positive(fact_trades_df: DataFrame) -> None:
    invalid_count = fact_trades_df.filter(F.col("close_price") <= 0).count()
    if invalid_count > 0:
        raise ValueError(f"fact_trades contains {invalid_count} rows with non-positive close_price")
    LOGGER.info("DQ passed | close_price is positive")


def check_event_timestamp_not_null(fact_trades_df: DataFrame) -> None:
    null_count = fact_trades_df.filter(F.col("event_timestamp").isNull()).count()
    if null_count > 0:
        raise ValueError(f"fact_trades contains {null_count} rows with null event_timestamp")
    LOGGER.info("DQ passed | event_timestamp is non-null")


def check_symbol_referential_integrity(fact_trades_df: DataFrame, dim_symbol_df: DataFrame) -> None:
    missing_symbols_df = fact_trades_df.select("symbol").distinct().join(
        dim_symbol_df.select("symbol").distinct(),
        on="symbol",
        how="left_anti",
    )
    missing_count = missing_symbols_df.count()
    if missing_count > 0:
        raise ValueError(f"dim_symbol is missing {missing_count} symbols referenced by fact_trades")
    LOGGER.info("DQ passed | dim_symbol covers all fact_trades symbols")


def check_unique_symbol_timestamp_pairs(fact_trades_df: DataFrame) -> None:
    duplicate_count = (
        fact_trades_df.groupBy("symbol", "event_timestamp")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_count > 0:
        raise ValueError(
            f"fact_trades contains {duplicate_count} duplicate symbol/event_timestamp pairs"
        )
    LOGGER.info("DQ passed | symbol and event_timestamp pairs are unique")


def run_checks(fact_trades_df: DataFrame, dim_symbol_df: DataFrame) -> None:
    min_rows = get_min_fact_row_threshold()
    checks = [
        lambda: check_fact_trades_has_rows(fact_trades_df, min_rows),
        lambda: check_symbol_not_null(fact_trades_df),
        lambda: check_close_price_positive(fact_trades_df),
        lambda: check_event_timestamp_not_null(fact_trades_df),
        lambda: check_unique_symbol_timestamp_pairs(fact_trades_df),
        lambda: check_symbol_referential_integrity(fact_trades_df, dim_symbol_df),
    ]

    passed_checks = 0
    for check in checks:
        check()
        passed_checks += 1

    LOGGER.info(
        "DQ summary | total_checks=%s passed_checks=%s failed_checks=%s",
        len(checks),
        passed_checks,
        len(checks) - passed_checks,
    )


def run(staging_root: str | None = None) -> int:
    configure_logging()
    load_env_file(Path(".env"))
    resolved_staging_root = get_staging_root(staging_root)
    LOGGER.info("Starting curated data quality checks against staging zone")
    LOGGER.info("Resolved staging root: %s", resolved_staging_root)
    spark = get_spark_session("financial-market-dq")

    try:
        fact_trades_df, dim_symbol_df, _ = read_staging_datasets(spark, resolved_staging_root)
        run_checks(fact_trades_df, dim_symbol_df)
        promote_staging_to_curated(spark)
        LOGGER.info("Curated data quality checks completed successfully; curated zone updated")
        return 0
    except Exception as exc:
        LOGGER.exception(
            "Curated data quality checks failed; curated zone was not updated: %s",
            exc,
        )
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(run())
