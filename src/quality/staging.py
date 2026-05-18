from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession


LOGGER = logging.getLogger("curated_staging")


def get_staging_root(staging_root: str | None = None) -> str:
    import os
    return staging_root or os.getenv("CURATED_STAGING_ROOT", "s3a://curated-zone/staging")


def get_curated_root(curated_root: str | None = None) -> str:
    import os
    return curated_root or os.getenv("CURATED_ROOT_PATH", "s3a://curated-zone")


def configure_partition_overwrite(spark: SparkSession) -> None:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def read_staging_datasets(spark: SparkSession, staging_root: str) -> tuple[DataFrame, DataFrame, DataFrame]:
    fact_trades_df = spark.read.parquet(f"{staging_root}/fact_trades")
    dim_symbol_df = spark.read.parquet(f"{staging_root}/dim_symbol")
    dim_time_df = spark.read.parquet(f"{staging_root}/dim_time")
    return fact_trades_df, dim_symbol_df, dim_time_df


def promote_staging_to_curated(
    spark: SparkSession,
    staging_root: str | None = None,
    curated_root: str | None = None,
) -> None:
    resolved_staging_root = get_staging_root(staging_root)
    resolved_curated_root = get_curated_root(curated_root)
    configure_partition_overwrite(spark)

    fact_trades_df, dim_symbol_df, dim_time_df = read_staging_datasets(
        spark,
        resolved_staging_root,
    )

    (
        fact_trades_df.write.mode("overwrite")
        .partitionBy("date")
        .parquet(f"{resolved_curated_root}/fact_trades")
    )
    dim_symbol_df.write.mode("overwrite").parquet(f"{resolved_curated_root}/dim_symbol")
    dim_time_df.write.mode("overwrite").parquet(f"{resolved_curated_root}/dim_time")

    LOGGER.info(
        "Promoted staging datasets to curated zone | staging=%s curated=%s",
        resolved_staging_root,
        resolved_curated_root,
    )
