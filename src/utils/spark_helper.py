from __future__ import annotations

import os

from pyspark.sql import SparkSession

from src.utils.env import load_env_file


def get_spark_session(app_name: str = "DataTransformation") -> SparkSession:
    load_env_file()

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minio_admin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio_password_secure")

    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def configure_partition_overwrite(spark: SparkSession) -> None:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
