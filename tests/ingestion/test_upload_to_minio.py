from pathlib import Path

import pytest

from src.ingestion.upload_raw_to_minio import (
    DiscoveredFile,
    IngestionConfig,
    UploadResult,
    build_raw_object_key,
    discover_csv_files,
    ensure_bucket_exists,
    extract_symbol_from_path,
    load_config,
    prepare_ingestion_plan,
    process_ingestion_plan,
    validate_csv_schema,
    validate_source_file,
)
from src.utils.env import load_env_file


# =========================================================
# Fixtures / Helpers
# =========================================================

class MockMinioClient:
    def __init__(self):
        self.buckets = set()
        self.uploaded_objects = []

    def bucket_exists(self, bucket_name):
        return bucket_name in self.buckets

    def make_bucket(self, bucket_name):
        self.buckets.add(bucket_name)

    def fput_object(
        self,
        bucket_name,
        object_name,
        file_path,
        content_type,
    ):
        self.uploaded_objects.append(
            {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "file_path": file_path,
                "content_type": content_type,
            }
        )


VALID_CSV_HEADER = (
    "timestamp,open,high,low,close,volume,close_time,quote_asset_volume\n"
    "2024-01-01 00:00:00,1,2,1,1.5,10,2024-01-01 00:00:59.999,15\n"
)


class FailingMinioClient(MockMinioClient):
    def fput_object(
        self,
        bucket_name,
        object_name,
        file_path,
        content_type,
    ):
        raise RuntimeError("Upload failed")


# =========================================================
# load_env_file
# =========================================================

def test_load_env_file_loads_environment_variables(
    tmp_path,
    monkeypatch,
):
    env_file = tmp_path / ".env"

    env_file.write_text(
        "\n".join(
            [
                "RAW_BUCKET_NAME=test-bucket",
                "MINIO_ENDPOINT=http://localhost:9000",
            ]
        )
    )

    monkeypatch.delenv("RAW_BUCKET_NAME", raising=False)
    monkeypatch.delenv("MINIO_ENDPOINT", raising=False)

    load_env_file(env_file)

    assert load_config().bucket_name == "test-bucket"
    assert load_config().minio_endpoint == "localhost:9000"


# =========================================================
# discover_csv_files
# =========================================================

def test_discover_csv_files_returns_all_csv_files(
    tmp_path,
):
    btc_dir = tmp_path / "BTCUSDT"
    eth_dir = tmp_path / "ETHUSDT"

    btc_dir.mkdir()
    eth_dir.mkdir()

    btc_file = btc_dir / "btc.csv"
    eth_file = eth_dir / "eth.csv"

    btc_file.write_text("sample")
    eth_file.write_text("sample")

    files = discover_csv_files(tmp_path)

    assert len(files) == 2
    assert btc_file in files
    assert eth_file in files


def test_discover_csv_files_raises_when_directory_missing():
    missing_dir = Path("non_existing_directory")

    with pytest.raises(FileNotFoundError):
        discover_csv_files(missing_dir)


# =========================================================
# extract_symbol_from_path
# =========================================================

def test_extract_symbol_from_path_uses_parent_directory():
    file_path = Path("data/BTCUSDT/sample.csv")

    symbol = extract_symbol_from_path(file_path)

    assert symbol == "BTCUSDT"


def test_extract_symbol_from_path_falls_back_to_filename():
    file_path = Path("BTCUSDT.csv")

    symbol = extract_symbol_from_path(file_path)

    assert symbol == "BTCUSDT"


# =========================================================
# build_raw_object_key
# =========================================================

def test_build_raw_object_key_builds_partitioned_path():
    object_key = build_raw_object_key(
        "market_data",
        "BTCUSDT",
        "sample.csv",
    )

    assert (
        object_key
        == "market_data/symbol=BTCUSDT/sample.csv"
    )


# =========================================================
# prepare_ingestion_plan
# =========================================================

def test_prepare_ingestion_plan_creates_discovered_files(
    tmp_path,
):
    btc_dir = tmp_path / "BTCUSDT"
    btc_dir.mkdir()

    file_path = btc_dir / "data.csv"
    file_path.write_text("sample")

    plan = prepare_ingestion_plan(
        [file_path],
        "market_data",
    )

    assert len(plan) == 1

    item = plan[0]

    assert item.symbol == "BTCUSDT"

    assert (
        item.object_key
        == "market_data/symbol=BTCUSDT/data.csv"
    )

    assert item.source_path == file_path


# =========================================================
# validate_source_file
# =========================================================

def test_validate_source_file_accepts_valid_file(
    tmp_path,
):
    file_path = tmp_path / "sample.csv"

    file_path.write_text(VALID_CSV_HEADER, encoding="utf-8")

    validate_source_file(file_path)


def test_validate_source_file_raises_for_empty_file(
    tmp_path,
):
    file_path = tmp_path / "empty.csv"

    file_path.write_text("")

    with pytest.raises(ValueError):
        validate_source_file(file_path)


def test_validate_source_file_raises_for_missing_file():
    with pytest.raises(FileNotFoundError):
        validate_source_file(Path("missing.csv"))


def test_validate_csv_schema_accepts_valid_header(tmp_path):
    csv_file = tmp_path / "BTCUSDT.csv"
    csv_file.write_text(
        "timestamp,open,high,low,close,volume,close_time,quote_asset_volume\n",
        encoding="utf-8",
    )
    validate_csv_schema(csv_file)


def test_validate_csv_schema_raises_for_missing_columns(tmp_path):
    csv_file = tmp_path / "BTCUSDT.csv"
    csv_file.write_text("timestamp,open,close\n", encoding="utf-8")

    with pytest.raises(ValueError, match="missing columns"):
        validate_csv_schema(csv_file)


# =========================================================
# ensure_bucket_exists
# =========================================================

def test_ensure_bucket_exists_creates_bucket():
    client = MockMinioClient()

    ensure_bucket_exists(client, "raw-zone")

    assert "raw-zone" in client.buckets


def test_ensure_bucket_exists_skips_existing_bucket():
    client = MockMinioClient()

    client.buckets.add("raw-zone")

    ensure_bucket_exists(client, "raw-zone")

    assert len(client.buckets) == 1


# =========================================================
# process_ingestion_plan
# =========================================================

def test_process_ingestion_plan_uploads_files_successfully(
    tmp_path,
):
    file_path = tmp_path / "sample.csv"

    file_path.write_text(VALID_CSV_HEADER, encoding="utf-8")

    item = DiscoveredFile(
        source_path=file_path,
        symbol="BTCUSDT",
        object_key="market_data/symbol=BTCUSDT/sample.csv",
    )

    client = MockMinioClient()

    results = process_ingestion_plan(
        client,
        "raw-zone",
        [item],
    )

    assert len(results) == 1

    result = results[0]

    assert result.status == "success"

    assert len(client.uploaded_objects) == 1

    uploaded = client.uploaded_objects[0]

    assert uploaded["bucket_name"] == "raw-zone"

    assert (
        uploaded["object_name"]
        == "market_data/symbol=BTCUSDT/sample.csv"
    )


def test_process_ingestion_plan_handles_failed_uploads(
    tmp_path,
):
    file_path = tmp_path / "sample.csv"

    file_path.write_text(VALID_CSV_HEADER, encoding="utf-8")

    item = DiscoveredFile(
        source_path=file_path,
        symbol="BTCUSDT",
        object_key="market_data/symbol=BTCUSDT/sample.csv",
    )

    client = FailingMinioClient()

    results = process_ingestion_plan(
        client,
        "raw-zone",
        [item],
    )

    assert len(results) == 1

    result = results[0]

    assert result.status == "failed"

    assert "Upload failed" in result.message


# =========================================================
# load_config
# =========================================================

def test_load_config_reads_environment_variables(
    monkeypatch,
):
    monkeypatch.setenv(
        "RAW_SOURCE_DIR",
        "data",
    )

    monkeypatch.setenv(
        "RAW_BUCKET_NAME",
        "crypto-raw",
    )

    monkeypatch.setenv(
        "MINIO_ENDPOINT",
        "http://localhost:9000",
    )

    monkeypatch.setenv(
        "MINIO_ROOT_USER",
        "admin",
    )

    monkeypatch.setenv(
        "MINIO_ROOT_PASSWORD",
        "password",
    )

    config = load_config()

    assert isinstance(config, IngestionConfig)

    assert config.bucket_name == "crypto-raw"

    assert config.minio_endpoint == "localhost:9000"

    assert config.minio_access_key == "admin"

    assert config.minio_secret_key == "password"

    assert config.secure is False