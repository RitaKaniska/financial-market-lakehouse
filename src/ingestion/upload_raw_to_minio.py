from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from minio import Minio
from minio.error import S3Error


LOGGER = logging.getLogger("raw_ingestion")


@dataclass(frozen=True)
class IngestionConfig:
    source_dir: Path
    bucket_name: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    raw_prefix: str = "market_data"
    secure: bool = False


@dataclass(frozen=True)
class DiscoveredFile:
    source_path: Path
    symbol: str
    object_key: str


def load_env_file(env_path: Path = Path(".env")) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def load_config() -> IngestionConfig:
    source_dir = Path(os.getenv("RAW_SOURCE_DIR", "data"))
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    secure = endpoint.startswith("https://")
    return IngestionConfig(
        source_dir=source_dir,
        bucket_name=os.getenv("RAW_BUCKET_NAME", "raw-zone"),
        minio_endpoint=endpoint.replace("http://", "").replace("https://", ""),
        minio_access_key=os.getenv("MINIO_ROOT_USER", ""),
        minio_secret_key=os.getenv("MINIO_ROOT_PASSWORD", ""),
        secure=secure,
    )


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def discover_csv_files(source_dir: Path) -> list[Path]:
    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source_dir}")

    return sorted(source_dir.rglob("*.csv"))


def extract_symbol_from_path(file_path: Path) -> str:
    parent_symbol = file_path.parent.name.strip().upper()
    file_symbol = file_path.stem.strip().upper()

    if not parent_symbol and not file_symbol:
        raise ValueError(f"Unable to infer symbol from path: {file_path}")

    if parent_symbol and file_symbol and parent_symbol != file_symbol:
        LOGGER.warning(
            "Symbol mismatch detected; using parent directory symbol. "
            "parent_symbol=%s file_symbol=%s path=%s",
            parent_symbol,
            file_symbol,
            file_path,
        )

    return parent_symbol or file_symbol


def build_raw_object_key(raw_prefix: str, symbol: str, filename: str) -> str:
    return f"{raw_prefix}/symbol={symbol}/{filename}"


def prepare_ingestion_plan(
    source_files: Iterable[Path], raw_prefix: str
) -> list[DiscoveredFile]:
    plan: list[DiscoveredFile] = []

    for file_path in source_files:
        symbol = extract_symbol_from_path(file_path)
        object_key = build_raw_object_key(raw_prefix, symbol, file_path.name)
        plan.append(
            DiscoveredFile(
                source_path=file_path,
                symbol=symbol,
                object_key=object_key,
            )
        )

    return plan


def get_minio_client(config: IngestionConfig) -> Minio:
    return Minio(
        endpoint=config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        secure=config.secure,
    )


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    if client.bucket_exists(bucket_name):
        LOGGER.info("Bucket already exists: %s", bucket_name)
        return

    client.make_bucket(bucket_name)
    LOGGER.info("Created bucket: %s", bucket_name)


def upload_file_to_minio(client: Minio, bucket_name: str, item: DiscoveredFile) -> None:
    client.fput_object(
        bucket_name=bucket_name,
        object_name=item.object_key,
        file_path=str(item.source_path),
        content_type="text/csv",
    )
    LOGGER.info(
        "Uploaded file | bucket=%s symbol=%s source=%s target=%s",
        bucket_name,
        item.symbol,
        item.source_path,
        item.object_key,
    )


def main() -> None:
    configure_logging()
    load_env_file()
    config = load_config()

    LOGGER.info("Starting raw ingestion to MinIO")
    LOGGER.info("Source directory: %s", config.source_dir)
    LOGGER.info("Target bucket: %s", config.bucket_name)
    LOGGER.info("MinIO endpoint: %s", config.minio_endpoint)

    source_files = discover_csv_files(config.source_dir)
    ingestion_plan = prepare_ingestion_plan(source_files, config.raw_prefix)
    client = get_minio_client(config)

    ensure_bucket_exists(client, config.bucket_name)

    LOGGER.info("Discovered %s CSV files for raw ingestion", len(ingestion_plan))
    for item in ingestion_plan:
        upload_file_to_minio(client, config.bucket_name, item)
    LOGGER.info("Completed upload of %s files to bucket %s", len(ingestion_plan), config.bucket_name)


def run() -> int:
    try:
        main()
    except (FileNotFoundError, ValueError, S3Error) as exc:
        LOGGER.exception("Raw ingestion failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
