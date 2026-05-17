from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st


def load_env_file(env_path: Path = Path(".env")) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


@st.cache_resource
def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    load_env_file()
    connection = duckdb.connect(database=":memory:")
    connection.execute("INSTALL httpfs;")
    connection.execute("LOAD httpfs;")
    connection.execute("SET s3_url_style='path';")
    connection.execute("SET s3_use_ssl=false;")
    connection.execute(f"SET s3_endpoint='{os.getenv('MINIO_ENDPOINT', 'localhost:9000').replace('http://', '').replace('https://', '')}';")
    connection.execute(f"SET s3_access_key_id='{os.getenv('MINIO_ROOT_USER', 'minio_admin')}';")
    connection.execute(f"SET s3_secret_access_key='{os.getenv('MINIO_ROOT_PASSWORD', 'minio_password_secure')}';")
    return connection


def get_fact_trades_path() -> str:
    return os.getenv("CURATED_FACT_TRADES_PATH", "s3://curated-zone/fact_trades/*/*.parquet")


@st.cache_data(show_spinner=False)
def get_available_symbols() -> list[str]:
    connection = get_duckdb_connection()
    fact_path = get_fact_trades_path()
    query = f"""
        SELECT DISTINCT symbol
        FROM read_parquet('{fact_path}')
        WHERE symbol IS NOT NULL
        ORDER BY symbol
    """
    rows = connection.execute(query).fetchall()
    return [row[0] for row in rows]


@st.cache_data(show_spinner=False)
def load_fact_preview(selected_symbol: str) -> pd.DataFrame:
    connection = get_duckdb_connection()
    fact_path = get_fact_trades_path()

    if selected_symbol == "All":
        query = f"""
            SELECT *
            FROM read_parquet('{fact_path}')
            ORDER BY event_timestamp DESC
            LIMIT 200
        """
        return connection.execute(query).df()

    query = f"""
        SELECT *
        FROM read_parquet('{fact_path}')
        WHERE symbol = ?
        ORDER BY event_timestamp DESC
        LIMIT 200
    """
    return connection.execute(query, [selected_symbol]).df()


@st.cache_data(show_spinner=False)
def load_summary_metrics() -> tuple[int, int]:
    connection = get_duckdb_connection()
    fact_path = get_fact_trades_path()
    query = f"""
        SELECT COUNT(*) AS total_rows, COUNT(DISTINCT symbol) AS total_symbols
        FROM read_parquet('{fact_path}')
    """
    total_rows, total_symbols = connection.execute(query).fetchone()
    return total_rows, total_symbols


def main() -> None:
    st.set_page_config(page_title="Market Data Dashboard", layout="wide")
    st.title("Financial Market Serving Layer")
    st.caption("DuckDB + Streamlit preview for curated market data")

    try:
        total_rows, total_symbols = load_summary_metrics()
        symbols = get_available_symbols()
    except Exception as exc:
        st.error("Unable to load curated data from DuckDB.")
        st.exception(exc)
        return

    metric_col_1, metric_col_2 = st.columns(2)
    metric_col_1.metric("Fact Rows", f"{total_rows:,}")
    metric_col_2.metric("Tracked Symbols", f"{total_symbols:,}")

    symbol_options = ["All", *symbols]
    selected_symbol = st.selectbox("Select a symbol", symbol_options)

    preview_df = load_fact_preview(selected_symbol)
    st.subheader("Curated Data Preview")
    st.dataframe(preview_df, use_container_width=True)


if __name__ == "__main__":
    main()
