from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import streamlit as st


from dashboard.charts import (
    build_candlestick_chart,
    build_volume_by_hour_chart,
    build_vwap_close_chart,
)
from dashboard.queries import (
    compute_price_change_pct,
    compute_vwap_close_correlation,
    load_price_candles,
    load_symbol_date_bounds,
    load_volume_by_hour,
    load_vwap_close_series,
)
from src.utils.env import load_env_file


@st.cache_resource
def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    load_env_file()
    connection = duckdb.connect(database=":memory:")
    connection.execute("INSTALL httpfs;")
    connection.execute("LOAD httpfs;")
    connection.execute("SET s3_url_style='path';")
    connection.execute("SET s3_use_ssl=false;")
    connection.execute(
        f"SET s3_endpoint='{os.getenv('MINIO_ENDPOINT', 'localhost:9000').replace('http://', '').replace('https://', '')}';"
    )
    connection.execute(f"SET s3_access_key_id='{os.getenv('MINIO_ROOT_USER', 'minio_admin')}';")
    connection.execute(
        f"SET s3_secret_access_key='{os.getenv('MINIO_ROOT_PASSWORD', 'minio_password_secure')}';"
    )
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
def load_summary_metrics() -> tuple[int, int]:
    connection = get_duckdb_connection()
    fact_path = get_fact_trades_path()
    query = f"""
        SELECT COUNT(*) AS total_rows, COUNT(DISTINCT symbol) AS total_symbols
        FROM read_parquet('{fact_path}')
    """
    total_rows, total_symbols = connection.execute(query).fetchone()
    return total_rows, total_symbols


@st.cache_data(show_spinner=False)
def load_symbol_bounds(symbol: str) -> tuple[date | None, date | None]:
    connection = get_duckdb_connection()
    return load_symbol_date_bounds(connection, get_fact_trades_path(), symbol)


def render_sidebar(symbols: list[str]) -> tuple[str, date, date]:
    st.sidebar.header("Filters")
    selected_symbol = st.sidebar.selectbox("Symbol", symbols)

    min_date, max_date = load_symbol_bounds(selected_symbol)
    if min_date is None or max_date is None:
        st.sidebar.warning("No dated rows found for this symbol.")
        today = date.today()
        return selected_symbol, today, today

    default_start = max(min_date, max_date - timedelta(days=7))
    start_date, end_date = st.sidebar.date_input(
        "Date range",
        value=(default_start, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if isinstance(start_date, tuple):
        range_start, range_end = start_date
    else:
        range_start, range_end = start_date, end_date

    if range_start > range_end:
        st.sidebar.error("Start date must be on or before end date.")
        range_end = range_start

    return selected_symbol, range_start, range_end


def main() -> None:
    st.set_page_config(page_title="Market Data Dashboard", layout="wide")
    st.title("Financial Market Serving Layer")
    st.caption("DuckDB + Streamlit analytics on curated market data")

    try:
        total_rows, total_symbols = load_summary_metrics()
        symbols = get_available_symbols()
    except Exception as exc:
        st.error("Unable to load curated data from DuckDB.")
        st.exception(exc)
        st.info(
            "Run the Airflow DAG (`ingest_raw_data` -> `transform_market_data` -> "
            "`run_data_quality_checks`) before opening this dashboard."
        )
        return

    if not symbols:
        st.warning("Curated fact data is empty.")
        return

    metric_col_1, metric_col_2 = st.columns(2)
    metric_col_1.metric("Fact Rows", f"{total_rows:,}")
    metric_col_2.metric("Tracked Symbols", f"{total_symbols:,}")

    selected_symbol, start_date, end_date = render_sidebar(symbols)
    connection = get_duckdb_connection()
    fact_path = get_fact_trades_path()

    price_df = load_price_candles(connection, fact_path, selected_symbol, start_date, end_date)
    volume_df = load_volume_by_hour(connection, fact_path, selected_symbol, start_date, end_date)
    vwap_df = load_vwap_close_series(connection, fact_path, selected_symbol, start_date, end_date)

    price_change = compute_price_change_pct(price_df)
    vwap_correlation = compute_vwap_close_correlation(vwap_df)

    insight_col_1, insight_col_2, insight_col_3 = st.columns(3)
    insight_col_1.metric("Selected symbol", selected_symbol)
    insight_col_2.metric(
        "Price change",
        f"{price_change:.2f}%" if price_change is not None else "N/A",
    )
    insight_col_3.metric(
        "VWAP vs close correlation",
        f"{vwap_correlation:.3f}" if vwap_correlation is not None else "N/A",
    )

    st.subheader("1. Price movement over time")
    if price_df.empty:
        st.info("No price data for the selected filters.")
    else:
        st.plotly_chart(
            build_candlestick_chart(price_df, selected_symbol),
            use_container_width=True,
        )
        if vwap_correlation is not None and vwap_correlation > 0.9:
            st.caption("VWAP tracks close closely in this window, suggesting a stable intraday trend.")
        elif vwap_correlation is not None and vwap_correlation < 0.5:
            st.caption("VWAP and close diverge in this window, suggesting higher short-term volatility.")

    st.subheader("2. Volume distribution by hour")
    if volume_df.empty:
        st.info("No volume data for the selected filters.")
    else:
        peak_hour = int(volume_df.loc[volume_df["total_volume"].idxmax(), "hour_of_day"])
        st.plotly_chart(
            build_volume_by_hour_chart(volume_df, selected_symbol),
            use_container_width=True,
        )
        st.caption(f"Peak trading hour in this range: **{peak_hour:02d}:00** UTC.")

    st.subheader("3. VWAP vs close price")
    if vwap_df.empty:
        st.info("No VWAP data for the selected filters.")
    else:
        st.plotly_chart(
            build_vwap_close_chart(vwap_df, selected_symbol, vwap_correlation),
            use_container_width=True,
        )

    with st.expander("Curated data preview (latest 200 rows)"):
        preview_query = f"""
            SELECT *
            FROM read_parquet('{fact_path}')
            WHERE symbol = ?
              AND date >= ?
              AND date <= ?
            ORDER BY event_timestamp DESC
            LIMIT 200
        """
        preview_df = connection.execute(
            preview_query,
            [selected_symbol, start_date, end_date],
        ).df()
        st.dataframe(preview_df, use_container_width=True)


if __name__ == "__main__":
    main()
