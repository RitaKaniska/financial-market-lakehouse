from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd


def load_price_candles(
    connection: duckdb.DuckDBPyConnection,
    fact_path: str,
    symbol: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    query = f"""
        SELECT
            event_timestamp,
            open_price,
            high_price,
            low_price,
            close_price
        FROM read_parquet('{fact_path}')
        WHERE symbol = ?
          AND date >= ?
          AND date <= ?
        ORDER BY event_timestamp
    """
    return connection.execute(query, [symbol, start_date, end_date]).df()


def load_volume_by_hour(
    connection: duckdb.DuckDBPyConnection,
    fact_path: str,
    symbol: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    query = f"""
        SELECT
            EXTRACT(hour FROM event_timestamp)::INTEGER AS hour_of_day,
            SUM(volume) AS total_volume
        FROM read_parquet('{fact_path}')
        WHERE symbol = ?
          AND date >= ?
          AND date <= ?
        GROUP BY 1
        ORDER BY 1
    """
    return connection.execute(query, [symbol, start_date, end_date]).df()


def load_vwap_close_series(
    connection: duckdb.DuckDBPyConnection,
    fact_path: str,
    symbol: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    query = f"""
        SELECT
            event_timestamp,
            close_price,
            vwap
        FROM read_parquet('{fact_path}')
        WHERE symbol = ?
          AND date >= ?
          AND date <= ?
          AND vwap IS NOT NULL
        ORDER BY event_timestamp
    """
    return connection.execute(query, [symbol, start_date, end_date]).df()


def load_symbol_date_bounds(
    connection: duckdb.DuckDBPyConnection,
    fact_path: str,
    symbol: str,
) -> tuple[date | None, date | None]:
    query = f"""
        SELECT MIN(date), MAX(date)
        FROM read_parquet('{fact_path}')
        WHERE symbol = ?
    """
    min_date, max_date = connection.execute(query, [symbol]).fetchone()
    return min_date, max_date


def compute_price_change_pct(series: pd.DataFrame) -> float | None:
    if series.empty:
        return None
    first_close = series["close_price"].iloc[0]
    last_close = series["close_price"].iloc[-1]
    if first_close == 0:
        return None
    return ((last_close - first_close) / first_close) * 100


def compute_vwap_close_correlation(series: pd.DataFrame) -> float | None:
    if series.empty or len(series) < 2:
        return None
    return float(series["close_price"].corr(series["vwap"]))
