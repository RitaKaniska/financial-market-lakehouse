from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def build_candlestick_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    figure = go.Figure(
        data=[
            go.Candlestick(
                x=df["event_timestamp"],
                open=df["open_price"],
                high=df["high_price"],
                low=df["low_price"],
                close=df["close_price"],
                name=symbol,
            )
        ]
    )
    figure.update_layout(
        title=f"{symbol} price candles",
        xaxis_title="Time",
        yaxis_title="Price",
        height=420,
        margin=dict(l=40, r=20, t=60, b=40),
        xaxis_rangeslider_visible=False,
    )
    return figure


def build_volume_by_hour_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    figure = go.Figure(
        data=[
            go.Bar(
                x=df["hour_of_day"],
                y=df["total_volume"],
                marker_color="#2563eb",
                name="Volume",
            )
        ]
    )
    figure.update_layout(
        title=f"{symbol} volume by hour of day",
        xaxis_title="Hour (0-23)",
        yaxis_title="Total volume",
        height=380,
        margin=dict(l=40, r=20, t=60, b=40),
        bargap=0.2,
    )
    return figure


def build_vwap_close_chart(df: pd.DataFrame, symbol: str, correlation: float | None) -> go.Figure:
    title = f"{symbol} VWAP vs close"
    if correlation is not None:
        title = f"{title} (corr={correlation:.3f})"

    figure = make_subplots(specs=[[{"secondary_y": True}]])
    figure.add_trace(
        go.Scatter(
            x=df["event_timestamp"],
            y=df["close_price"],
            mode="lines",
            name="Close",
            line=dict(color="#0f766e", width=1.5),
        ),
        secondary_y=False,
    )
    figure.add_trace(
        go.Scatter(
            x=df["event_timestamp"],
            y=df["vwap"],
            mode="lines",
            name="VWAP",
            line=dict(color="#dc2626", width=1.5),
        ),
        secondary_y=False,
    )
    figure.update_layout(
        title=title,
        xaxis_title="Time",
        height=420,
        margin=dict(l=40, r=20, t=60, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    figure.update_yaxes(title_text="Price", secondary_y=False)
    return figure
