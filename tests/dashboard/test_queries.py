import pandas as pd

from dashboard.queries import compute_price_change_pct, compute_vwap_close_correlation


def test_compute_price_change_pct_returns_percent_change():
    series = pd.DataFrame({"close_price": [100.0, 110.0]})
    assert compute_price_change_pct(series) == 10.0


def test_compute_price_change_pct_returns_none_for_empty_frame():
    series = pd.DataFrame({"close_price": []})
    assert compute_price_change_pct(series) is None


def test_compute_vwap_close_correlation_returns_positive_value():
    series = pd.DataFrame(
        {
            "close_price": [1.0, 2.0, 3.0, 4.0],
            "vwap": [1.1, 2.1, 3.1, 4.1],
        }
    )
    correlation = compute_vwap_close_correlation(series)
    assert correlation is not None
    assert correlation > 0.99


def test_compute_vwap_close_correlation_returns_none_for_single_row():
    series = pd.DataFrame({"close_price": [1.0], "vwap": [1.0]})
    assert compute_vwap_close_correlation(series) is None
