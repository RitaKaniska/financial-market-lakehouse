import os

import pytest

from src.quality.check_curated_data import get_min_fact_row_threshold


def test_get_min_fact_row_threshold_defaults_to_100(monkeypatch):
    monkeypatch.delenv("MIN_CURATED_FACT_ROWS", raising=False)
    assert get_min_fact_row_threshold() == 100


def test_get_min_fact_row_threshold_reads_environment(monkeypatch):
    monkeypatch.setenv("MIN_CURATED_FACT_ROWS", "42")
    assert get_min_fact_row_threshold() == 42


def test_get_min_fact_row_threshold_rejects_invalid_value(monkeypatch):
    monkeypatch.setenv("MIN_CURATED_FACT_ROWS", "not-a-number")
    with pytest.raises(ValueError):
        get_min_fact_row_threshold()
