from __future__ import annotations

import datetime as dt

import polars as pl

from basalt.analytics.trade import TradeAnalytics, TradeAnalyticsConfig, TradeGenericConfig
from basalt.schema_utils import _apply_docs


def _run_trade_vwap(df: pl.DataFrame) -> float:
    cfg = TradeAnalyticsConfig(
        generic_metrics=[TradeGenericConfig(measures=["VWAP"], sides=["Total"])],
        discrepancy_metrics=[],
        flag_metrics=[],
        change_metrics=[],
        impact_metrics=[],
    )
    analytics = TradeAnalytics(cfg)
    analytics.trades = df.lazy()
    out = analytics.compute().collect()
    return float(out["TradeTotalVWAP"][0])


def test_vwap_is_size_weighted_not_arithmetic_mean():
    df = pl.DataFrame(
        {
            "MIC": ["XPAR", "XPAR"],
            "ListingId": [1, 1],
            "Ticker": ["ABC", "ABC"],
            "TimeBucket": [dt.datetime(2025, 1, 7, 9, 0), dt.datetime(2025, 1, 7, 9, 0)],
            "Classification": ["LIT_CONTINUOUS", "LIT_CONTINUOUS"],
            "AggressorSide": [1, 1],
            "MarketState": ["CONTINUOUS_TRADING", "CONTINUOUS_TRADING"],
            "LocalPrice": [1.0, 10.0],
            "Size": [1.0, 10.0],
        }
    )
    vwap = _run_trade_vwap(df)
    expected_weighted = (1.0 * 1.0 + 10.0 * 10.0) / (1.0 + 10.0)
    arithmetic_mean = (1.0 + 10.0) / 2.0
    assert abs(vwap - expected_weighted) < 1e-12
    assert abs(vwap - arithmetic_mean) > 1e-6


def test_vwap_docs_state_weighted_formula():
    docs = _apply_docs("trade", "VWAP")
    definition = str(docs.get("definition") or "")
    description = str(docs.get("description") or "")
    assert "sum(LocalPrice*Size)/sum(Size)" in definition
    assert "arithmetic mean" in description
