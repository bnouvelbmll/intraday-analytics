from __future__ import annotations

import datetime as dt

from basalt.analytics.l2 import L2AnalyticsConfig, L2AnalyticsTW, L2VolatilityConfig
import polars as pl


def _l2_series() -> pl.DataFrame:
    base = dt.datetime(2025, 1, 7, 9, 0, 0)
    seconds = [0, 10, 20, 30, 40, 50]
    prices = [100.0, 101.0, 99.0, 102.0, 98.0, 103.0]
    return pl.DataFrame(
        {
            "MIC": ["XPAR"] * len(seconds),
            "ListingId": [1] * len(seconds),
            "Ticker": ["ABC"] * len(seconds),
            "CurrencyCode": ["EUR"] * len(seconds),
            "TimeBucket": [base] * len(seconds),
            "EventTimestamp": [base + dt.timedelta(seconds=s) for s in seconds],
            "MarketState": ["CONTINUOUS_TRADING"] * len(seconds),
            "BidPrice1": prices,
            "AskPrice1": prices,
        }
    )


def _run(cfg: L2AnalyticsConfig, l2: pl.DataFrame) -> pl.DataFrame:
    analytics = L2AnalyticsTW(cfg)
    analytics.l2 = l2.lazy()
    return analytics.compute().collect()


def test_volatility_subsampling_changes_estimate():
    l2 = _l2_series()
    cfg_no_subsample = L2AnalyticsConfig(
        time_bucket_seconds=60,
        volatility=[L2VolatilityConfig(source="Mid", aggregations=["Std"])],
        liquidity=[],
        spreads=[],
        imbalances=[],
        ohlc=[],
    )
    cfg_subsample = L2AnalyticsConfig(
        time_bucket_seconds=60,
        volatility=[
            L2VolatilityConfig(
                source="Mid",
                aggregations=["Std"],
                subsample_seconds=20,
            )
        ],
        liquidity=[],
        spreads=[],
        imbalances=[],
        ohlc=[],
    )

    out_no_subsample = _run(cfg_no_subsample, l2)
    out_subsample = _run(cfg_subsample, l2)

    v0 = float(out_no_subsample["L2VolatilityMidStd"][0])
    v1 = float(out_subsample["L2VolatilityMidStd"][0])
    assert v1 != v0
    assert v1 < v0


def test_second_level_volatility_aggregates_columns_present():
    l2 = _l2_series()
    cfg = L2AnalyticsConfig(
        time_bucket_seconds=60,
        volatility=[
            L2VolatilityConfig(
                source="Mid",
                aggregations=["Std"],
                subsample_seconds=10,
                second_level_window_seconds=30,
                second_level_aggregations=[
                    "Min",
                    "Max",
                    "Mean",
                    "Median",
                    "Std",
                    "Last",
                    "TWMean",
                    "TWStd",
                ],
            )
        ],
        liquidity=[],
        spreads=[],
        imbalances=[],
        ohlc=[],
    )
    out = _run(cfg, l2)
    for col in (
        "L2VolatilityMidWin30sMin",
        "L2VolatilityMidWin30sMax",
        "L2VolatilityMidWin30sMean",
        "L2VolatilityMidWin30sMedian",
        "L2VolatilityMidWin30sStd",
        "L2VolatilityMidWin30sLast",
        "L2VolatilityMidWin30sTWMean",
        "L2VolatilityMidWin30sTWStd",
    ):
        assert col in out.columns

    mn = float(out["L2VolatilityMidWin30sMin"][0])
    mx = float(out["L2VolatilityMidWin30sMax"][0])
    mean = float(out["L2VolatilityMidWin30sMean"][0])
    std = float(out["L2VolatilityMidWin30sStd"][0])
    tw_std = float(out["L2VolatilityMidWin30sTWStd"][0])
    assert mn <= mean <= mx
    assert std >= 0.0
    assert tw_std >= 0.0
