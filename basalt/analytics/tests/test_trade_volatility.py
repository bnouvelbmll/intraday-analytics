from __future__ import annotations

import datetime as dt
import numpy as np
import polars as pl

from basalt.analytics.trade import TradeAnalytics, TradeAnalyticsConfig, TradeGenericConfig
from basalt.configuration import PassConfig


def _run_trade(cfg: TradeAnalyticsConfig, trades: pl.DataFrame) -> pl.DataFrame:
    analytics = TradeAnalytics(cfg)
    analytics.trades = trades.lazy()
    return analytics.compute().collect()


def test_trade_volatility_formula_annualized_std_log_returns():
    prices = [100.0, 101.0, 100.0, 101.0, 100.0]
    ts0 = dt.datetime(2025, 1, 7, 9, 0, 0)
    trades = pl.DataFrame(
        {
            "MIC": ["XPAR"] * len(prices),
            "ListingId": [1] * len(prices),
            "Ticker": ["ABC"] * len(prices),
            "TimeBucket": [ts0] * len(prices),
            "Classification": ["LIT_CONTINUOUS"] * len(prices),
            "AggressorSide": [1] * len(prices),
            "MarketState": ["CONTINUOUS_TRADING"] * len(prices),
            "LocalPrice": prices,
            "Size": [1.0] * len(prices),
        }
    )
    cfg = TradeAnalyticsConfig(
        time_bucket_seconds=60,
        generic_metrics=[TradeGenericConfig(measures=["Volatility"], sides=["Total"])],
        discrepancy_metrics=[],
        flag_metrics=[],
        change_metrics=[],
        impact_metrics=[],
    )
    out = _run_trade(cfg, trades)
    col = "TradeTotalVolatility"
    assert col in out.columns

    log_rets = np.diff(np.log(np.asarray(prices)))
    std_ret = np.std(log_rets, ddof=1)
    expected = std_ret * np.sqrt((252 * 24 * 60 * 60) * len(log_rets) / 60.0)
    assert abs(float(out[col][0]) - expected) < 1e-9


def test_trade_volatility_attribute_filters():
    ts0 = dt.datetime(2025, 1, 7, 9, 0, 0)
    trades = pl.DataFrame(
        {
            "MIC": ["XPAR"] * 6,
            "ListingId": [1] * 6,
            "Ticker": ["ABC"] * 6,
            "TimeBucket": [ts0] * 6,
            "Classification": ["LIT_CONTINUOUS"] * 6,
            "AggressorSide": [1] * 6,
            "MarketState": ["CONTINUOUS_TRADING"] * 6,
            "BMLLTradeType": ["LIT", "LIT", "LIT", "DARK", "DARK", "DARK"],
            "LocalPrice": [100.0, 100.2, 100.4, 100.0, 103.0, 98.0],
            "Size": [1.0] * 6,
        }
    )
    base = TradeAnalyticsConfig(
        time_bucket_seconds=60,
        generic_metrics=[TradeGenericConfig(measures=["Volatility"], sides=["Total"])],
        discrepancy_metrics=[],
        flag_metrics=[],
        change_metrics=[],
        impact_metrics=[],
    )
    filtered = TradeAnalyticsConfig(
        time_bucket_seconds=60,
        generic_metrics=[
            TradeGenericConfig(
                measures=["Volatility"],
                sides=["Total"],
                attribute_filters={"BMLLTradeType": ["LIT"]},
            )
        ],
        discrepancy_metrics=[],
        flag_metrics=[],
        change_metrics=[],
        impact_metrics=[],
    )
    out_base = _run_trade(base, trades)
    out_filtered = _run_trade(filtered, trades)
    assert float(out_filtered["TradeTotalVolatility"][0]) != float(
        out_base["TradeTotalVolatility"][0]
    )


def test_pass_config_propagates_trade_bucket_seconds():
    p = PassConfig(name="p1", modules=["trade"], time_bucket_seconds=300)
    assert p.trade_analytics.time_bucket_seconds == 300
