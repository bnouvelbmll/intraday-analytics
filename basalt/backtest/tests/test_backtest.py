from __future__ import annotations

import datetime as dt

import polars as pl

from basalt.backtest.cli_ext import get_cli_extension
from basalt.backtest.core import BacktestDatasetConfig, run_backtest
from basalt.backtest.strategies import (
    NaiveMarketMakingVWAPStrategy,
    TrendFollowingIndicatorStrategy,
    build_talib_demo_frame,
)


def _sample_frame() -> pl.DataFrame:
    t0 = dt.datetime(2026, 1, 2, 9, 0, 0)
    return pl.DataFrame(
        {
            "Ticker": ["AAA"] * 8 + ["BBB"] * 8,
            "TimeBucket": [t0 + dt.timedelta(seconds=i) for i in range(8)]
            + [t0 + dt.timedelta(seconds=i) for i in range(8)],
            "LocalPrice": [100, 99, 98, 99, 100, 101, 102, 101] * 2,
            "MarketState": ["CONTINUOUS_TRADING"] * 7
            + ["AUCTION"]
            + ["CONTINUOUS_TRADING"] * 8,
            "RSI_14": [35, 32, 28, 31, 45, 62, 70, 55] * 2,
        }
    )


def test_vwap_marketmaking_returns_profit_and_fills():
    df = _sample_frame()
    result = run_backtest(df, strategy=NaiveMarketMakingVWAPStrategy(window=3, threshold_bps=0.0))
    assert isinstance(result["profit"], float)
    fills = result["fills"]
    assert isinstance(fills, pl.DataFrame)
    assert fills.height > 0
    assert set(fills.columns) >= {"symbol", "timestamp", "price", "fill_qty", "side", "strategy"}


def test_trend_following_uses_indicator_column():
    df = _sample_frame()
    strategy = TrendFollowingIndicatorStrategy(
        indicator_col="RSI_14",
        upper_threshold=60,
        lower_threshold=30,
    )
    result = run_backtest(df, strategy=strategy)
    fills = result["fills"]
    assert isinstance(fills, pl.DataFrame)
    assert fills.height > 0


def test_continuous_trading_filter_applied():
    df = _sample_frame()
    ds = BacktestDatasetConfig(
        price_col="LocalPrice",
        timestamp_col="TimeBucket",
        symbol_col="Ticker",
        market_state_col="MarketState",
        continuous_values=("CONTINUOUS_TRADING",),
    )
    result = run_backtest(df, strategy=NaiveMarketMakingVWAPStrategy(window=2), dataset=ds)
    assert result["rows"] == 15


def test_talib_demo_frame_and_cli_extension():
    demo = build_talib_demo_frame(rows=64)
    assert demo.height == 64
    ext = get_cli_extension()
    assert "backtest" in ext

