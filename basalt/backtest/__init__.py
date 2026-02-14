from __future__ import annotations

from .core import BacktestConfig, BacktestDatasetConfig, run_backtest
from .strategies import (
    NaiveMarketMakingVWAPStrategy,
    TrendFollowingIndicatorStrategy,
    build_talib_demo_frame,
)
from .cli_ext import get_cli_extension

__all__ = [
    "BacktestConfig",
    "BacktestDatasetConfig",
    "run_backtest",
    "NaiveMarketMakingVWAPStrategy",
    "TrendFollowingIndicatorStrategy",
    "build_talib_demo_frame",
    "get_cli_extension",
]


def get_basalt_plugin():
    return {
        "name": "backtest",
        "provides": [
            "strategy backtesting",
            "fills dataframe generation",
            "analytics dataset evaluation",
        ],
        "cli_extensions": ["backtest"],
    }

