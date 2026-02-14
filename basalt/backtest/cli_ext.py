from __future__ import annotations

import json

import polars as pl

from .core import BacktestConfig, BacktestDatasetConfig, run_backtest
from .strategies import (
    NaiveMarketMakingVWAPStrategy,
    TrendFollowingIndicatorStrategy,
    build_talib_demo_frame,
)


class BacktestCLI:
    @staticmethod
    def run(
        *,
        input_path: str | None = None,
        strategy: str = "vwap_mm",
        price_col: str = "LocalPrice",
        timestamp_col: str = "TimeBucket",
        symbol_col: str = "Ticker",
        market_state_col: str = "MarketState",
        continuous_values: str = "CONTINUOUS_TRADING",
        output_fills_path: str | None = None,
        indicator_col: str = "RSI_14",
        upper_threshold: float = 60.0,
        lower_threshold: float = 40.0,
    ) -> dict:
        """
        Run a simple strategy backtest on parquet input or a synthetic TALIB demo frame.
        """
        if input_path:
            df = pl.read_parquet(input_path)
        else:
            df = build_talib_demo_frame()

        ds = BacktestDatasetConfig(
            price_col=price_col,
            timestamp_col=timestamp_col,
            symbol_col=symbol_col,
            market_state_col=market_state_col or None,
            continuous_values=tuple(x.strip() for x in continuous_values.split(",") if x.strip()),
        )
        if strategy == "vwap_mm":
            strat = NaiveMarketMakingVWAPStrategy()
        elif strategy == "trend_indicator":
            strat = TrendFollowingIndicatorStrategy(
                indicator_col=indicator_col,
                upper_threshold=upper_threshold,
                lower_threshold=lower_threshold,
            )
        else:
            raise ValueError("strategy must be one of: vwap_mm, trend_indicator")

        result = run_backtest(df, strategy=strat, dataset=ds, config=BacktestConfig())
        fills = result.get("fills")
        if output_fills_path and isinstance(fills, pl.DataFrame):
            fills.write_parquet(output_fills_path)

        payload = {
            "strategy": result["strategy"],
            "rows": result["rows"],
            "profit": result["profit"],
            "final_equity": result["final_equity"],
            "fills_count": fills.height if isinstance(fills, pl.DataFrame) else 0,
            "fills_path": output_fills_path,
        }
        print(json.dumps(payload, indent=2, default=str))
        return payload


def get_cli_extension():
    return {"backtest": BacktestCLI}

