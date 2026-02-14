from __future__ import annotations

from dataclasses import dataclass
import polars as pl

from .core import BacktestDatasetConfig


@dataclass(frozen=True)
class NaiveMarketMakingVWAPStrategy:
    """
    Mean-reversion style strategy around lagged rolling VWAP.

    Rule:
    - Buy when price < prev_vwap * (1 - threshold_bps * 1e-4)
    - Sell when price > prev_vwap * (1 + threshold_bps * 1e-4)
    - Hold previous position otherwise.
    """

    window: int = 20
    threshold_bps: float = 5.0
    max_position: float = 1.0
    name: str = "naive_marketmaking_vwap"

    def generate_targets(
        self,
        df: pl.DataFrame,
        *,
        dataset: BacktestDatasetConfig,
    ) -> pl.DataFrame:
        price = pl.col(dataset.price_col)
        rolling_vwap = price.rolling_mean(window_size=max(2, int(self.window)))
        prev_vwap = rolling_vwap.shift(1).over(dataset.symbol_col)
        up = prev_vwap * (1.0 + self.threshold_bps * 1e-4)
        down = prev_vwap * (1.0 - self.threshold_bps * 1e-4)

        signal = (
            pl.when(price < down)
            .then(1.0)
            .when(price > up)
            .then(-1.0)
            .otherwise(0.0)
            .alias("_signal")
        )

        out = df.select([dataset.symbol_col, dataset.timestamp_col, dataset.price_col]).with_columns(signal)
        out = out.with_columns(
            pl.col("_signal")
            .cum_sum()
            .over(dataset.symbol_col)
            .clip(-abs(self.max_position), abs(self.max_position))
            .alias("target_position")
        )
        return out.select([dataset.symbol_col, dataset.timestamp_col, "target_position"])


@dataclass(frozen=True)
class TrendFollowingIndicatorStrategy:
    """
    Trend-following strategy driven by an existing indicator column.

    Typical use with prior TA-Lib pass:
    - `indicator_col="RSI_14"` with thresholds 30/70
    """

    indicator_col: str
    upper_threshold: float
    lower_threshold: float
    max_position: float = 1.0
    name: str = "trend_following_indicator"

    def generate_targets(
        self,
        df: pl.DataFrame,
        *,
        dataset: BacktestDatasetConfig,
    ) -> pl.DataFrame:
        if self.indicator_col not in df.columns:
            raise ValueError(f"Indicator column not found: {self.indicator_col}")
        ind = pl.col(self.indicator_col).cast(pl.Float64)
        signal = (
            pl.when(ind >= self.upper_threshold)
            .then(1.0)
            .when(ind <= self.lower_threshold)
            .then(-1.0)
            .otherwise(0.0)
            .alias("_signal")
        )
        out = df.select([dataset.symbol_col, dataset.timestamp_col, self.indicator_col]).with_columns(signal)
        out = out.with_columns(
            pl.col("_signal")
            .cum_sum()
            .over(dataset.symbol_col)
            .clip(-abs(self.max_position), abs(self.max_position))
            .alias("target_position")
        )
        return out.select([dataset.symbol_col, dataset.timestamp_col, "target_position"])


def build_talib_demo_frame(rows: int = 180) -> pl.DataFrame:
    """
    Synthetic dataset compatible with `TrendFollowingIndicatorStrategy`.
    """
    rows = max(20, int(rows))
    ts = pl.datetime_range(
        start=pl.datetime(2026, 1, 2, 9, 0, 0),
        end=pl.datetime(2026, 1, 2, 9, 0, 0) + pl.duration(seconds=rows - 1),
        interval="1s",
        eager=True,
    )
    # deterministic shape with trend + oscillation proxy for indicator.
    base = pl.Series("i", list(range(rows))).cast(pl.Float64)
    price = 100.0 + (base * 0.03) + ((base % 17) - 8) * 0.07
    indicator = 50.0 + ((base % 20) - 10) * 3.0
    return pl.DataFrame(
        {
            "Ticker": ["DEMO"] * rows,
            "TimeBucket": ts,
            "LocalPrice": price,
            "MarketState": ["CONTINUOUS_TRADING"] * rows,
            # Example "talib pass output" indicator
            "RSI_14": indicator,
        }
    )
