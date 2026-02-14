from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import polars as pl


@dataclass(frozen=True)
class BacktestDatasetConfig:
    """
    Dataset mapping used by the backtest engine.

    Fields:
    - `price_col`: price used for fills and mark-to-market.
    - `timestamp_col`: event timestamp used to order fills.
    - `symbol_col`: symbol identifier used to isolate strategy state by instrument.
    - `market_state_col`: optional market-state column.
    - `continuous_values`: accepted values for continuous trading filtering.
    """

    price_col: str = "LocalPrice"
    timestamp_col: str = "TimeBucket"
    symbol_col: str = "Ticker"
    market_state_col: str | None = "MarketState"
    continuous_values: tuple[str, ...] = ("CONTINUOUS_TRADING",)


@dataclass(frozen=True)
class BacktestConfig:
    """
    Runtime parameters for a backtest.

    Fields:
    - `initial_cash`: starting cash used to evaluate final equity.
    - `unit_size`: quantity traded for each unit of target position change.
    - `return_fills`: if true return fills dataframe; otherwise it may be omitted by callers.
    """

    initial_cash: float = 0.0
    unit_size: float = 1.0
    return_fills: bool = True


class Strategy(Protocol):
    name: str

    def generate_targets(
        self,
        df: pl.DataFrame,
        *,
        dataset: BacktestDatasetConfig,
    ) -> pl.DataFrame:
        """
        Return frame with target positions.

        Output columns must include:
        - symbol column (`dataset.symbol_col`)
        - timestamp column (`dataset.timestamp_col`)
        - `target_position` (float)
        """


def _to_dataframe(data: Any) -> pl.DataFrame:
    if isinstance(data, pl.DataFrame):
        return data
    if isinstance(data, pl.LazyFrame):
        return data.collect()
    if hasattr(data, "to_dict"):
        try:
            return pl.DataFrame(data)
        except Exception:
            pass
    return pl.DataFrame(data)


def prepare_dataset(
    data: Any,
    *,
    dataset: BacktestDatasetConfig,
) -> pl.DataFrame:
    """
    Normalize and filter input data for backtesting.
    """
    df = _to_dataframe(data)
    required = [dataset.price_col, dataset.timestamp_col, dataset.symbol_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required dataset columns: {missing}")

    out = df
    if dataset.market_state_col and dataset.market_state_col in out.columns:
        out = out.filter(pl.col(dataset.market_state_col).is_in(list(dataset.continuous_values)))

    out = out.with_columns(
        pl.col(dataset.price_col).cast(pl.Float64).alias(dataset.price_col),
    )
    out = out.sort([dataset.symbol_col, dataset.timestamp_col])
    return out


def run_backtest(
    data: Any,
    *,
    strategy: Strategy,
    dataset: BacktestDatasetConfig | None = None,
    config: BacktestConfig | None = None,
) -> dict[str, Any]:
    """
    Run a simple event-driven backtest and return summary + fills.
    """
    ds = dataset or BacktestDatasetConfig()
    cfg = config or BacktestConfig()
    df = prepare_dataset(data, dataset=ds)
    if df.is_empty():
        return {
            "strategy": strategy.name,
            "rows": 0,
            "fills": pl.DataFrame(),
            "profit": 0.0,
            "final_equity": cfg.initial_cash,
        }

    targets = strategy.generate_targets(df, dataset=ds)
    for col in (ds.symbol_col, ds.timestamp_col, "target_position"):
        if col not in targets.columns:
            raise ValueError(f"Strategy output missing required column: {col}")
    targets = targets.sort([ds.symbol_col, ds.timestamp_col])

    merged = df.join(
        targets.select([ds.symbol_col, ds.timestamp_col, "target_position"]),
        on=[ds.symbol_col, ds.timestamp_col],
        how="left",
    ).with_columns(
        pl.col("target_position").fill_null(0.0).cast(pl.Float64),
    )

    merged = merged.with_columns(
        pl.col("target_position")
        .shift(1)
        .over(ds.symbol_col)
        .fill_null(0.0)
        .alias("_prev_target"),
    ).with_columns(
        (pl.col("target_position") - pl.col("_prev_target")).alias("_delta_position"),
    ).with_columns(
        (pl.col("_delta_position") * cfg.unit_size).alias("fill_qty"),
        (-(pl.col("_delta_position") * cfg.unit_size) * pl.col(ds.price_col)).alias("cash_flow"),
    )

    fills = (
        merged.filter(pl.col("fill_qty") != 0.0)
        .select(
            [
                pl.col(ds.symbol_col).alias("symbol"),
                pl.col(ds.timestamp_col).alias("timestamp"),
                pl.col(ds.price_col).alias("price"),
                pl.col("fill_qty"),
                pl.when(pl.col("fill_qty") > 0).then(pl.lit("BUY")).otherwise(pl.lit("SELL")).alias("side"),
            ]
        )
        .with_columns(pl.lit(strategy.name).alias("strategy"))
    )

    cash = cfg.initial_cash + float(merged.select(pl.col("cash_flow").sum()).item())
    positions = (
        merged.group_by(ds.symbol_col)
        .agg(
            pl.col("target_position").last().alias("pos"),
            pl.col(ds.price_col).last().alias("last_px"),
        )
        .with_columns((pl.col("pos") * cfg.unit_size * pl.col("last_px")).alias("mtm"))
    )
    mtm = 0.0 if positions.is_empty() else float(positions.select(pl.col("mtm").sum()).item())
    final_equity = cash + mtm
    profit = final_equity - cfg.initial_cash

    return {
        "strategy": strategy.name,
        "rows": df.height,
        "fills": fills if cfg.return_fills else None,
        "profit": float(profit),
        "final_equity": float(final_equity),
    }
