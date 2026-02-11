from __future__ import annotations

"""
Subset of the 101 Formulaic Alphas.

Formulas adapted from:
Kakushadze, Z. (2016). 101 Formulaic Alphas.
"""

from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import pandas as pd
import polars as pl
from pydantic import BaseModel, Field, ConfigDict

from intraday_analytics.analytics_base import BaseAnalytics
from intraday_analytics.analytics_registry import register_analytics


@dataclass(frozen=True)
class AlphaColumns:
    open: str
    high: str
    low: str
    close: str
    volume: str
    vwap: str
    notional: str


class Alpha101AnalyticsConfig(BaseModel):
    """
    Subset of WorldQuant 101 Formulaic Alphas (adapted).

    This module implements a **subset** of the original formulas from
    Kakushadze (2016) and adapts them to intraday buckets. Window lengths
    are interpreted in bucket counts and can be scaled with `window_multiplier`.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "alpha101",
                "tier": "post",
                "desc": "Postprocessing: subset of 101 formulaic alphas (adapted).",
                "outputs": ["Alpha001", "Alpha101"],
                "schema_keys": ["alpha101"],
            }
        }
    )

    ENABLED: bool = True
    metric_prefix: Optional[str] = Field(
        None,
        description="Prefix for alpha output columns.",
        json_schema_extra={
            "long_doc": "Prepended to all alpha outputs.\n"
            "Useful to namespace alpha outputs from other modules.\n"
            "Leave empty to keep AlphaXXX names.\n"
        },
    )
    source_pass: str = Field(
        "pass1",
        description="Pass name to use as input.",
        json_schema_extra={
            "long_doc": "Selects the pass output stored in the pipeline context.\n"
            "Alpha101 analytics operate on a previous pass's output.\n"
            "This pass should expose OHLCV (Open/High/Low/Close/Volume/VWAP).\n"
        },
    )
    alpha_ids: List[int] = Field(
        default_factory=lambda: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 101],
        description="Alpha IDs to compute (subset).",
        json_schema_extra={
            "long_doc": "Subset of 101 Formulaic Alphas to compute.\n"
            "Currently supported: 1-10 and 101.\n"
            "Unsupported IDs are ignored.\n"
        },
    )
    window_multiplier: float = Field(
        1.0,
        gt=0,
        description="Scale factor for time windows (buckets).",
        json_schema_extra={
            "long_doc": "Scales rolling windows in alpha formulas.\n"
            "Example: 0.5 halves all windows; 2.0 doubles them.\n"
            "Use to adapt daily formulas to intraday buckets.\n"
        },
    )
    open_col: str = "Open"
    high_col: str = "High"
    low_col: str = "Low"
    close_col: str = "Close"
    volume_col: str = "Volume"
    vwap_col: str = "VWAP"
    notional_col: str = "Notional"


def _scaled(window: int, multiplier: float) -> int:
    return max(1, int(round(window * multiplier)))


def _signed_power(x: pd.Series, a: float) -> pd.Series:
    return np.sign(x) * (np.abs(x) ** a)


def _rank_cs(df: pd.DataFrame, col: str, index: pd.Index | None = None) -> pd.Series:
    ranked = df.groupby("TimeBucket")[col].rank(pct=True)
    if index is not None and len(index) == len(ranked):
        ranked.index = index
    return ranked


def _ts_rank(series: pd.Series, window: int) -> pd.Series:
    def _apply(s):
        return s.rolling(window).apply(
            lambda w: pd.Series(w).rank(pct=True).iloc[-1], raw=False
        )

    return (
        series.groupby(level=0, group_keys=False)
        .apply(_apply)
        .reset_index(level=0, drop=True)
    )


def _ts_argmax(series: pd.Series, window: int) -> pd.Series:
    def _apply(s):
        return s.rolling(window).apply(lambda w: np.argmax(w) + 1, raw=True)

    return (
        series.groupby(level=0, group_keys=False)
        .apply(_apply)
        .reset_index(level=0, drop=True)
    )


def _rolling_corr(x: pd.Series, y: pd.Series, window: int) -> pd.Series:
    def _apply(s):
        return s.rolling(window).corr(y.loc[s.index])

    return (
        x.groupby(level=0, group_keys=False)
        .apply(_apply)
        .reset_index(level=0, drop=True)
    )


def _rolling_sum(series: pd.Series, window: int) -> pd.Series:
    return (
        series.groupby(level=0, group_keys=False)
        .apply(lambda s: s.rolling(window).sum())
        .reset_index(level=0, drop=True)
    )


def _rolling_min(series: pd.Series, window: int) -> pd.Series:
    return (
        series.groupby(level=0, group_keys=False)
        .apply(lambda s: s.rolling(window).min())
        .reset_index(level=0, drop=True)
    )


def _rolling_max(series: pd.Series, window: int) -> pd.Series:
    return (
        series.groupby(level=0, group_keys=False)
        .apply(lambda s: s.rolling(window).max())
        .reset_index(level=0, drop=True)
    )


def _rolling_std(series: pd.Series, window: int) -> pd.Series:
    return (
        series.groupby(level=0, group_keys=False)
        .apply(lambda s: s.rolling(window).std())
        .reset_index(level=0, drop=True)
    )


@register_analytics("alpha101", config_attr="alpha101_analytics")
class Alpha101Analytics(BaseAnalytics):
    """
    Subset of the 101 Formulaic Alphas (Kakushadze, 2016).
    """

    REQUIRES: List[str] = []

    def __init__(self, config: Alpha101AnalyticsConfig):
        super().__init__(
            "alpha101",
            {},
            join_keys=["ListingId", "TimeBucket"],
            metric_prefix=config.metric_prefix,
        )
        self.config = config

    def compute(self) -> pl.LazyFrame:
        source_df = self.context.get(self.config.source_pass)
        if source_df is None:
            raise ValueError(
                f"Source pass '{self.config.source_pass}' not found in context."
            )
        if isinstance(source_df, pl.LazyFrame):
            df = source_df.collect()
        elif isinstance(source_df, pl.DataFrame):
            df = source_df
        else:
            raise ValueError("Unsupported source pass type.")

        cols = AlphaColumns(
            open=self.config.open_col,
            high=self.config.high_col,
            low=self.config.low_col,
            close=self.config.close_col,
            volume=self.config.volume_col,
            vwap=self.config.vwap_col,
            notional=self.config.notional_col,
        )
        pdf = df.to_pandas()
        if "TimeBucket" not in pdf.columns:
            raise ValueError("TimeBucket column required for alpha computations.")
        pdf = pdf.sort_values(["ListingId", "TimeBucket"])
        pdf.set_index(["ListingId", "TimeBucket"], inplace=True)

        close = pdf[cols.close]
        open_ = pdf[cols.open]
        high = pdf[cols.high]
        low = pdf[cols.low]
        volume = pdf[cols.volume]
        vwap = pdf[cols.vwap] if cols.vwap in pdf.columns else None
        if vwap is None and cols.notional in pdf.columns and cols.volume in pdf.columns:
            vwap = pdf[cols.notional] / pdf[cols.volume].replace(0, np.nan)
        returns = close.groupby(level=0).pct_change()

        out = pd.DataFrame(index=pdf.index)
        m = self.config.window_multiplier

        if 1 in self.config.alpha_ids:
            w20 = _scaled(20, m)
            w5 = _scaled(5, m)
            base = pd.Series(
                np.where(
                    returns < 0,
                    _rolling_std(returns, w20),
                    close,
                ),
                index=pdf.index,
            )
            alpha = _rank_cs(
                pdf.reset_index().assign(tmp=_ts_argmax(_signed_power(base, 2), w5)),
                "tmp",
                index=pdf.index,
            ) - 0.5
            out["Alpha001"] = alpha

        if 2 in self.config.alpha_ids:
            w2 = _scaled(2, m)
            w6 = _scaled(6, m)
            logv = np.log(volume.replace(0, np.nan))
            x = _rank_cs(
                pdf.reset_index().assign(tmp=logv.groupby(level=0).diff(w2)),
                "tmp",
                index=pdf.index,
            )
            y = _rank_cs(
                pdf.reset_index().assign(tmp=(close - open_) / open_),
                "tmp",
                index=pdf.index,
            )
            alpha = -1 * _rolling_corr(x, y, w6)
            out["Alpha002"] = alpha

        if 3 in self.config.alpha_ids:
            w10 = _scaled(10, m)
            x = _rank_cs(pdf.reset_index().assign(tmp=open_), "tmp", index=pdf.index)
            y = _rank_cs(pdf.reset_index().assign(tmp=volume), "tmp", index=pdf.index)
            alpha = -1 * _rolling_corr(x, y, w10)
            out["Alpha003"] = alpha

        if 4 in self.config.alpha_ids:
            w9 = _scaled(9, m)
            rank_low = _rank_cs(
                pdf.reset_index().assign(tmp=low), "tmp", index=pdf.index
            )
            alpha = -1 * _ts_rank(rank_low, w9)
            out["Alpha004"] = alpha

        if 5 in self.config.alpha_ids and vwap is not None:
            w10 = _scaled(10, m)
            mean_vwap = _rolling_sum(vwap, w10) / w10
            left = _rank_cs(
                pdf.reset_index().assign(tmp=(open_ - mean_vwap)),
                "tmp",
                index=pdf.index,
            )
            right = _rank_cs(
                pdf.reset_index().assign(tmp=(close - vwap)),
                "tmp",
                index=pdf.index,
            )
            alpha = left * (-1 * right.abs())
            out["Alpha005"] = alpha

        if 6 in self.config.alpha_ids:
            w10 = _scaled(10, m)
            alpha = -1 * _rolling_corr(open_, volume, w10)
            out["Alpha006"] = alpha.values

        if 7 in self.config.alpha_ids:
            w20 = _scaled(20, m)
            w7 = _scaled(7, m)
            w60 = _scaled(60, m)
            adv20 = volume.groupby(level=0).rolling(w20).mean().reset_index(level=0, drop=True)
            delta_close = close.groupby(level=0).diff(w7)
            cond = adv20 < volume
            alpha = np.where(
                cond,
                (-1 * _ts_rank(delta_close.abs(), w60)) * np.sign(delta_close),
                -1.0,
            )
            out["Alpha007"] = alpha

        if 8 in self.config.alpha_ids:
            w5 = _scaled(5, m)
            w10 = _scaled(10, m)
            sum_open = _rolling_sum(open_, w5)
            sum_ret = _rolling_sum(returns, w5)
            base = sum_open * sum_ret
            alpha = -1 * _rank_cs(
                pdf.reset_index().assign(tmp=base - base.groupby(level=0).shift(w10)),
                "tmp",
                index=pdf.index,
            )
            out["Alpha008"] = alpha

        if 9 in self.config.alpha_ids:
            w1 = _scaled(1, m)
            w5 = _scaled(5, m)
            delta_close = close.groupby(level=0).diff(w1)
            min_delta = _rolling_min(delta_close, w5)
            max_delta = _rolling_max(delta_close, w5)
            alpha = np.where(
                0 < min_delta,
                delta_close,
                np.where(max_delta < 0, delta_close, -1 * delta_close),
            )
            out["Alpha009"] = alpha

        if 10 in self.config.alpha_ids:
            w1 = _scaled(1, m)
            w4 = _scaled(4, m)
            delta_close = close.groupby(level=0).diff(w1)
            min_delta = _rolling_min(delta_close, w4)
            max_delta = _rolling_max(delta_close, w4)
            base = np.where(
                0 < min_delta,
                delta_close,
                np.where(max_delta < 0, delta_close, -1 * delta_close),
            )
            alpha = _rank_cs(
                pdf.reset_index().assign(tmp=base), "tmp", index=pdf.index
            )
            out["Alpha010"] = alpha

        if 101 in self.config.alpha_ids:
            denom = (high - low) + 0.001
            alpha = (close - open_) / denom
            out["Alpha101"] = alpha.values

        out.reset_index(inplace=True)
        return pl.from_pandas(out).lazy()
