import polars as pl
import numpy as np
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Union, Literal
from intraday_analytics.analytics_base import BaseAnalytics
from intraday_analytics.analytics_registry import register_analytics
import logging

try:
    import talib

    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False


class TalibIndicatorConfig(BaseModel):
    name: str  # e.g., "SMA", "RSI"
    input_col: str  # e.g., "Close"
    timeperiod: int = 14
    output_col: Optional[str] = None


class GenericAnalyticsConfig(BaseModel):
    ENABLED: bool = True
    metric_prefix: Optional[str] = None
    source_pass: str = "pass1"
    group_by: List[str] = ["ListingId", "TimeBucket"]
    resample_rule: Optional[str] = None  # e.g., "15m"
    aggregations: Dict[str, str] = Field(
        default_factory=dict
    )  # col -> "sum", "mean", "last", "first"
    talib_indicators: List[TalibIndicatorConfig] = Field(default_factory=list)


@register_analytics("generic", config_attr="generic_analytics")
class GenericAnalytics(BaseAnalytics):
    REQUIRES = []  # Depends on previous pass, not raw tables directly

    def __init__(self, config: GenericAnalyticsConfig):
        super().__init__(
            "generic",
            {},
            join_keys=config.group_by,
            metric_prefix=config.metric_prefix,
        )
        self.config = config
        self.ref = None

    def compute(self) -> pl.LazyFrame:
        # 1. Get input from context
        source_df = self.context.get(self.config.source_pass)
        if source_df is None:
            # It might be a LazyFrame in some contexts, or DataFrame in others
            # The pipeline stores collected DataFrames in context
            raise ValueError(
                f"Source pass '{self.config.source_pass}' not found in context. Available: {list(self.context.keys())}"
            )

        # Ensure we work with LazyFrame
        if isinstance(source_df, pl.DataFrame):
            lf = source_df.lazy()
        else:
            lf = source_df

        # Join with ref if available (to get InstrumentId etc.)
        if self.ref is not None:
            # Assuming ref has ListingId
            if isinstance(self.ref, pl.DataFrame):
                ref_lazy = self.ref.lazy()
            else:
                ref_lazy = self.ref

            # Only join if ListingId is in both
            # And we want to avoid column collisions, but we need columns for grouping
            lf = lf.join(ref_lazy, on="ListingId", how="left")

        # 2. Resample if needed
        if self.config.resample_rule:
            # Assuming TimeBucket is the time column and is in the group_by list
            if "TimeBucket" in self.config.group_by:
                lf = lf.with_columns(
                    pl.col("TimeBucket").dt.truncate(self.config.resample_rule)
                )

        # 3. GroupBy & Aggregate
        if self.config.aggregations:
            aggs = []
            for col, method in self.config.aggregations.items():
                if method == "sum":
                    aggs.append(pl.col(col).sum())
                elif method == "mean":
                    aggs.append(pl.col(col).mean())
                elif method == "first":
                    aggs.append(pl.col(col).first())
                elif method == "last":
                    aggs.append(pl.col(col).last())
                elif method == "max":
                    aggs.append(pl.col(col).max())
                elif method == "min":
                    aggs.append(pl.col(col).min())
                elif method == "count":
                    aggs.append(pl.col(col).count())
                else:
                    logging.warning(
                        f"Unknown aggregation method '{method}' for column '{col}'"
                    )

            lf = lf.group_by(self.config.group_by).agg(aggs)

        # 4. TA-Lib Indicators
        if self.config.talib_indicators:
            if not TALIB_AVAILABLE:
                logging.warning("TA-Lib is not installed. Skipping TA-Lib indicators.")
            else:
                # We need to apply indicators per group (e.g. ListingId)
                # We assume the data is sorted by time within the group
                lf = lf.sort(self.config.group_by)

                for ind in self.config.talib_indicators:
                    out_col = ind.output_col or f"{ind.name}_{ind.timeperiod}"

                    def compute_talib(
                        s: pl.Series, name=ind.name, period=ind.timeperiod
                    ) -> pl.Series:
                        try:
                            fn = getattr(talib, name)
                            # TA-Lib expects float64 numpy array
                            res = fn(s.cast(pl.Float64).to_numpy(), timeperiod=period)
                            return pl.Series(res)
                        except Exception as e:
                            logging.error(f"Error computing {name}: {e}")
                            return pl.Series([None] * len(s))

                    # Use map_batches within over() to apply per group
                    # We need to group by the non-time keys in group_by
                    group_keys = [k for k in self.config.group_by if k != "TimeBucket"]

                    if group_keys:
                        lf = lf.with_columns(
                            pl.col(ind.input_col)
                            .map_batches(
                                lambda s: compute_talib(s), return_dtype=pl.Float64
                            )
                            .over(group_keys)
                            .alias(out_col)
                        )
                    else:
                        # No grouping (e.g. single time series)
                        lf = lf.with_columns(
                            pl.col(ind.input_col)
                            .map_batches(
                                lambda s: compute_talib(s), return_dtype=pl.Float64
                            )
                            .alias(out_col)
                        )

        if self.metric_prefix:
            schema_cols = lf.collect_schema().names()
            rename_map = {
                col: self.apply_prefix(col)
                for col in schema_cols
                if col not in self.config.group_by
            }
            if rename_map:
                lf = lf.rename(rename_map)

        self.df = lf
        return self.df
