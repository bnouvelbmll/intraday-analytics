import polars as pl
import numpy as np
from pydantic import BaseModel, Field, ConfigDict
from typing import Any, List, Dict, Optional, Union, Literal
from basalt.analytics_base import BaseAnalytics
from basalt.analytics_registry import register_analytics
import logging

try:
    from basalt.analytics.talib import (
        TALIB_AVAILABLE,
        TalibIndicatorConfig,
        get_talib_function_metadata,
        list_talib_functions,
        talib,
    )
except Exception:
    TALIB_AVAILABLE = False
    talib = None

    class TalibIndicatorConfig(BaseModel):
        """
        Configuration for a single TA-Lib indicator.

        Fallback definition used when the optional `bmll-basalt-talib` package
        is not installed.
        """

        name: str = Field(..., description="TA-Lib indicator name (e.g., SMA, RSI).")
        input_col: str = Field(..., description="Input column for the indicator.")
        timeperiod: Optional[int] = Field(
            14, description="Default timeperiod if not provided in parameters."
        )
        parameters: dict[str, Any] = Field(
            default_factory=dict,
            description="JSON kwargs passed to the TA-Lib function.",
        )
        output_col: Optional[str] = Field(
            None, description="Optional output column name override."
        )
        output_index: int = Field(
            0, description="Output index for multi-output TA-Lib functions."
        )

    def list_talib_functions() -> list[str]:
        return []

    def get_talib_function_metadata(_name: str) -> dict[str, Any]:
        return {}


class GenericAnalyticsConfig(BaseModel):
    """
    Generic analytics configuration.

    Defines postprocessing aggregations and optional TA-Lib indicators on
    outputs of a previous pass. The generic pipeline loads the output of a
    selected pass from the context, optionally resamples TimeBucket, applies
    group-by aggregations, and computes TA-Lib indicators when available. This
    provides a flexible way to derive secondary metrics without modifying core
    analytics modules.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "generic",
                "tier": "post",
                "desc": "Postprocessing: generic expressions and derived metrics.",
                "outputs": ["Custom"],
                "schema_keys": ["generic"],
            }
        }
    )

    ENABLED: bool = Field(
        True,
        description="Enable or disable the generic analytics module for this pass.",
    )
    metric_prefix: Optional[str] = Field(
        None,
        description="Prefix for generic metric columns.",
        json_schema_extra={
            "long_doc": "Prepended to all generic output columns.\n"
            "Useful to namespace derived metrics from other passes.\n"
            "Example: 'G_' yields G_MyMetric.\n"
            "Applies to all generic outputs in this pass.\n"
            "Implemented by `BaseAnalytics.metric_prefix`.\n"
            "See `basalt/analytics_base.py` for naming rules.\n"
            "Changing the prefix changes column names and joins.\n"
            "Keep stable for production outputs.\n"
            "Leave empty to use default naming.\n"
        },
    )
    source_pass: str = Field(
        "pass1",
        description="Pass name to use as input.",
        json_schema_extra={
            "long_doc": "Selects the pass output stored in the pipeline context.\n"
            "Generic analytics operate on a previous pass's output.\n"
            "Example: source_pass='pass1' uses results from pass1.\n"
            "Used in `GenericAnalytics.compute()` to fetch the source frame.\n"
            "If the pass is missing, an error is raised.\n"
            "Ensure the source pass runs before this pass.\n"
            "This enables multi-stage pipelines with derived metrics.\n"
            "Changing this changes the input dataset.\n"
            "Only applies to generic analytics.",
        },
    )
    group_by: List[str] = Field(
        default_factory=lambda: ["ListingId", "TimeBucket"],
        description="Group-by columns for aggregation.",
        json_schema_extra={
            "long_doc": "Defines grouping keys for generic aggregations.\n"
            "Common keys: ListingId, TimeBucket, MIC.\n"
            "Used in `GenericAnalytics.compute()` when applying aggregations.\n"
            "Ensure these columns exist in the source pass output.\n"
            "If a key is missing, aggregation will fail.\n"
            "You can include additional keys to create finer granularity.\n"
            "Large group-by keys can increase output size.\n"
            "For resampled data, keep TimeBucket in group_by.\n"
            "This controls grouping for both aggregations and TA-Lib.",
        },
    )
    resample_rule: Optional[str] = Field(
        None,
        description="Optional resample rule (e.g. '15m').",
        json_schema_extra={
            "long_doc": "If set, truncates `TimeBucket` to this rule before aggregation.\n"
            "Example: '15m' buckets results into 15-minute windows.\n"
            "Uses Polars `dt.truncate` on TimeBucket.\n"
            "Applied before group-by and aggregations.\n"
            "Requires TimeBucket to be present in group_by.\n"
            "If TimeBucket is missing, resampling will be skipped.\n"
            "Useful to downsample high-frequency outputs.\n"
            "Changing this changes output granularity.\n"
            "This does not affect upstream passes.",
        },
    )  # e.g., "15m"
    aggregations: Dict[str, str] = Field(
        default_factory=dict,
        description="Column aggregation mapping (col -> method).",
        json_schema_extra={
            "long_doc": "Defines which columns to aggregate and how.\n"
            "Example: {'Volume': 'sum', 'VWAP': 'mean'}.\n"
            "Methods can be sum, mean, first, last, max, min, count.\n"
            "Used directly in `GenericAnalytics.compute()`.\n"
            "Only columns present in the source pass can be aggregated.\n"
            "If a column is missing, it is ignored with a warning.\n"
            "Aggregation is performed after optional resampling.\n"
            "Large numbers of aggregations increase output width.\n"
            "This is the primary knob for generic analytics output.",
        },
    )  # col -> "sum", "mean", "last", "first"
    talib_indicators: List[TalibIndicatorConfig] = Field(
        default_factory=list,
        description="TA-Lib indicators to compute.",
        json_schema_extra={
            "long_doc": "If TA-Lib is installed, each indicator is applied per group.\n"
            "Indicators are computed over the input column series.\n"
            "Example: SMA on Close with timeperiod=14.\n"
            "Computed inside `GenericAnalytics.compute()` after aggregation.\n"
            "Requires TA-Lib to be installed (optional dependency).\n"
            "If TA-Lib is missing, indicators are skipped with a warning.\n"
            "Indicators can significantly increase compute time.\n"
            "Use only the indicators you need.\n"
            "Outputs are named using indicator config output_col if provided.",
        },
    )


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
                    default_suffix = (
                        str(ind.timeperiod)
                        if getattr(ind, "timeperiod", None) is not None
                        else "raw"
                    )
                    out_col = ind.output_col or f"{ind.name}_{default_suffix}"

                    def compute_talib(
                        s: pl.Series,
                        name=ind.name,
                        period=ind.timeperiod,
                        params=dict(getattr(ind, "parameters", {}) or {}),
                        output_index=int(getattr(ind, "output_index", 0) or 0),
                    ) -> pl.Series:
                        try:
                            if talib is None:
                                return pl.Series([None] * len(s))
                            fn = getattr(talib, name)
                            call_kwargs = dict(params)
                            if period is not None and "timeperiod" not in call_kwargs:
                                call_kwargs["timeperiod"] = period
                            # TA-Lib expects float64 numpy array
                            res = fn(s.cast(pl.Float64).to_numpy(), **call_kwargs)
                            if isinstance(res, (tuple, list)):
                                if not res:
                                    return pl.Series([None] * len(s))
                                idx = max(0, min(output_index, len(res) - 1))
                                res = res[idx]
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
