from __future__ import annotations

from typing import List, Optional, Literal

import pandas as pd
import polars as pl
from pydantic import BaseModel, ConfigDict, Field

from basalt.analytics_base import BaseAnalytics, analytic_doc
from basalt.analytics_registry import register_analytics
from basalt.analytics.utils.correlation_common import hurst_exponent


class HurstAnalyticsConfig(BaseModel):
    """
    Hurst exponent analytics computed on a prior pass output.

    This module summarizes each selected numeric series into a single
    Hurst exponent value per group (e.g., per ListingId).
    """

    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "hurst",
                "tier": "post",
                "desc": "Postprocessing: per-series Hurst exponent.",
                "outputs": ["Metric", "HurstExponent"],
                "schema_keys": ["hurst"],
            }
        }
    )

    ENABLED: bool = Field(
        True,
        description="Enable or disable Hurst analytics for this pass.",
        json_schema_extra={
            "long_doc": "Toggle to enable or disable the Hurst module.\n"
            "When disabled, the module is skipped entirely.\n",
        },
    )
    metric_prefix: Optional[str] = Field(
        None,
        description="Optional prefix for generated output names (rows ignore prefix).",
        json_schema_extra={
            "long_doc": "Prefix applied to output names when using wide output formats.\n"
            "Rows output ignores prefixes by design.\n",
        },
    )
    source_pass: str = Field(
        "pass1",
        description="Pass name to use as input.",
        json_schema_extra={
            "long_doc": "Selects which pass output is analyzed.\n"
            "Use a pass containing the metrics you want to measure.\n"
            "The module reads its output directly from the pass context.\n",
        },
    )
    columns: Optional[List[str]] = Field(
        None,
        description="Columns to include (defaults to numeric columns).",
        json_schema_extra={
            "long_doc": "Explicit list of series to analyze.\n"
            "If omitted, all numeric columns (excluding group keys) are used.\n"
            "Non-numeric columns are ignored.\n",
        },
    )
    group_by: List[str] = Field(
        default_factory=lambda: ["ListingId"],
        description="Grouping columns for Hurst computations.",
        json_schema_extra={
            "long_doc": "Groups define independent Hurst computations.\n"
            "Typical grouping is per ListingId, optionally with TimeBucket.\n"
            "Each group yields one Hurst value per selected series.\n",
        },
    )
    max_lag: int = Field(
        20,
        description="Maximum lag for the Hurst estimator.",
        json_schema_extra={
            "long_doc": "Upper bound on lag values used in the estimator.\n"
            "Larger values increase stability but require longer series.\n"
            "The estimator adapts to available sample length.\n",
        },
    )
    min_periods: int = Field(
        30,
        description="Minimum observations required to compute Hurst.",
        json_schema_extra={
            "long_doc": "Minimum number of finite observations before the\n"
            "Hurst exponent is computed. If below this threshold, output is null.\n",
        },
    )
    mode: Literal["static", "rolling"] = Field(
        "static",
        description="Compute a single Hurst per series or a rolling Hurst series.",
        json_schema_extra={
            "long_doc": "Static mode outputs one Hurst exponent per metric and group.\n"
            "Rolling mode outputs a Hurst value per time bucket using a window.\n"
            "Rolling mode requires a time column and a rolling window length.\n",
        },
    )
    time_col: str = Field(
        "TimeBucket",
        description="Time column used for rolling computations.",
        json_schema_extra={
            "long_doc": "Name of the column used to order observations.\n"
            "Only used when mode is 'rolling'.\n"
            "The output includes this column as the window end timestamp.\n",
        },
    )
    rolling_window: Optional[int] = Field(
        None,
        description="Window length for rolling Hurst computations.",
        json_schema_extra={
            "long_doc": "Number of rows used in each rolling window.\n"
            "Required when mode is 'rolling'.\n",
        },
    )
    rolling_min_periods: int = Field(
        30,
        description="Minimum observations per rolling window.",
        json_schema_extra={
            "long_doc": "Minimum non-null observations required per rolling window.\n"
            "If fewer, the rolling Hurst value is null.\n",
        },
    )
    rolling_step: int = Field(
        1,
        description="Step size between rolling windows.",
        json_schema_extra={
            "long_doc": "Downsample rolling output by taking every Nth window.\n"
            "Useful to reduce output size.\n",
        },
    )


@analytic_doc(
    module="hurst",
    pattern=r"^Metric$",
    template="Metric name used to compute the Hurst exponent.",
    unit="Label",
    description="Source metric identifier for the Hurst estimate.",
)
@analytic_doc(
    module="hurst",
    pattern=r"^HurstExponent$",
    template="Estimated Hurst exponent for the source metric.",
    unit="Index",
    description=(
        "Hurst exponent per group. Values < 0.5 indicate mean reversion, "
        "≈0.5 random walk, and > 0.5 trending behavior."
    ),
)
def _register_hurst_docs():
    return None


@register_analytics("hurst", config_attr="hurst_analytics")
class HurstAnalytics(BaseAnalytics):
    REQUIRES: List[str] = []

    def __init__(self, config: HurstAnalyticsConfig):
        super().__init__(
            "hurst",
            {},
            join_keys=config.group_by,
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

        pdf = df.to_pandas()
        group_cols = list(self.config.group_by or [])
        for col in group_cols:
            if col not in pdf.columns:
                raise ValueError(f"Group column {col} not found.")

        if self.config.columns:
            cols = [c for c in self.config.columns if c in pdf.columns]
        else:
            cols = (
                pdf.select_dtypes(include=["number"]).columns.difference(group_cols)
            ).tolist()

        if not cols:
            return pl.DataFrame().lazy()

        rows = []
        if group_cols:
            groups = pdf.groupby(group_cols, dropna=False)
        else:
            groups = [((), pdf)]

        if self.config.mode == "rolling":
            if not self.config.rolling_window:
                raise ValueError("rolling_window must be set for rolling mode.")
            if self.config.time_col not in pdf.columns:
                raise ValueError(
                    f"Time column {self.config.time_col} not found."
                )

        for key, frame in groups:
            if group_cols and not isinstance(key, tuple):
                key = (key,)
            if self.config.mode == "rolling":
                frame = frame.sort_values(self.config.time_col)
            for col in cols:
                if self.config.mode == "rolling":
                    series = frame[col]
                    rolling = series.rolling(
                        window=int(self.config.rolling_window),
                        min_periods=int(self.config.rolling_min_periods),
                    ).apply(
                        lambda x: (
                            (val if val is not None else float("nan"))
                            if (val := hurst_exponent(x, max_lag=self.config.max_lag))
                            is not None
                            else float("nan")
                        ),
                        raw=True,
                    )
                    time_vals = frame[self.config.time_col].values
                    for idx in range(0, len(time_vals), int(self.config.rolling_step)):
                        value = rolling.iat[idx]
                        row = {
                            self.config.time_col: time_vals[idx],
                            "Metric": col,
                            "HurstExponent": None if pd.isna(value) else value,
                        }
                        if group_cols:
                            row.update(dict(zip(group_cols, key)))
                        rows.append(row)
                else:
                    series = frame[col].dropna().values
                    value = None
                    if series.size >= self.config.min_periods:
                        value = hurst_exponent(
                            series, max_lag=self.config.max_lag
                        )
                    row = {
                        "Metric": col,
                        "HurstExponent": value,
                    }
                    if group_cols:
                        row.update(dict(zip(group_cols, key)))
                    rows.append(row)

        return pl.from_pandas(pd.DataFrame(rows)).lazy()
