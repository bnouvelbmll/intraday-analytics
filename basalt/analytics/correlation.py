from __future__ import annotations

from typing import List, Optional, Literal

import pandas as pd
import polars as pl
from pydantic import BaseModel, Field, ConfigDict

from basalt.analytics_base import BaseAnalytics, analytic_doc
from basalt.analytics_registry import register_analytics
from basalt.analytics.utils.correlation_common import soft_corr_matrix


class CorrelationAnalyticsConfig(BaseModel):
    """
    Correlation matrix analytics on a prior pass output.

    Use this module to discover co-movement and regime shifts between metrics.
    Pairwise correlations can highlight redundant signals, diversification
    opportunities, or stress events when correlations spike or collapse.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "correlation",
                "tier": "post",
                "desc": "Postprocessing: correlation matrices (rows or matrix).",
                "outputs": ["MetricX", "MetricY", "Corr"],
                "schema_keys": ["correlation"],
            }
        }
    )

    ENABLED: bool = Field(
        True,
        description="Enable or disable correlation analytics for this pass.",
        json_schema_extra={
            "long_doc": "Toggle to enable or disable the correlation module.\n"
            "When disabled, the module is skipped entirely.\n",
        },
    )
    metric_prefix: Optional[str] = Field(
        None,
        description="Optional prefix (not used for correlation rows).",
        json_schema_extra={
            "long_doc": "Prefix applied to output names when using matrix output.\n"
            "Rows output ignores prefixes by design.\n",
        },
    )
    source_pass: str = Field(
        "pass1",
        description="Pass name to use as input.",
        json_schema_extra={
            "long_doc": "Selects which pass output is analyzed.\n"
            "Use a pass containing the metrics you want to correlate.\n"
            "The module reads its output directly from the pass context.\n",
        },
    )
    columns: Optional[List[str]] = Field(
        None,
        description="Columns to include (defaults to numeric columns).",
        json_schema_extra={
            "long_doc": "Explicit list of series to correlate.\n"
            "If omitted, all numeric columns (excluding group keys) are used.\n"
            "Non-numeric columns are ignored.\n",
        },
    )
    group_by: List[str] = Field(
        default_factory=lambda: ["ListingId"],
        description="Grouping columns for correlation matrices.",
        json_schema_extra={
            "long_doc": "Groups define independent correlation matrices.\n"
            "Typical grouping is per ListingId, optionally with TimeBucket.\n"
            "Each group yields a full matrix or rows depending on output_format.\n",
        },
    )
    method: Literal["pearson", "spearman", "soft"] = Field(
        "pearson",
        description="Correlation method.",
        json_schema_extra={
            "long_doc": "Pearson and Spearman use standard Pandas correlation.\n"
            "Soft correlation emphasizes magnitude patterns using `soft_*` settings.\n"
            "Soft mode is useful for momentum-like alignment across metrics.\n",
        },
    )
    output_format: Literal["rows", "matrix"] = Field(
        "rows",
        description="Output as rows (MetricX/MetricY/Corr) or embedded matrix.",
        json_schema_extra={
            "long_doc": "Rows format yields one row per metric pair.\n"
            "Matrix format yields a single row per group with metrics and matrix.\n"
            "Rows are easier to join downstream; matrices are compact for inspection.\n",
        },
    )
    soft_alpha: float = Field(
        1.0,
        description="Soft-correlation smoothing power (only for method='soft').",
        json_schema_extra={
            "long_doc": "Controls how strongly large moves dominate the correlation.\n"
            "alpha > 1 emphasizes large deviations, alpha < 1 smooths them.\n"
            "Only used when method is 'soft'.\n",
        },
    )
    soft_sign_only: bool = Field(
        False,
        description="Use only the sign of changes (method='soft').",
        json_schema_extra={
            "long_doc": "If True, soft correlation uses only direction, not magnitude.\n"
            "This is similar to correlating signed returns.\n",
        },
    )
    soft_recenter: bool = Field(
        False,
        description="Center each series before soft correlation.",
        json_schema_extra={
            "long_doc": "If True, subtracts the mean from each series before correlation.\n"
            "Useful when series have persistent offsets.\n",
        },
    )
    soft_normalisation: Literal["l2", "mean", "max", "none"] = Field(
        "l2",
        description="Per-series normalisation for soft correlation.",
        json_schema_extra={
            "long_doc": "Selects how each series is scaled before correlation.\n"
            "l2 approximates standard correlation; mean/max emphasize relative moves.\n",
        },
    )
    soft_multiresolution: Optional[List[int]] = Field(
        None,
        description="Optional block sizes for multi-resolution soft correlation.",
        json_schema_extra={
            "long_doc": "If set, averages correlations over block-averaged resolutions.\n"
            "Each value is a block size used to downsample the series.\n",
        },
    )
    soft_num_periods_rolling: Optional[int] = Field(
        None,
        description="Optional number of rolling tail windows for soft correlation.",
        json_schema_extra={
            "long_doc": "If set, averages correlations over shrinking tail windows.\n"
            "This emphasizes more recent data while retaining longer context.\n",
        },
    )
    soft_cumulative_product: bool = Field(
        False,
        description="Apply soft correlation on cumulative product series.",
        json_schema_extra={
            "long_doc": "If True, treats inputs as returns and builds a price-like series.\n"
            "Useful when you want correlation on compounded paths.\n",
        },
    )
    soft_cumulative_product_renormalise: Optional[Literal["mean", "min"]] = Field(
        None,
        description="Renormalise cumulative product series by mean or min.",
        json_schema_extra={
            "long_doc": "Rescales cumulative products to reduce drift before correlation.\n"
            "Only used when cumulative product mode is enabled.\n",
        },
    )
    soft_full_norm_alpha_scale: bool = Field(
        True,
        description="Rescale soft-correlation back by alpha after smoothing.",
        json_schema_extra={
            "long_doc": "If True, rescales the smoothed correlation using alpha.\n"
            "This keeps the output range comparable to standard correlations.\n",
        },
    )


@analytic_doc(
    module="correlation",
    pattern=r"^MetricX$",
    template="First metric name in the correlation pair.",
    unit="Label",
    description="Source metric identifier for the left axis of the pair.",
)
@analytic_doc(
    module="correlation",
    pattern=r"^MetricY$",
    template="Second metric name in the correlation pair.",
    unit="Label",
    description="Source metric identifier for the right axis of the pair.",
)
@analytic_doc(
    module="correlation",
    pattern=r"^Corr$",
    template="Pairwise correlation coefficient between MetricX and MetricY.",
    unit="Correlation",
    description=(
        "Correlation coefficient in [-1, 1] computed per configured grouping "
        "and method."
    ),
)
def _register_correlation_docs():
    return None


@register_analytics("correlation", config_attr="correlation_analytics")
class CorrelationAnalytics(BaseAnalytics):
    REQUIRES: List[str] = []

    def __init__(self, config: CorrelationAnalyticsConfig):
        super().__init__(
            "correlation",
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
            grouped = pdf.groupby(group_cols, dropna=False)
            groups = grouped
        else:
            groups = [((), pdf)]

        for key, frame in groups:
            if self.config.method in {"pearson", "spearman"}:
                corr = frame[cols].corr(method=self.config.method)
            else:
                values = frame[cols].to_numpy().T
                corr = soft_corr_matrix(
                    values,
                    alpha=self.config.soft_alpha,
                    sign_only=self.config.soft_sign_only,
                    recenter=self.config.soft_recenter,
                    normalisation=self.config.soft_normalisation,
                    multiresolution=self.config.soft_multiresolution,
                    num_periods_rolling=self.config.soft_num_periods_rolling,
                    cumulative_product=self.config.soft_cumulative_product,
                    cumulative_product_renormalise=(
                        self.config.soft_cumulative_product_renormalise
                    ),
                    full_norm_alpha_scale=self.config.soft_full_norm_alpha_scale,
                )
                corr = pd.DataFrame(corr, index=cols, columns=cols)
            if self.config.output_format == "matrix":
                payload = {
                    "metrics": cols,
                    "corr_matrix": corr.values.tolist(),
                }
                row = {}
                if group_cols:
                    if not isinstance(key, tuple):
                        key = (key,)
                    row.update(dict(zip(group_cols, key)))
                row.update(payload)
                rows.append(row)
            else:
                for i, c1 in enumerate(cols):
                    for j, c2 in enumerate(cols):
                        row = {
                            "MetricX": c1,
                            "MetricY": c2,
                            "Corr": corr.iloc[i, j],
                        }
                        if group_cols:
                            if not isinstance(key, tuple):
                                key = (key,)
                            row.update(dict(zip(group_cols, key)))
                        rows.append(row)

        return pl.from_pandas(pd.DataFrame(rows)).lazy()
