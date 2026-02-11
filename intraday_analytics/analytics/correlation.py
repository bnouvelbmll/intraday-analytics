from __future__ import annotations

from typing import List, Optional, Literal

import pandas as pd
import polars as pl
from pydantic import BaseModel, Field

from intraday_analytics.analytics_base import BaseAnalytics
from intraday_analytics.analytics_registry import register_analytics


class CorrelationAnalyticsConfig(BaseModel):
    """
    Correlation matrix analytics on a prior pass output.
    """

    ENABLED: bool = True
    metric_prefix: Optional[str] = Field(
        None,
        description="Optional prefix (not used for correlation rows).",
    )
    source_pass: str = Field(
        "pass1",
        description="Pass name to use as input.",
    )
    columns: Optional[List[str]] = Field(
        None,
        description="Columns to include (defaults to numeric columns).",
    )
    group_by: List[str] = Field(
        default_factory=lambda: ["ListingId"],
        description="Grouping columns for correlation matrices.",
    )
    method: Literal["pearson", "spearman"] = Field(
        "pearson",
        description="Correlation method.",
    )
    output_format: Literal["rows", "matrix"] = Field(
        "rows",
        description="Output as rows (MetricX/MetricY/Corr) or embedded matrix.",
    )


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
            corr = frame[cols].corr(method=self.config.method)
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
