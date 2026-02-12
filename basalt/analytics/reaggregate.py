from __future__ import annotations

from typing import List, Optional, Literal
import logging

import polars as pl
from pydantic import BaseModel, Field, ConfigDict

from basalt.analytics_base import (
    BaseAnalytics,
    apply_overrides,
    default_hint_for_column,
)
from basalt.analytics_registry import register_analytics

logger = logging.getLogger(__name__)


class ReaggregateAnalyticsConfig(BaseModel):
    """
    Reaggregate postprocessing configuration.

    Aggregates the output of a previous pass by joining a group dataframe
    (e.g. ListingId -> IndexId) and applying default aggregation hints for
    each metric column.

    Usage examples
    --------------
    Context-provided group dataframe:
        reaggregate_analytics:
          source_pass: pass1
          group_df_context_key: index_membership
          join_column: ListingId
          group_column: IndexId
          group_by: [IndexId, TimeBucket]

    Group dataframe as prior pass output:
        reaggregate_analytics:
          source_pass: pass1
          group_df_pass: pass0
          join_column: ListingId
          group_column: IndexId
          group_by: [IndexId, TimeBucket]
    """

    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "reaggregate",
                "tier": "post",
                "desc": "Postprocessing: reaggregate previous pass by a group map.",
                "outputs": ["IndexAggregation"],
                "schema_keys": ["reaggregate"],
            }
        }
    )

    ENABLED: bool = Field(
        True,
        description="Enable or disable reaggregation analytics for this pass.",
    )
    metric_prefix: Optional[str] = Field(
        None,
        description="Prefix for reaggregated metric columns.",
        json_schema_extra={
            "long_doc": "Prepended to all reaggregated output columns.\n"
            "Use to distinguish index-level metrics from listing-level metrics.\n"
            "Leave empty to keep original column names.\n"
        },
    )
    source_pass: str = Field(
        "pass1",
        description="Pass name to use as input.",
        json_schema_extra={
            "long_doc": "Selects which previous pass output to reaggregate.\n"
            "The source pass output must be present in the pipeline context.\n"
            "Typically this is the first pass with raw analytics.\n"
        },
    )
    group_df_context_key: str = Field(
        "group_df",
        description="Context key where the group dataframe is stored.",
        json_schema_extra={
            "long_doc": "The group dataframe maps join_column -> group_column.\n"
            "Example: ListingId -> IndexId.\n"
        },
    )
    group_df_pass: Optional[str] = Field(
        None,
        description="Pass name to use as the group dataframe.",
        json_schema_extra={
            "long_doc": "If set, uses the output of a previous pass as the group dataframe.\n"
            "This pass output must include the join_column and group_column.\n"
            "When provided, this takes precedence over group_df_context_key.\n"
            "\n"
            "Example:\n"
            "  group_df_pass: pass0\n"
        },
    )
    join_column: str = Field(
        "ListingId",
        description="Join column in the source frame.",
        json_schema_extra={
            "long_doc": "Used to join the source pass output with the group dataframe.\n"
        },
    )
    group_column: str = Field(
        "IndexId",
        description="Group column in the group dataframe.",
        json_schema_extra={
            "long_doc": "The target grouping column (e.g., IndexId).\n"
        },
    )
    group_by: List[str] = Field(
        default_factory=lambda: ["IndexId", "TimeBucket"],
        description="Group-by columns after join.",
        json_schema_extra={
            "long_doc": "Defines grouping keys for the reaggregation.\n"
            "Must include TimeBucket to keep per-timebucket outputs.\n"
        },
    )
    resample_rule: Optional[str] = Field(
        None,
        description="Optional resample rule (e.g. '15m').",
        json_schema_extra={
            "long_doc": "If set, truncates TimeBucket to this rule before aggregation.\n"
            "Example: '15m' aggregates 1-minute outputs into 15-minute buckets.\n"
        },
    )
    weight_col: Optional[str] = Field(
        "TradeNotionalEUR",
        description="Base weight column used for NotionalWeighted aggregations.",
        json_schema_extra={
            "long_doc": "Used when hints select NotionalWeighted aggregation.\n"
            "By default, daily weights are derived from this column.\n"
            "If missing, falls back to mean aggregation.\n"
        },
    )
    weight_mode: Literal["daily", "bucket"] = Field(
        "daily",
        description="Weight scope for NotionalWeighted: 'daily' (default) or 'bucket'.",
        json_schema_extra={
            "long_doc": "daily: derive one weight per join_column and trading day from weight_col.\n"
            "bucket: use the per-row bucket value directly (legacy behavior).\n"
            "daily is recommended to avoid unstable intraday weighting.\n"
        },
    )


@register_analytics("reaggregate", config_attr="reaggregate_analytics")
class ReaggregateAnalytics(BaseAnalytics):
    """
    Postprocessing reaggregate module.
    """

    REQUIRES: List[str] = []

    def __init__(self, config: ReaggregateAnalyticsConfig):
        super().__init__(
            "reaggregate",
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
        if isinstance(source_df, pl.DataFrame):
            lf = source_df.lazy()
        else:
            lf = source_df

        group_df = None
        if self.config.group_df_pass:
            group_df = self.context.get(self.config.group_df_pass)
        if group_df is None:
            group_df = self.context.get(self.config.group_df_context_key)
        if group_df is None:
            raise ValueError(
                "Group dataframe not found. Set group_df_pass or provide a context "
                f"entry for '{self.config.group_df_context_key}'."
            )
        if isinstance(group_df, pl.DataFrame):
            gf = group_df.lazy()
        else:
            gf = group_df

        lf = lf.join(
            gf.select([self.config.join_column, self.config.group_column]),
            on=self.config.join_column,
            how="left",
        )

        if self.config.resample_rule and "TimeBucket" in lf.collect_schema().names():
            lf = lf.with_columns(
                pl.col("TimeBucket").dt.truncate(self.config.resample_rule)
            )

        group_by = self.config.group_by or [self.config.group_column, "TimeBucket"]
        if self.config.weight_col and self.config.weight_mode == "daily":
            schema_cols = lf.collect_schema().names()
            if (
                self.config.weight_col in schema_cols
                and self.config.join_column in schema_cols
                and "TimeBucket" in schema_cols
            ):
                lf = lf.with_columns(
                    __WeightDate=pl.col("TimeBucket").dt.date()
                ).with_columns(
                    __NotionalWeightDaily=pl.col(self.config.weight_col)
                    .sum()
                    .over([self.config.join_column, "__WeightDate"])
                )
            else:
                logger.warning(
                    "Reaggregate daily weights requested but required columns are missing; "
                    "falling back to bucket weights."
                )
        cols = lf.collect_schema().names()

        key_cols = {
            self.config.join_column,
            self.config.group_column,
            "TimeBucket",
            "ListingId",
            "InstrumentId",
            "MIC",
            "Ticker",
            "CurrencyCode",
        }
        # Any explicit grouping key should be treated as an identity column and
        # excluded from metric aggregation (it will be preserved by group_by).
        key_cols.update(group_by)
        metric_cols = [c for c in cols if c not in key_cols and not c.startswith("__")]

        exprs: List[pl.Expr] = []
        for col in metric_cols:
            override = apply_overrides("reaggregate", col, self.config.weight_col)
            hint = override if override else default_hint_for_column(
                col, self.config.weight_col
            )
            agg = hint["default_agg"]
            weight_col = hint["weight_col"]
            if (
                agg in {"NotionalWeighted", "VWA", "TWA"}
                and self.config.weight_mode == "daily"
                and "__NotionalWeightDaily" in cols
            ):
                weight_col = "__NotionalWeightDaily"

            if agg in {"Sum"}:
                expr = pl.col(col).sum()
            elif agg in {"Mean", "Avg"}:
                expr = pl.col(col).mean()
            elif agg == "Last":
                expr = pl.col(col).last()
            elif agg == "First":
                expr = pl.col(col).first()
            elif agg == "Min":
                expr = pl.col(col).min()
            elif agg == "Max":
                expr = pl.col(col).max()
            elif agg == "Median":
                expr = pl.col(col).median()
            elif agg == "Std":
                expr = pl.col(col).std()
            elif agg in {"NotionalWeighted", "VWA", "TWA"} and weight_col and weight_col in cols:
                expr = (pl.col(col) * pl.col(weight_col)).sum() / pl.col(weight_col).sum()
            elif agg == "NotAggregated":
                continue
            else:
                expr = pl.col(col).mean()

            exprs.append(expr.alias(self.apply_prefix(col)))

        return lf.group_by(group_by).agg(exprs)
