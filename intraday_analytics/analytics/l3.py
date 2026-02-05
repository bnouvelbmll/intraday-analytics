import polars as pl
from intraday_analytics.analytics_base import BaseAnalytics
from pydantic import BaseModel, Field
from typing import List, Union, Literal, Optional, Set, Dict, Any
import logging

from .common import CombinatorialMetricConfig, Side, AggregationMethod, apply_aggregation
from .utils import apply_alias
from intraday_analytics.analytics_base import (
    AnalyticSpec,
    AnalyticContext,
    AnalyticDoc,
    analytic_expression,
    build_expressions,
)
from intraday_analytics.analytics_registry import register_analytics


# =============================
# Configuration Models
# =============================

L3Action = Literal["Insert", "Remove", "Update", "UpdateInserted", "UpdateRemoved"]
L3Measure = Literal["Count", "Volume"]


class L3MetricConfig(CombinatorialMetricConfig):
    """
    Basic L3 Event Analytics (Counts and Volumes).
    """

    metric_type: Literal["L3_Generic"] = "L3_Generic"

    sides: Union[Side, List[Side]] = Field(..., description="Side of the book.")

    actions: Union[L3Action, List[L3Action]] = Field(
        ..., description="LOB Action type."
    )

    measures: Union[L3Measure, List[L3Measure]] = Field(
        ..., description="Measure to compute (Count or Volume)."
    )
    aggregations: List[AggregationMethod] = Field(
        default_factory=lambda: ["Sum"],
        description="Aggregations to apply to the analytic series.",
    )


class L3AdvancedConfig(CombinatorialMetricConfig):
    """
    Advanced L3 Analytics (Fleeting Liquidity, Latency, etc.)
    """

    metric_type: Literal["L3_Advanced"] = "L3_Advanced"

    variant: Union[
        Literal[
            "ArrivalFlowImbalance",
            "CancelToTradeRatio",
            "AvgQueuePosition",
            "AvgRestingTime",
            "FleetingLiquidityRatio",
            "AvgReplacementLatency",
        ],
        List[str],
    ] = Field(..., description="Advanced analytic type.")

    fleeting_threshold_ms: Optional[int] = Field(
        100, description="Threshold for fleeting liquidity."
    )


class L3AnalyticsConfig(BaseModel):
    ENABLED: bool = True
    metric_prefix: Optional[str] = None
    generic_metrics: List[L3MetricConfig] = Field(default_factory=list)
    advanced_metrics: List[L3AdvancedConfig] = Field(default_factory=list)


# =============================
# Analytic Specs
# =============================


class L3GenericAnalytic(AnalyticSpec):
    MODULE = "l3"
    DOCS = [
        AnalyticDoc(
            pattern=r"^(?P<action>Insert|Remove|Update|UpdateInserted|UpdateRemoved)(?P<measure>Count)(?P<side>Bid|Ask)(?P<agg>First|Last|Min|Max|Mean|Sum|Median|Std)?$",
            template="{measure} of {action} L3 events on {side} side; aggregated by {agg_or_sum} within TimeBucket.",
            unit="Orders",
        ),
        AnalyticDoc(
            pattern=r"^(?P<action>Insert|Remove|Update|UpdateInserted|UpdateRemoved)(?P<measure>Volume)(?P<side>Bid|Ask)(?P<agg>First|Last|Min|Max|Mean|Sum|Median|Std)?$",
            template="{measure} of {action} L3 events on {side} side; aggregated by {agg_or_sum} within TimeBucket.",
            unit="Shares",
        ),
        AnalyticDoc(
            pattern=r"^_count$",
            template="Count of L3 events in the TimeBucket.",
            unit="Orders",
        ),
    ]
    ConfigModel = L3MetricConfig

    def expressions(
        self, ctx: AnalyticContext, config: L3MetricConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        side = variant["sides"]
        action = variant["actions"]
        measure = variant["measures"]

        side_val = 1 if side == "Bid" else 2
        side_filter = pl.col("Side") == side_val

        if action == "Insert":
            action_filter = pl.col("LobAction") == 2
            vol_col = "Size"
        elif action == "Remove":
            action_filter = pl.col("LobAction") == 3
            vol_col = "OldSize"
        elif action == "Update":
            action_filter = pl.col("LobAction") == 4
            vol_col = None
        elif action == "UpdateInserted":
            action_filter = pl.col("LobAction") == 4
            vol_col = "Size"
        elif action == "UpdateRemoved":
            action_filter = pl.col("LobAction") == 4
            vol_col = "OldSize"
        else:
            return []

        cond = side_filter & action_filter
        if config.market_states:
            cond = cond & pl.col("MarketState").is_in(config.market_states)

        if measure == "Count":
            expr = pl.when(cond).then(1).otherwise(0)
        elif measure == "Volume":
            col = pl.col(vol_col) if vol_col else pl.lit(0)
            expr = pl.when(cond).then(col).otherwise(0)
        else:
            return []

        outputs = []
        for agg in config.aggregations:
            agg_expr = apply_aggregation(expr, agg)
            if agg_expr is None:
                continue
            default_name = f"{action}{measure}{side}"
            if agg != "Sum" and config.output_name_pattern is None:
                default_name = f"{default_name}{agg}"
            outputs.append(
                apply_alias(
                    agg_expr,
                    config.output_name_pattern,
                    {**variant, "agg": agg},
                    default_name,
                    prefix=ctx.cache.get("metric_prefix"),
                )
            )
        return outputs


class L3AdvancedAnalytic(AnalyticSpec):
    MODULE = "l3"
    ConfigModel = L3AdvancedConfig
    REQUIRED_OUTPUTS: Dict[str, List[str]] = {
        "ArrivalFlowImbalance": ["InsertVolumeBid", "InsertVolumeAsk"],
        "CancelToTradeRatio": ["RemoveCountBid", "RemoveCountAsk"],
        "FleetingLiquidityRatio": ["InsertVolumeBid", "InsertVolumeAsk"],
    }

    @analytic_expression(
        "ArrivalFlowImbalance",
        pattern=r"^ArrivalFlowImbalance$",
        unit="Imbalance",
    )
    def _expression_arrival_flow_imbalance(
        self, base_df: pl.LazyFrame, l3: pl.LazyFrame, adv: Dict[str, Any], prefix: str
    ) -> pl.LazyFrame:
        """Normalized difference between bid-side and ask-side insert volume per TimeBucket."""
        col_name = self._output_name(adv, "ArrivalFlowImbalance", prefix)
        bid_col = f"{prefix}InsertVolumeBid" if prefix else "InsertVolumeBid"
        ask_col = f"{prefix}InsertVolumeAsk" if prefix else "InsertVolumeAsk"
        return base_df.with_columns(
            (
                (pl.col(bid_col) - pl.col(ask_col))
                / (pl.col(bid_col) + pl.col(ask_col) + 1e-9)
            ).alias(col_name)
        )

    @analytic_expression(
        "CancelToTradeRatio",
        pattern=r"^CancelToTradeRatio$",
        unit="Percentage",
    )
    def _expression_cancel_to_trade_ratio(
        self, base_df: pl.LazyFrame, l3: pl.LazyFrame, adv: Dict[str, Any], prefix: str
    ) -> pl.LazyFrame:
        """Ratio of cancellations to executions per TimeBucket."""
        gcols = ["ListingId", "TimeBucket"]
        exec_counts = (
            l3.filter(pl.col("ExecutionSize") > 0)
            .group_by(gcols)
            .agg(pl.len().alias("ExecCount"))
        )
        base_df = base_df.join(exec_counts, on=gcols, how="left").with_columns(
            pl.col("ExecCount").fill_null(0)
        )
        col_name = self._output_name(adv, "CancelToTradeRatio", prefix)
        bid_col = f"{prefix}RemoveCountBid" if prefix else "RemoveCountBid"
        ask_col = f"{prefix}RemoveCountAsk" if prefix else "RemoveCountAsk"
        return base_df.with_columns(
            (
                (pl.col(bid_col) + pl.col(ask_col))
                / (pl.col("ExecCount") + 1e-9)
            ).alias(col_name)
        ).drop("ExecCount")

    @analytic_expression(
        "AvgQueuePosition",
        pattern=r"^AvgQueuePosition$",
        unit="Shares",
    )
    def _expression_avg_queue_position(
        self, base_df: pl.LazyFrame, l3: pl.LazyFrame, adv: Dict[str, Any], prefix: str
    ) -> pl.LazyFrame:
        """Mean SizeAhead for executions within the TimeBucket."""
        gcols = ["ListingId", "TimeBucket"]
        queue_pos = (
            l3.filter(pl.col("ExecutionSize") > 0)
            .group_by(gcols)
            .agg(pl.col("SizeAhead").mean().alias("AvgQueuePosition"))
        )
        col_name = self._output_name(adv, "AvgQueuePosition", prefix)
        if col_name != "AvgQueuePosition":
            queue_pos = queue_pos.rename({"AvgQueuePosition": col_name})
        return base_df.join(queue_pos, on=gcols, how="left")

    @analytic_expression(
        "AvgRestingTime",
        pattern=r"^AvgRestingTime$",
        unit="Nanoseconds",
    )
    def _expression_avg_resting_time(
        self, base_df: pl.LazyFrame, l3: pl.LazyFrame, adv: Dict[str, Any], prefix: str
    ) -> pl.LazyFrame:
        """Mean lifetime (ns) of orders from insert to end within the TimeBucket."""
        gcols = ["ListingId", "TimeBucket"]
        resting_metrics = (
            self._compute_lifetimes(l3, adv, adv.get("market_states"))
            .group_by(gcols)
            .agg(pl.col("DurationNs").mean().alias("AvgRestingTime"))
        )
        col_name = self._output_name(adv, "AvgRestingTime", prefix)
        if col_name != "AvgRestingTime":
            resting_metrics = resting_metrics.rename({"AvgRestingTime": col_name})
        return base_df.join(resting_metrics, on=gcols, how="left")

    @analytic_expression(
        "FleetingLiquidityRatio",
        pattern=r"^FleetingLiquidityRatio$",
        unit="Percentage",
    )
    def _expression_fleeting_liquidity_ratio(
        self, base_df: pl.LazyFrame, l3: pl.LazyFrame, adv: Dict[str, Any], prefix: str
    ) -> pl.LazyFrame:
        """Share of inserted volume that is cancelled within the fleeting threshold in the TimeBucket."""
        gcols = ["ListingId", "TimeBucket"]
        threshold_ms = adv.get("fleeting_threshold_ms", 100)
        threshold_ns = threshold_ms * 1_000_000
        lifetimes = self._compute_lifetimes(l3, adv, adv.get("market_states"))
        fleeting_metrics = lifetimes.group_by(gcols).agg(
            (
                pl.when(pl.col("DurationNs") < threshold_ns)
                .then(pl.col("InsertSize"))
                .otherwise(pl.lit(0))
            )
            .sum()
            .alias("FleetingVolume")
        )
        base_df = base_df.join(fleeting_metrics, on=gcols, how="left")
        col_name = self._output_name(adv, "FleetingLiquidityRatio", prefix)
        bid_col = f"{prefix}InsertVolumeBid" if prefix else "InsertVolumeBid"
        ask_col = f"{prefix}InsertVolumeAsk" if prefix else "InsertVolumeAsk"
        return base_df.with_columns(
            (
                pl.col("FleetingVolume")
                / (pl.col(bid_col) + pl.col(ask_col) + 1e-9)
            ).alias(col_name)
        ).drop("FleetingVolume")

    @analytic_expression(
        "AvgReplacementLatency",
        pattern=r"^AvgReplacementLatency$",
        unit="Nanoseconds",
    )
    def _expression_avg_replacement_latency(
        self, base_df: pl.LazyFrame, l3: pl.LazyFrame, adv: Dict[str, Any], prefix: str
    ) -> pl.LazyFrame:
        """Mean latency (ns) between an execution and the next insert on the same side within the TimeBucket."""
        gcols = ["ListingId", "TimeBucket"]
        latency = self._compute_replacement_latency(l3, adv.get("market_states"))
        col_name = self._output_name(adv, "AvgReplacementLatency", prefix)
        if col_name != "AvgReplacementLatency":
            latency = latency.rename({"AvgReplacementLatency": col_name})
        return base_df.join(latency, on=gcols, how="left")

    def expressions(
        self, ctx: AnalyticContext, config: L3AdvancedConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        return []

    def apply(
        self,
        base_df: pl.LazyFrame,
        l3: pl.LazyFrame,
        advanced_variants: List[Dict[str, Any]],
        prefix: str,
    ) -> pl.LazyFrame:
        required_outputs = self._collect_required_outputs(advanced_variants)
        for adv in advanced_variants:
            v = adv["variant"]
            expression_fn = self.EXPRESSIONS.get(v)
            if expression_fn is None:
                continue
            base_df = expression_fn(self, base_df, l3, adv, prefix)

        if required_outputs:
            cols = base_df.collect_schema().names()
            missing = [name for name in required_outputs if name not in cols]
            if missing:
                logging.warning(
                    "Missing required L3 intermediate outputs for advanced analytics: %s",
                    ", ".join(missing),
                )

        return base_df

    @staticmethod
    def _output_name(adv: Dict[str, Any], default: str, prefix: str) -> str:
        pattern = adv.get("_output_name_pattern")
        col_name = pattern.format(**adv) if pattern else default
        if prefix:
            return f"{prefix}{col_name}"
        return col_name

    def _collect_required_outputs(self, advanced_variants: List[Dict[str, Any]]) -> List[str]:
        required: Set[str] = set()
        for adv in advanced_variants:
            for name in self.REQUIRED_OUTPUTS.get(adv.get("variant"), []):
                required.add(name)
        return list(required)

    def _compute_lifetimes(
        self, l3: pl.LazyFrame, config_dict: Dict[str, Any], market_states: Optional[List[str]] = None
    ):
        if market_states:
            l3 = l3.filter(pl.col("MarketState").is_in(market_states))

        inserts = (
            l3.filter(pl.col("LobAction") == 2)
            .select(["ListingId", "OrderID", "EventTimestamp", "Size", "TimeBucket"])
            .rename({"EventTimestamp": "InsertTime", "Size": "InsertSize"})
        )

        end_events = (
            l3.filter(pl.col("LobAction").is_in([1, 3]))
            .select(["ListingId", "OrderID", "EventTimestamp", "TimeBucket"])
            .rename({"EventTimestamp": "EndTime"})
        )

        lifetimes = inserts.join(end_events, on=["ListingId", "OrderID"], how="inner")
        return lifetimes.with_columns(
            (pl.col("EndTime") - pl.col("InsertTime"))
            .dt.total_nanoseconds()
            .alias("DurationNs")
        )

    def _compute_replacement_latency(
        self, l3: pl.LazyFrame, market_states: Optional[List[str]] = None
    ):
        if market_states:
            l3 = l3.filter(pl.col("MarketState").is_in(market_states))
        events = (
            l3.filter((pl.col("LobAction") == 2) | (pl.col("ExecutionSize") > 0))
            .select(
                [
                    "ListingId",
                    "TimeBucket",
                    "Side",
                    "LobAction",
                    "EventTimestamp",
                    "ExecutionSize",
                ]
            )
            .sort(["ListingId", "Side", "EventTimestamp"])
        )

        events = events.with_columns(
            pl.col("EventTimestamp")
            .shift(-1)
            .over(["ListingId", "Side"])
            .alias("NextEventTime"),
            pl.col("LobAction")
            .shift(-1)
            .over(["ListingId", "Side"])
            .alias("NextLobAction"),
        )

        return (
            events.filter(
                (pl.col("ExecutionSize") > 0) & (pl.col("NextLobAction") == 2)
            )
            .with_columns(
                (pl.col("NextEventTime") - pl.col("EventTimestamp"))
                .dt.total_nanoseconds()
                .alias("LatencyNs")
            )
            .group_by(["ListingId", "TimeBucket"])
            .agg(pl.col("LatencyNs").mean().alias("AvgReplacementLatency"))
        )


# =============================
# Analytics Module
# =============================


@register_analytics("l3", config_attr="l3_analytics")
class L3Analytics(BaseAnalytics):
    """
    Computes L3 order book event analytics.
    Fully supports combinatorial configuration and advanced analytics.
    """

    REQUIRES = ["l3"]

    def __init__(self, config: L3AnalyticsConfig):
        self.config = config
        super().__init__("l3", {}, metric_prefix=config.metric_prefix)

    def compute(self) -> pl.LazyFrame:
        ctx = self._build_context()
        generic_analytic = L3GenericAnalytic()
        expressions = self._build_generic_expressions(ctx, generic_analytic)
        advanced_variants = self._collect_advanced_variants()
        required_intermediates = self._required_intermediates(advanced_variants)

        if not expressions and not advanced_variants:
            expressions = self._default_expressions(ctx, generic_analytic)

        if required_intermediates:
            self._inject_required_intermediates(
                ctx, generic_analytic, expressions, required_intermediates
            )

        base_df = self._aggregate_base(expressions)
        base_df = self._apply_advanced(base_df, advanced_variants)

        self.df = base_df
        return base_df

    def _build_context(self) -> AnalyticContext:
        """Build the context used by L3 analytics for expression generation."""
        return AnalyticContext(
            base_df=self.l3,
            cache={"metric_prefix": self.metric_prefix},
            context=self.context,
        )

    def _build_generic_expressions(
        self, ctx: AnalyticContext, generic_analytic: L3GenericAnalytic
    ) -> List[pl.Expr]:
        """Generate expressions for configured generic L3 metrics."""
        return build_expressions(ctx, [(generic_analytic, self.config.generic_metrics)])

    def _collect_advanced_variants(self) -> List[Dict[str, Any]]:
        """Expand advanced metric configs into concrete variants with naming info."""
        advanced_variants = []
        for req in self.config.advanced_metrics:
            for variant in req.expand():
                variant["_output_name_pattern"] = req.output_name_pattern
                advanced_variants.append(variant)
        return advanced_variants

    def _required_intermediates(self, advanced_variants: List[Dict[str, Any]]) -> Set[str]:
        """Determine required intermediate generic outputs for advanced metrics."""
        if not advanced_variants:
            return set()
        return set(L3AdvancedAnalytic()._collect_required_outputs(advanced_variants))

    def _default_expressions(
        self, ctx: AnalyticContext, generic_analytic: L3GenericAnalytic
    ) -> List[pl.Expr]:
        """Generate a default generic metric set when no metrics are configured."""
        default_configs = [
            L3MetricConfig(
                sides=["Bid", "Ask"],
                actions=["Insert", "Remove"],
                measures=["Count", "Volume"],
            )
        ]
        return build_expressions(ctx, [(generic_analytic, default_configs)])

    def _inject_required_intermediates(
        self,
        ctx: AnalyticContext,
        generic_analytic: L3GenericAnalytic,
        expressions: List[pl.Expr],
        required_intermediates: Set[str],
    ) -> None:
        """Ensure required generic intermediates exist for advanced metrics."""
        requested_aliases = self._requested_aliases()
        required_map = {
            "InsertVolumeBid": {"sides": "Bid", "actions": "Insert", "measures": "Volume"},
            "InsertVolumeAsk": {"sides": "Ask", "actions": "Insert", "measures": "Volume"},
            "RemoveCountBid": {"sides": "Bid", "actions": "Remove", "measures": "Count"},
            "RemoveCountAsk": {"sides": "Ask", "actions": "Remove", "measures": "Count"},
        }
        for name in required_intermediates:
            prefixed_name = self.apply_prefix(name)
            if prefixed_name in requested_aliases:
                continue
            params = required_map.get(name)
            if not params:
                continue
            dummy_cfg = L3MetricConfig(
                sides=params["sides"],
                actions=params["actions"],
                measures=params["measures"],
                aggregations=["Sum"],
            )
            for variant in generic_analytic.expand_config(dummy_cfg):
                for expr in generic_analytic.expressions(ctx, dummy_cfg, variant):
                    expressions.append(expr.alias(prefixed_name))

    def _requested_aliases(self) -> Set[str]:
        """Collect the set of output aliases already covered by generic configs."""
        requested_aliases = set()
        for req in self.config.generic_metrics:
            for variant in req.expand():
                alias = (
                    req.output_name_pattern.format(**variant)
                    if req.output_name_pattern
                    else f"{variant['actions']}{variant['measures']}{variant['sides']}"
                )
                requested_aliases.add(self.apply_prefix(alias))
        return requested_aliases

    def _aggregate_base(self, expressions: List[pl.Expr]) -> pl.LazyFrame:
        """Aggregate the L3 table using the provided expressions."""
        gcols = ["ListingId", "TimeBucket"]
        if expressions:
            return self.l3.group_by(gcols).agg(expressions)
        return self.l3.group_by(gcols).agg(pl.len().alias("_count"))

    def _apply_advanced(
        self, base_df: pl.LazyFrame, advanced_variants: List[Dict[str, Any]]
    ) -> pl.LazyFrame:
        """Apply advanced analytics to the aggregated base output."""
        if not advanced_variants:
            return base_df
        advanced_analytic = L3AdvancedAnalytic()
        return advanced_analytic.apply(
            base_df,
            self.l3,
            advanced_variants,
            self.metric_prefix,
        )