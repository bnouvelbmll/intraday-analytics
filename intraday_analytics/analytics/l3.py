import polars as pl
from intraday_analytics.bases import BaseAnalytics
from pydantic import BaseModel, Field
from typing import List, Union, Literal, Optional, Set, Dict, Any

from .common import CombinatorialMetricConfig, Side, AggregationMethod, apply_aggregation
from .utils import apply_alias
from .metric_base import AnalyticSpec, AnalyticContext, AnalyticDoc


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
            pattern=r"^ArrivalFlowImbalance$",
            template="Normalized difference between bid-side and ask-side insert volume per TimeBucket.",
            unit="Imbalance",
        ),
        AnalyticDoc(
            pattern=r"^CancelToTradeRatio$",
            template="Ratio of cancellations to executions per TimeBucket.",
            unit="Percentage",
        ),
        AnalyticDoc(
            pattern=r"^AvgQueuePosition$",
            template="Mean SizeAhead for executions within the TimeBucket.",
            unit="Shares",
        ),
        AnalyticDoc(
            pattern=r"^AvgRestingTime$",
            template="Mean lifetime (ns) of orders from insert to end within the TimeBucket.",
            unit="Nanoseconds",
        ),
        AnalyticDoc(
            pattern=r"^FleetingLiquidityRatio$",
            template="Share of inserted volume that is cancelled within the fleeting threshold in the TimeBucket.",
            unit="Percentage",
        ),
        AnalyticDoc(
            pattern=r"^AvgReplacementLatency$",
            template="Mean latency (ns) between an execution and the next insert on the same side within the TimeBucket.",
            unit="Nanoseconds",
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
                )
            )
        return outputs


class L3AdvancedAnalytic(AnalyticSpec):
    MODULE = "l3"
    ConfigModel = L3AdvancedConfig

    def expressions(
        self, ctx: AnalyticContext, config: L3AdvancedConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        return []

    def apply(
        self,
        base_df: pl.LazyFrame,
        l3: pl.LazyFrame,
        advanced_variants: List[Dict[str, Any]],
    ) -> pl.LazyFrame:
        gcols = ["ListingId", "TimeBucket"]

        for adv in advanced_variants:
            v = adv["variant"]
            output_name_pattern = adv.get("_output_name_pattern")

            if v == "ArrivalFlowImbalance":
                pattern = adv.get("_output_name_pattern")
                col_name = pattern.format(**adv) if pattern else "ArrivalFlowImbalance"
                base_df = base_df.with_columns(
                    (
                        (pl.col("InsertVolumeBid") - pl.col("InsertVolumeAsk"))
                        / (pl.col("InsertVolumeBid") + pl.col("InsertVolumeAsk") + 1e-9)
                    ).alias(col_name)
                )

            elif v == "CancelToTradeRatio":
                exec_counts = (
                    l3.filter(pl.col("ExecutionSize") > 0)
                    .group_by(gcols)
                    .agg(pl.len().alias("ExecCount"))
                )
                base_df = base_df.join(exec_counts, on=gcols, how="left").with_columns(
                    pl.col("ExecCount").fill_null(0)
                )

                pattern = adv.get("_output_name_pattern")
                col_name = pattern.format(**adv) if pattern else "CancelToTradeRatio"
                base_df = base_df.with_columns(
                    (
                        (pl.col("RemoveCountBid") + pl.col("RemoveCountAsk"))
                        / (pl.col("ExecCount") + 1e-9)
                    ).alias(col_name)
                ).drop("ExecCount")

            elif v == "AvgQueuePosition":
                queue_pos = (
                    l3.filter(pl.col("ExecutionSize") > 0)
                    .group_by(gcols)
                    .agg(pl.col("SizeAhead").mean().alias("AvgQueuePosition"))
                )
                pattern = adv.get("_output_name_pattern")
                col_name = pattern.format(**adv) if pattern else "AvgQueuePosition"
                if col_name != "AvgQueuePosition":
                    queue_pos = queue_pos.rename({"AvgQueuePosition": col_name})
                base_df = base_df.join(queue_pos, on=gcols, how="left")

            elif v == "AvgRestingTime":
                resting_metrics = (
                    self._compute_lifetimes(l3, adv, adv.get("market_states"))
                    .group_by(gcols)
                    .agg(pl.col("DurationNs").mean().alias("AvgRestingTime"))
                )
                pattern = adv.get("_output_name_pattern")
                col_name = pattern.format(**adv) if pattern else "AvgRestingTime"
                if col_name != "AvgRestingTime":
                    resting_metrics = resting_metrics.rename({"AvgRestingTime": col_name})
                base_df = base_df.join(resting_metrics, on=gcols, how="left")

            elif v == "FleetingLiquidityRatio":
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
                pattern = adv.get("_output_name_pattern")
                col_name = pattern.format(**adv) if pattern else "FleetingLiquidityRatio"
                base_df = base_df.with_columns(
                    (
                        pl.col("FleetingVolume")
                        / (pl.col("InsertVolumeBid") + pl.col("InsertVolumeAsk") + 1e-9)
                    ).alias(col_name)
                ).drop("FleetingVolume")

            elif v == "AvgReplacementLatency":
                latency = self._compute_replacement_latency(
                    l3, adv.get("market_states")
                )
                pattern = adv.get("_output_name_pattern")
                col_name = pattern.format(**adv) if pattern else "AvgReplacementLatency"
                if col_name != "AvgReplacementLatency":
                    latency = latency.rename({"AvgReplacementLatency": col_name})
                base_df = base_df.join(latency, on=gcols, how="left")

        return base_df

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


class L3Analytics(BaseAnalytics):
    """
    Computes L3 order book event analytics.
    Fully supports combinatorial configuration and advanced analytics.
    """

    REQUIRES = ["l3"]

    def __init__(self, config: L3AnalyticsConfig):
        self.config = config
        super().__init__("l3", {})

    def compute(self) -> pl.LazyFrame:
        gcols = ["ListingId", "TimeBucket"]
        ctx = AnalyticContext(base_df=self.l3, cache={}, context=self.context)

        requested_generics = []
        for req in self.config.generic_metrics:
            requested_generics.extend(req.expand())
        requested_aliases = {
            req.get("output_name_pattern")
            or f"{req['actions']}{req['measures']}{req['sides']}"
            for req in requested_generics
        }

        advanced_variants = []
        for req in self.config.advanced_metrics:
            for variant in req.expand():
                variant["_output_name_pattern"] = req.output_name_pattern
                advanced_variants.append(variant)

        required_intermediates = set()
        for adv in advanced_variants:
            v = adv["variant"]
            if v == "ArrivalFlowImbalance":
                required_intermediates.add("InsertVolumeBid")
                required_intermediates.add("InsertVolumeAsk")
            elif v == "CancelToTradeRatio":
                required_intermediates.add("RemoveCountBid")
                required_intermediates.add("RemoveCountAsk")
            elif v == "FleetingLiquidityRatio":
                required_intermediates.add("InsertVolumeBid")
                required_intermediates.add("InsertVolumeAsk")

        generic_analytic = L3GenericAnalytic()
        expressions: List[pl.Expr] = []

        required_map = {
            "InsertVolumeBid": {"sides": "Bid", "actions": "Insert", "measures": "Volume"},
            "InsertVolumeAsk": {"sides": "Ask", "actions": "Insert", "measures": "Volume"},
            "RemoveCountBid": {"sides": "Bid", "actions": "Remove", "measures": "Count"},
            "RemoveCountAsk": {"sides": "Ask", "actions": "Remove", "measures": "Count"},
            "InsertCountBid": {"sides": "Bid", "actions": "Insert", "measures": "Count"},
            "InsertCountAsk": {"sides": "Ask", "actions": "Insert", "measures": "Count"},
        }
        for name in required_intermediates:
            if name in requested_aliases:
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
                exprs = generic_analytic.expressions(ctx, dummy_cfg, variant)
                for expr in exprs:
                    expressions.append(expr.alias(name))

        for req in self.config.generic_metrics:
            for variant in generic_analytic.expand_config(req):
                expressions.extend(generic_analytic.expressions(ctx, req, variant))

        if expressions:
            base_df = self.l3.group_by(gcols).agg(expressions)
        else:
            base_df = self.l3.group_by(gcols).agg(pl.len().alias("_count"))

        advanced_analytic = L3AdvancedAnalytic()
        base_df = advanced_analytic.apply(base_df, self.l3, advanced_variants)

        self.df = base_df
        return base_df

