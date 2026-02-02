import polars as pl
from intraday_analytics.bases import BaseAnalytics
from intraday_analytics.utils import dc
from pydantic import BaseModel, Field
from typing import List, Union, Literal, Optional, Set
from .common import CombinatorialMetricConfig, Side, AggregationMethod

# --- Configuration Models ---

L3Action = Literal["Insert", "Remove", "Update", "UpdateInserted", "UpdateRemoved"]
L3Measure = Literal["Count", "Volume"]


class L3MetricConfig(CombinatorialMetricConfig):
    """
    Basic L3 Event Metrics (Counts and Volumes).
    """

    metric_type: Literal["L3_Generic"] = "L3_Generic"

    sides: Union[Side, List[Side]] = Field(..., description="Side of the book.")

    actions: Union[L3Action, List[L3Action]] = Field(
        ..., description="LOB Action type."
    )

    measures: Union[L3Measure, List[L3Measure]] = Field(
        ..., description="Measure to compute (Count or Volume)."
    )


class L3AdvancedConfig(CombinatorialMetricConfig):
    """
    Advanced L3 Metrics (Fleeting Liquidity, Latency, etc.)
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
    ] = Field(..., description="Advanced metric type.")

    fleeting_threshold_ms: Optional[int] = Field(
        100, description="Threshold for fleeting liquidity."
    )


class L3AnalyticsConfig(BaseModel):
    ENABLED: bool = True
    generic_metrics: List[L3MetricConfig] = Field(default_factory=list)
    advanced_metrics: List[L3AdvancedConfig] = Field(default_factory=list)


class L3Analytics(BaseAnalytics):
    """
    Computes L3 order book event metrics.
    Fully supports combinatorial configuration and advanced metrics.
    """

    REQUIRES = ["l3"]

    def __init__(self, config: L3AnalyticsConfig):
        self.config = config
        # We define fill_cols dynamically based on what's produced,
        # but for BaseAnalytics we might need to know ahead of time.
        # For now, passing empty dict as schema is dynamic.
        super().__init__("l3", {})

    def compute(self) -> pl.LazyFrame:
        gcols = ["ListingId", "TimeBucket"]

        # 1. Identify all required base metrics (Generic)
        # Even if not requested for output, some advanced metrics depend on them.
        # e.g. ArrivalFlowImbalance needs InsertVolumeBid/Ask

        requested_generics = []
        for req in self.config.generic_metrics:
            requested_generics.extend(req.expand())

        # Check dependencies for advanced metrics
        advanced_variants = []
        for req in self.config.advanced_metrics:
            advanced_variants.extend(req.expand())

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

        # Build expressions for generic metrics
        expressions = []

        # Helper to generate expression for a specific metric
        def create_generic_expr(
            side, action, measure, market_states: Optional[List[str]] = None
        ):
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
                return None

            filters = side_filter & action_filter
            if market_states:
                filters = filters & pl.col("MarketState").is_in(market_states)

            if measure == "Count":
                return (pl.when(filters).then(1).otherwise(0)).sum()
            elif measure == "Volume":
                if vol_col:
                    return (pl.when(filters).then(pl.col(vol_col)).otherwise(0)).sum()
            return None

        # Add requested generic metrics
        for req in self.config.generic_metrics:
            for variant in req.expand():
                expr = create_generic_expr(
                    variant["sides"],
                    variant["actions"],
                    variant["measures"],
                    req.market_states,
                )
                if expr is not None:
                    alias = (
                        req.output_name_pattern.format(**variant)
                        if req.output_name_pattern
                        else f"{variant['actions']}{variant['measures']}{variant['sides']}"
                    )
                    expressions.append(expr.alias(alias))

        # Add required intermediates (if not already present)
        # We map the intermediate names to the parameters needed to generate them
        intermediate_map = {
            "InsertVolumeBid": ("Bid", "Insert", "Volume"),
            "InsertVolumeAsk": ("Ask", "Insert", "Volume"),
            "RemoveCountBid": ("Bid", "Remove", "Count"),
            "RemoveCountAsk": ("Ask", "Remove", "Count"),
        }

        existing_aliases = {e.meta.output_name() for e in expressions}

        for name in required_intermediates:
            if name not in existing_aliases:
                params = intermediate_map.get(name)
                if params:
                    expr = create_generic_expr(*params)
                    if expr is not None:
                        expressions.append(expr.alias(name))

        # Execute Base Aggregation
        if expressions:
            base_df = self.l3.group_by(gcols).agg(expressions)
        else:
            # Minimal base if nothing requested
            base_df = self.l3.group_by(gcols).agg(pl.len().alias("_count"))

        # --- Advanced Metrics ---

        for adv in advanced_variants:
            variant = adv["variant"]

            if variant == "ArrivalFlowImbalance":
                base_df = base_df.with_columns(
                    (
                        (pl.col("InsertVolumeBid") - pl.col("InsertVolumeAsk"))
                        / (pl.col("InsertVolumeBid") + pl.col("InsertVolumeAsk") + 1e-9)
                    ).alias("ArrivalFlowImbalance")
                )

            elif variant == "CancelToTradeRatio":
                # Requires ExecCount
                exec_counts = (
                    self.l3.filter(pl.col("ExecutionSize") > 0)
                    .group_by(gcols)
                    .agg(pl.len().alias("ExecCount"))
                )
                base_df = base_df.join(exec_counts, on=gcols, how="left").with_columns(
                    pl.col("ExecCount").fill_null(0)
                )

                base_df = base_df.with_columns(
                    (
                        (pl.col("RemoveCountBid") + pl.col("RemoveCountAsk"))
                        / (pl.col("ExecCount") + 1e-9)
                    ).alias("CancelToTradeRatio")
                ).drop("ExecCount")

            elif variant == "AvgQueuePosition":
                queue_pos = (
                    self.l3.filter(pl.col("ExecutionSize") > 0)
                    .group_by(gcols)
                    .agg(pl.col("SizeAhead").mean().alias("AvgQueuePosition"))
                )
                base_df = base_df.join(queue_pos, on=gcols, how="left")

            elif variant == "AvgRestingTime":
                resting_metrics = (
                    self._compute_lifetimes(adv, adv.get("market_states"))
                    .group_by(gcols)
                    .agg(pl.col("DurationNs").mean().alias("AvgRestingTime"))
                )
                base_df = base_df.join(resting_metrics, on=gcols, how="left")

            elif variant == "FleetingLiquidityRatio":
                threshold_ms = adv.get("fleeting_threshold_ms", 100)
                threshold_ns = threshold_ms * 1_000_000

                lifetimes = self._compute_lifetimes(adv, adv.get("market_states"))
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
                base_df = base_df.with_columns(
                    (
                        pl.col("FleetingVolume")
                        / (pl.col("InsertVolumeBid") + pl.col("InsertVolumeAsk") + 1e-9)
                    ).alias("FleetingLiquidityRatio")
                ).drop("FleetingVolume")

            elif variant == "AvgReplacementLatency":
                latency = self._compute_replacement_latency(adv.get("market_states"))
                base_df = base_df.join(latency, on=gcols, how="left")

        # Cleanup: Remove intermediates that were not explicitly requested
        # (Optional, but cleaner)
        requested_aliases = {
            req.get("output_name_pattern")
            or f"{req['actions']}{req['measures']}{req['sides']}"
            for req in requested_generics
        }
        # Also keep advanced metrics
        for adv in advanced_variants:
            requested_aliases.add(adv["variant"])

        # We might want to keep everything for debugging, or filter.
        # For now, returning everything.

        self.df = base_df
        return base_df

    def _compute_lifetimes(
        self, config_dict, market_states: Optional[List[str]] = None
    ):
        # Helper for Resting Time and Fleeting Liquidity
        l3 = self.l3
        if market_states:
            l3 = l3.filter(pl.col("MarketState").is_in(market_states))

        inserts = (
            l3.filter(pl.col("LobAction") == 2)
            .select(["ListingId", "OrderID", "EventTimestamp", "Size"])
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

    def _compute_replacement_latency(self, market_states: Optional[List[str]] = None):
        l3 = self.l3
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
