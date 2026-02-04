import polars as pl
from intraday_analytics.bases import BaseAnalytics
from pydantic import BaseModel, Field
from typing import List, Union, Literal, Dict, Any

from .common import CombinatorialMetricConfig, Side
from intraday_analytics.analytics_base import AnalyticSpec, AnalyticContext, AnalyticDoc, analytic_handler
from intraday_analytics.analytics_registry import register_analytics


# =============================
# Configuration Models
# =============================

L3ExecutionMeasure = Literal["ExecutedVolume", "VWAP"]


class L3ExecutionConfig(CombinatorialMetricConfig):
    """
    Analytics derived from L3 execution events.
    """

    metric_type: Literal["L3_Execution"] = "L3_Execution"

    sides: Union[Side, List[Side]] = Field(
        ..., description="Side of the execution (Bid/Ask)."
    )
    measures: Union[L3ExecutionMeasure, List[L3ExecutionMeasure]] = Field(
        ..., description="Measure to compute."
    )


TradeType = Literal["LIT", "DARK"]
AggressorSideType = Literal["Buy", "Sell", "Unknown"]
TradeBreakdownMeasure = Literal["Volume", "VWAP", "VWPP"]


class TradeBreakdownConfig(CombinatorialMetricConfig):
    """
    Analytics derived from Trades, broken down by Trade Type and Aggressor Side.
    """

    metric_type: Literal["Trade_Breakdown"] = "Trade_Breakdown"

    trade_types: Union[TradeType, List[TradeType]] = Field(
        ..., description="Trade classification (LIT, DARK)."
    )
    aggressor_sides: Union[AggressorSideType, List[AggressorSideType]] = Field(
        ..., description="Aggressor side (Buy, Sell, Unknown)."
    )
    measures: Union[TradeBreakdownMeasure, List[TradeBreakdownMeasure]] = Field(
        ..., description="Measure (Volume, VWAP, VWPP)."
    )


DerivedMetricVariant = Literal["TradeImbalance"]


class ExecutionDerivedConfig(CombinatorialMetricConfig):
    """
    Analytics derived from other execution analytics.
    """

    metric_type: Literal["Execution_Derived"] = "Execution_Derived"

    variant: Union[DerivedMetricVariant, List[DerivedMetricVariant]] = Field(
        ..., description="Derived analytic name."
    )


class ExecutionAnalyticsConfig(BaseModel):
    ENABLED: bool = True
    l3_execution: List[L3ExecutionConfig] = Field(default_factory=list)
    trade_breakdown: List[TradeBreakdownConfig] = Field(default_factory=list)
    derived_metrics: List[ExecutionDerivedConfig] = Field(default_factory=list)


# =============================
# Analytic Specs
# =============================


class L3ExecutionAnalytic(AnalyticSpec):
    MODULE = "execution"
    ConfigModel = L3ExecutionConfig

    @analytic_handler(
        "ExecutedVolume",
        pattern=r"^ExecutedVolume(?P<side>Bid|Ask)$",
        unit="Shares",
    )
    def _handle_executed_volume(self, cond):
        """Sum of executed volume from L3 executions on {side} side per TimeBucket."""
        return pl.when(cond).then(pl.col("ExecutionSize")).otherwise(0).sum()

    @analytic_handler(
        "VWAP",
        pattern=r"^Vwap(?P<side>Bid|Ask)$",
        unit="XLOC",
    )
    def _handle_vwap(self, cond):
        """VWAP of executions on {side} side per TimeBucket."""
        num = (
            pl.when(cond)
            .then(pl.col("ExecutionPrice") * pl.col("ExecutionSize"))
            .otherwise(0)
        ).sum()
        den = pl.when(cond).then(pl.col("ExecutionSize")).otherwise(0).sum()
        return num / den

    def expressions(
        self, ctx: AnalyticContext, config: L3ExecutionConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        side = variant["sides"]
        measure = variant["measures"]

        side_val = 1 if side == "Bid" else 2
        cond = pl.col("Side") == side_val

        handler = self.HANDLERS.get(measure)
        if handler is None:
            return []
        expr = handler(self, cond)

        if config.output_name_pattern:
            alias = config.output_name_pattern.format(**variant)
        else:
            if measure == "ExecutedVolume":
                alias = f"ExecutedVolume{side}"
            elif measure == "VWAP":
                alias = f"Vwap{side}"
            else:
                alias = f"{measure}{side}"

        return [expr.alias(alias)]


class TradeBreakdownAnalytic(AnalyticSpec):
    MODULE = "execution"
    ConfigModel = TradeBreakdownConfig

    @analytic_handler(
        "Volume",
        pattern=r"^(?P<trade_type>Lit|Dark)(?P<measure>Volume)(?P<agg_side>Buy|Sell|Unknown)Aggressor$",
        unit="Shares",
    )
    def _handle_volume(self, cond):
        """{trade_type} trade {measure} for {agg_side} aggressor side per TimeBucket."""
        return pl.when(cond).then(pl.col("Size")).otherwise(0).sum()

    @analytic_handler(
        "VWAP",
        pattern=r"^(?P<trade_type>Lit|Dark)(?P<measure>VWAP|VolumeWeightedPricePlacement)(?P<agg_side>Buy|Sell|Unknown)Aggressor$",
        unit="XLOC",
    )
    def _handle_vwap(self, cond):
        """{trade_type} trade {measure} for {agg_side} aggressor side per TimeBucket."""
        num = pl.when(cond).then(pl.col("LocalPrice") * pl.col("Size")).otherwise(0).sum()
        den = pl.when(cond).then(pl.col("Size")).otherwise(0).sum()
        return num / den

    @analytic_handler(
        "VWPP",
        pattern=r"^(?P<trade_type>Lit|Dark)(?P<measure>VWAP|VolumeWeightedPricePlacement)(?P<agg_side>Buy|Sell|Unknown)Aggressor$",
        unit="XLOC",
    )
    def _handle_vwpp(self, cond):
        """{trade_type} trade {measure} for {agg_side} aggressor side per TimeBucket."""
        val = (pl.col("PricePoint").clip(0, 1) * 2 - 1) * pl.col("Size")
        num = pl.when(cond).then(val).otherwise(0).sum()
        den = pl.when(cond).then(pl.col("Size")).otherwise(0).sum()
        return num / den

    def expressions(
        self, ctx: AnalyticContext, config: TradeBreakdownConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        t_type = variant["trade_types"]
        agg_side_str = variant["aggressor_sides"]
        measure = variant["measures"]

        agg_map = {"Buy": 1, "Sell": 2, "Unknown": 0}
        agg_side_val = agg_map.get(agg_side_str)
        cond = (pl.col("BMLLTradeType") == t_type) & (
            pl.col("AggressorSide") == agg_side_val
        )

        handler = self.HANDLERS.get(measure)
        if handler is None:
            return []
        expr = handler(self, cond)

        if config.output_name_pattern:
            alias = config.output_name_pattern.format(**variant)
        else:
            t_type_str = t_type.title()
            measure_str = (
                "VolumeWeightedPricePlacement" if measure == "VWPP" else measure
            )
            alias = f"{t_type_str}{measure_str}{agg_side_str}Aggressor"

        return [expr.alias(alias)]


class ExecutionDerivedAnalytic(AnalyticSpec):
    MODULE = "execution"
    DOCS = [
        AnalyticDoc(
            pattern=r"^TradeImbalance$",
            template="Normalized trade volume imbalance between buy and sell aggressor sides per TimeBucket.",
            unit="Imbalance",
        )
    ]
    ConfigModel = ExecutionDerivedConfig

    def expressions(
        self, ctx: AnalyticContext, config: ExecutionDerivedConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        return []

    def apply(self, df: pl.LazyFrame, config: ExecutionDerivedConfig, variant: Dict[str, Any]) -> pl.LazyFrame:
        v_name = variant["variant"]
        if v_name != "TradeImbalance":
            return df
        cols = df.collect_schema().names()
        if "ExecutedVolumeAsk" in cols and "ExecutedVolumeBid" in cols:
            return df.with_columns(
                (
                    (pl.col("ExecutedVolumeAsk") - pl.col("ExecutedVolumeBid"))
                    / (pl.col("ExecutedVolumeAsk") + pl.col("ExecutedVolumeBid"))
                ).alias("TradeImbalance")
            )
        return df


# =============================
# Analytics Module
# =============================


@register_analytics("execution", config_attr="execution_analytics")
class ExecutionAnalytics(BaseAnalytics):
    """
    Computes execution-related analytics.
    Fully configurable via ExecutionAnalyticsConfig.
    """

    REQUIRES = ["trades", "l3"]

    def __init__(self, config: ExecutionAnalyticsConfig):
        self.config = config
        super().__init__("execution", {})

    def compute(self) -> pl.LazyFrame:
        l3 = self.l3.with_columns(
            pl.col("TimeBucket").cast(pl.Int64).alias("TimeBucketInt")
        )
        trades = self.trades.with_columns(
            pl.col("TimeBucket").cast(pl.Int64).alias("TimeBucketInt")
        )
        ctx = AnalyticContext(base_df=l3, cache={}, context=self.context)

        l3_exprs = []

        l3_configs = self.config.l3_execution
        if (
            not l3_configs
            and not self.config.trade_breakdown
            and not self.config.derived_metrics
        ):
            l3_configs = [
                L3ExecutionConfig(
                    sides=["Bid", "Ask"], measures=["ExecutedVolume", "VWAP"]
                )
            ]

        l3_analytic = L3ExecutionAnalytic()
        for req in l3_configs:
            for variant in req.expand():
                l3_exprs.extend(l3_analytic.expressions(ctx, req, variant))

        if l3_exprs:
            l3_agg = l3.group_by(["ListingId", "TimeBucketInt"]).agg(l3_exprs)
        else:
            l3_agg = None

        trade_exprs = []

        trade_configs = self.config.trade_breakdown
        if (
            not self.config.l3_execution
            and not trade_configs
            and not self.config.derived_metrics
        ):
            trade_configs = [
                TradeBreakdownConfig(
                    trade_types=["LIT", "DARK"],
                    aggressor_sides=["Buy", "Sell", "Unknown"],
                    measures=["Volume", "VWAP", "VWPP"],
                )
            ]

        trade_analytic = TradeBreakdownAnalytic()
        for req in trade_configs:
            for variant in req.expand():
                trade_exprs.extend(trade_analytic.expressions(ctx, req, variant))

        if trade_exprs:
            trades_agg = trades.group_by(["ListingId", "TimeBucketInt"]).agg(
                trade_exprs
            )
        else:
            trades_agg = None

        if l3_agg is not None and trades_agg is not None:
            df = l3_agg.join(trades_agg, on=["ListingId", "TimeBucketInt"], how="full")
        elif l3_agg is not None:
            df = l3_agg
        elif trades_agg is not None:
            df = trades_agg
        else:
            df = l3.select(["ListingId", "TimeBucketInt"]).unique()

        df = df.with_columns(
            pl.col("TimeBucketInt").cast(pl.Datetime("ns")).alias("TimeBucket")
        ).drop("TimeBucketInt")

        derived_configs = self.config.derived_metrics
        if (
            not self.config.l3_execution
            and not self.config.trade_breakdown
            and not derived_configs
        ):
            derived_configs = [ExecutionDerivedConfig(variant="TradeImbalance")]

        derived_analytic = ExecutionDerivedAnalytic()
        for req in derived_configs:
            for variant in req.expand():
                df = derived_analytic.apply(df, req, variant)

        self.df = df
        return df
