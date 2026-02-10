import polars as pl
from intraday_analytics.analytics_base import BaseAnalytics
from pydantic import BaseModel, Field
from typing import List, Union, Literal, Dict, Any, Optional

from .common import CombinatorialMetricConfig, Side
from intraday_analytics.analytics_base import (
    AnalyticSpec,
    AnalyticContext,
    analytic_expression,
    apply_metric_prefix,
)
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
        ...,
        description="Side of the execution (Bid/Ask).",
        json_schema_extra={
            "long_doc": "Selects execution side(s) for L3 execution metrics.\n"
            "Options: Bid, Ask, or list of both.\n"
            "Each side expands into separate output columns.\n"
            "Used in `L3ExecutionAnalytic` to filter executions.\n"
            "Combines with measures for expansion.\n"
            "Example: sides=['Bid','Ask'] yields two columns.\n"
            "If you only need total execution, pick one side.\n"
            "Side selection affects interpretation of flow.\n"
            "Output names include side tokens.",
        },
    )
    measures: Union[L3ExecutionMeasure, List[L3ExecutionMeasure]] = Field(
        ...,
        description="Measure to compute.",
        json_schema_extra={
            "long_doc": "Selects execution measures (ExecutedVolume, VWAP).\n"
            "Each measure expands into separate output columns.\n"
            "Used in `L3ExecutionAnalytic` expressions.\n"
            "ExecutedVolume sums execution size by side.\n"
            "VWAP computes size-weighted average price.\n"
            "Requires ExecutionPrice and ExecutionSize columns.\n"
            "Combine with sides for expansion.\n"
            "VWAP adds price dimension to execution flow.\n"
            "Output names include measure tokens.",
        },
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
        ...,
        description="Trade classification (LIT, DARK).",
        json_schema_extra={
            "long_doc": "Selects trade type(s) for breakdown metrics.\n"
            "Options: LIT, DARK (as defined in trade classifications).\n"
            "Each type expands into separate output columns.\n"
            "Used in `TradeBreakdownAnalytic` filtering.\n"
            "Requires trade type columns in input data.\n"
            "If missing, outputs may be empty.\n"
            "Combine with aggressor sides and measures.\n"
            "Output names include trade type tokens.\n"
            "Trade type selection affects volume attribution.",
        },
    )
    aggressor_sides: Union[AggressorSideType, List[AggressorSideType]] = Field(
        ...,
        description="Aggressor side (Buy, Sell, Unknown).",
        json_schema_extra={
            "long_doc": "Selects aggressor side(s) for trade breakdown.\n"
            "Options: Buy, Sell, Unknown.\n"
            "Each side expands into separate output columns.\n"
            "Used in `TradeBreakdownAnalytic` filtering.\n"
            "Requires aggressor side column in input trades.\n"
            "Unknown includes trades without clear direction.\n"
            "Combine with trade types and measures.\n"
            "Output names include aggressor side tokens.\n"
            "Side selection affects sign interpretation.",
        },
    )
    measures: Union[TradeBreakdownMeasure, List[TradeBreakdownMeasure]] = Field(
        ...,
        description="Measure (Volume, VWAP, VWPP).",
        json_schema_extra={
            "long_doc": "Selects measures for trade breakdown metrics.\n"
            "Options: Volume, VWAP, VWPP.\n"
            "Each measure expands into separate output columns.\n"
            "Used in `TradeBreakdownAnalytic`.\n"
            "VWPP is volume-weighted price placement.\n"
            "Requires price and size columns in input data.\n"
            "Combine with trade types and aggressor sides.\n"
            "Output names include measure tokens.\n"
            "Measures control units (shares vs price).",
        },
    )


DerivedMetricVariant = Literal["TradeImbalance"]


class ExecutionDerivedConfig(CombinatorialMetricConfig):
    """
    Analytics derived from other execution analytics.
    """

    metric_type: Literal["Execution_Derived"] = "Execution_Derived"

    variant: Union[DerivedMetricVariant, List[DerivedMetricVariant]] = Field(
        ...,
        description="Derived analytic name.",
        json_schema_extra={
            "long_doc": "Selects derived execution metric variant(s).\n"
            "Currently supported: TradeImbalance.\n"
            "Each variant expands into separate output columns.\n"
            "Computed in `ExecutionDerivedAnalytic`.\n"
            "Requires base execution/trade breakdown metrics.\n"
            "If prerequisites are missing, outputs may be null.\n"
            "Use to summarize execution flow in a single number.\n"
            "Output names include derived variant tokens.\n"
            "Derived metrics add minimal extra compute.",
        },
    )


class ExecutionAnalyticsConfig(BaseModel):
    """
    Execution analytics configuration.

    Defines metrics derived from L3 executions and trade breakdowns, plus
    optional derived metrics such as trade imbalance. The execution analytics
    pipeline uses execution events and trade data to compute per-trade and
    per-execution summaries (e.g., executed volume, VWAP), then aggregates these
    within TimeBuckets. Derived metrics combine previously computed execution
    components into higher-level summaries.
    """

    ENABLED: bool = True
    metric_prefix: Optional[str] = Field(
        None,
        description="Prefix for execution metric columns.",
        json_schema_extra={
            "long_doc": "Prepended to all execution output columns.\n"
            "Useful to namespace execution outputs vs trades/L3.\n"
            "Example: 'EX_' yields EX_ExecutedVolumeBid.\n"
            "Applies to all execution metrics in this pass.\n"
            "Implementation uses `BaseAnalytics.metric_prefix`.\n"
            "See `intraday_analytics/analytics_base.py` for naming.\n"
            "Changing the prefix changes column names and joins.\n"
            "Keep stable for production outputs.\n"
            "Leave empty to use default module naming.\n"
        },
    )
    l3_execution: List[L3ExecutionConfig] = Field(
        default_factory=list,
        description="Execution metrics derived from L3 executions.",
        json_schema_extra={
            "long_doc": "Metrics computed from L3 execution events (ExecutedVolume, VWAP).\n"
            "Each entry expands by side and measure.\n"
            "Example: sides=['Bid','Ask'], measures=['ExecutedVolume'].\n"
            "Computed in `L3ExecutionAnalytic`.\n"
            "Requires L3 execution columns (ExecutionPrice/ExecutionSize).\n"
            "Aggregation is per TimeBucket.\n"
            "Useful for execution-side flow metrics.\n"
            "Large lists create many output columns.\n"
            "Output names include side tokens.",
        },
    )
    trade_breakdown: List[TradeBreakdownConfig] = Field(
        default_factory=list,
        description="Trade breakdown metrics.",
        json_schema_extra={
            "long_doc": "Metrics based on trade type and aggressor side.\n"
            "Each entry expands by trade type, aggressor side, and measure.\n"
            "Example: trade_types=['LIT'], aggressor_sides=['Buy'], measures=['VWAP'].\n"
            "Computed in `TradeBreakdownAnalytic`.\n"
            "Requires trade-level columns (Classification, AggressorSide).\n"
            "Useful for lit vs dark or buy vs sell decompositions.\n"
            "Aggregation is per TimeBucket.\n"
            "Large configs can explode output width.\n"
            "Output names include trade type and side tokens.",
        },
    )
    derived_metrics: List[ExecutionDerivedConfig] = Field(
        default_factory=list,
        description="Derived execution metrics.",
        json_schema_extra={
            "long_doc": "Metrics derived from other execution analytics (e.g., trade imbalance).\n"
            "These rely on previously computed execution components.\n"
            "Computed in `ExecutionDerivedAnalytic`.\n"
            "Variants expand by the specified derived metric names.\n"
            "Useful for high-level summaries of execution flow.\n"
            "Requires prerequisite metrics to be enabled.\n"
            "If prerequisites are missing, outputs may be null.\n"
            "Keep configs small to limit output width.\n"
            "Output names include derived metric tokens.",
        },
    )


# =============================
# Analytic Specs
# =============================


class L3ExecutionAnalytic(AnalyticSpec):
    MODULE = "execution"
    ConfigModel = L3ExecutionConfig

    @analytic_expression(
        "ExecutedVolume",
        pattern=r"^ExecutedVolume(?P<side>Bid|Ask)$",
        unit="Shares",
    )
    def _expression_executed_volume(self, cond):
        """
        Sum of executed volume from L3 executions on {side} side per TimeBucket.

        Why:
            To measure the volume executed against resting orders on a specific side.

        Interest:
            ExecutedVolumeBid represents selling pressure (aggressor hitting the bid), while ExecutedVolumeAsk represents buying pressure (aggressor lifting the ask).

        Usage:
            Used to calculate order flow imbalance and directional volume.
        """
        return pl.when(cond).then(pl.col("ExecutionSize")).otherwise(0).sum()

    @analytic_expression(
        "VWAP",
        pattern=r"^Vwap(?P<side>Bid|Ask)$",
        unit="XLOC",
    )
    def _expression_vwap(self, cond):
        """
        VWAP of executions on {side} side per TimeBucket.

        Why:
            To calculate the average price of executions on a specific side.

        Interest:
            Comparing Bid VWAP and Ask VWAP gives insight into the effective spread and price levels where trading is occurring.

        Usage:
            Used for execution quality analysis and spread estimation.
        """
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

        expression_fn = self.EXPRESSIONS.get(measure)
        if expression_fn is None:
            return []
        expr = expression_fn(self, cond)

        if config.output_name_pattern:
            alias = config.output_name_pattern.format(**variant)
        else:
            if measure == "ExecutedVolume":
                alias = f"ExecutedVolume{side}"
            elif measure == "VWAP":
                alias = f"Vwap{side}"
            else:
                alias = f"{measure}{side}"

        return [expr.alias(apply_metric_prefix(ctx, alias))]


class TradeBreakdownAnalytic(AnalyticSpec):
    MODULE = "execution"
    ConfigModel = TradeBreakdownConfig

    @analytic_expression(
        "Volume",
        pattern=r"^(?P<trade_type>Lit|Dark)(?P<measure>Volume)(?P<agg_side>Buy|Sell|Unknown)Aggressor$",
        unit="Shares",
    )
    def _expression_volume(self, cond):
        """
        {trade_type} trade {measure} for {agg_side} aggressor side per TimeBucket.

        Why:
            To decompose volume by venue type (Lit/Dark) and aggressor direction.

        Interest:
            Lit volume contributes to price discovery, while Dark volume provides liquidity without pre-trade transparency. Aggressor side reveals the direction of flow.

        Usage:
            Used to analyze market structure, dark pool participation, and directional flow.
        """
        return pl.when(cond).then(pl.col("Size")).otherwise(0).sum()

    @analytic_expression(
        "VWAP",
        pattern=r"^(?P<trade_type>Lit|Dark)(?P<measure>VWAP|VolumeWeightedPricePlacement)(?P<agg_side>Buy|Sell|Unknown)Aggressor$",
        unit="XLOC",
    )
    def _expression_vwap(self, cond):
        """
        {trade_type} trade {measure} for {agg_side} aggressor side per TimeBucket.

        Why:
            To calculate the average execution price for specific trade segments.

        Interest:
            Comparing Lit vs. Dark VWAP can reveal price improvement opportunities in dark pools.

        Usage:
            Used for venue performance analysis and execution routing decisions.
        """
        num = (
            pl.when(cond).then(pl.col("LocalPrice") * pl.col("Size")).otherwise(0).sum()
        )
        den = pl.when(cond).then(pl.col("Size")).otherwise(0).sum()
        return num / den

    @analytic_expression(
        "VWPP",
        pattern=r"^(?P<trade_type>Lit|Dark)(?P<measure>VWAP|VolumeWeightedPricePlacement)(?P<agg_side>Buy|Sell|Unknown)Aggressor$",
        unit="XLOC",
    )
    def _expression_vwpp(self, cond):
        """
        {trade_type} trade {measure} for {agg_side} aggressor side per TimeBucket.

        Why:
            To measure where trades occur relative to the spread (Volume Weighted Price Placement).

        Interest:
            VWPP close to 0 means trading at the bid, 1 at the ask. It indicates the aggressiveness of the execution.

        Usage:
            Used to assess execution quality and spread capture.
        """
        val = (pl.col("PricePoint").clip(0, 1) * 2 - 1) * pl.col("Size")
        num = pl.when(cond).then(val).otherwise(0).sum()
        den = pl.when(cond).then(pl.col("Size")).otherwise(0).sum()
        return num / den

    def expressions(
        self,
        ctx: AnalyticContext,
        config: TradeBreakdownConfig,
        variant: Dict[str, Any],
    ) -> List[pl.Expr]:
        t_type = variant["trade_types"]
        agg_side_str = variant["aggressor_sides"]
        measure = variant["measures"]

        agg_map = {"Buy": 1, "Sell": 2, "Unknown": 0}
        agg_side_val = agg_map.get(agg_side_str)
        cond = (pl.col("BMLLTradeType") == t_type) & (
            pl.col("AggressorSide") == agg_side_val
        )

        expression_fn = self.EXPRESSIONS.get(measure)
        if expression_fn is None:
            return []
        expr = expression_fn(self, cond)

        if config.output_name_pattern:
            alias = config.output_name_pattern.format(**variant)
        else:
            t_type_str = t_type.title()
            measure_str = (
                "VolumeWeightedPricePlacement" if measure == "VWPP" else measure
            )
            alias = f"{t_type_str}{measure_str}{agg_side_str}Aggressor"

        return [expr.alias(apply_metric_prefix(ctx, alias))]


class ExecutionDerivedAnalytic(AnalyticSpec):
    MODULE = "execution"
    ConfigModel = ExecutionDerivedConfig

    @analytic_expression(
        "TradeImbalance",
        pattern=r"^TradeImbalance$",
        unit="Imbalance",
    )
    def _expression_trade_imbalance(
        self, df: pl.LazyFrame, prefix: str
    ) -> pl.LazyFrame:
        """
        Normalized trade volume imbalance between buy and sell aggressor sides per TimeBucket.

        Why:
            To measure the net direction of trading flow.

        Interest:
            A positive imbalance indicates more buying pressure (aggressors lifting the ask), while negative indicates selling pressure (aggressors hitting the bid).

        Usage:
            Used as a momentum signal and to predict short-term price movements.
        """
        cols = df.collect_schema().names()
        ask_col = f"{prefix}ExecutedVolumeAsk" if prefix else "ExecutedVolumeAsk"
        bid_col = f"{prefix}ExecutedVolumeBid" if prefix else "ExecutedVolumeBid"
        out_col = f"{prefix}TradeImbalance" if prefix else "TradeImbalance"
        if ask_col in cols and bid_col in cols:
            return df.with_columns(
                (
                    (pl.col(ask_col) - pl.col(bid_col))
                    / (pl.col(ask_col) + pl.col(bid_col))
                ).alias(out_col)
            )
        return df

    def expressions(
        self,
        ctx: AnalyticContext,
        config: ExecutionDerivedConfig,
        variant: Dict[str, Any],
    ) -> List[pl.Expr]:
        return []

    def apply(
        self,
        df: pl.LazyFrame,
        config: ExecutionDerivedConfig,
        variant: Dict[str, Any],
        prefix: str,
    ) -> pl.LazyFrame:
        v_name = variant["variant"]
        expression_fn = self.EXPRESSIONS.get(v_name)
        if expression_fn is None:
            return df
        return expression_fn(self, df, prefix)


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
        super().__init__("execution", {}, metric_prefix=config.metric_prefix)

    def compute(self) -> pl.LazyFrame:
        l3 = self.l3.with_columns(
            pl.col("TimeBucket").cast(pl.Int64).alias("TimeBucketInt")
        )
        trades = self.trades.with_columns(
            pl.col("TimeBucket").cast(pl.Int64).alias("TimeBucketInt")
        )
        ctx = AnalyticContext(
            base_df=l3,
            cache={"metric_prefix": self.metric_prefix},
            context=self.context,
        )

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

        df = df.filter(
            pl.col("ListingId").is_not_null() & pl.col("TimeBucketInt").is_not_null()
        )

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
                df = derived_analytic.apply(df, req, variant, self.metric_prefix)

        self.df = df
        return df
