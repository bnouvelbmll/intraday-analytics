import logging
import polars as pl
from intraday_analytics.analytics_base import BaseAnalytics, BaseTWAnalytics
from pydantic import BaseModel, Field
from typing import Optional, List, Union, Literal, Dict, Any

from .common import CombinatorialMetricConfig, Side, AggregationMethod
from intraday_analytics.analytics_base import (
    AnalyticSpec,
    AnalyticContext,
    AnalyticDoc,
    analytic_expression,
    build_expressions,
    apply_metric_prefix,
)
from intraday_analytics.analytics_registry import register_analytics


# =============================
# Configuration Models
# =============================

L2LiquidityMeasure = Literal[
    "Quantity",
    "CumQuantity",
    "CumNotional",
    "Price",
    "InsertAge",
    "LastMod",
    "SizeAhead",
    "NumOrders",
    "CumOrders",
]


class L2LiquidityConfig(CombinatorialMetricConfig):
    """
    Configuration for L2 Liquidity analytics (at specific levels).
    """

    metric_type: Literal["L2_Liquidity"] = "L2_Liquidity"

    sides: Union[Side, List[Side]] = Field(
        ...,
        description="Side of the book to analyze (Bid, Ask).",
        json_schema_extra={
            "long_doc": "Selects book side(s) for liquidity metrics.\n"
            "Options: Bid, Ask, or a list of both.\n"
            "Each side expands into separate metric columns.\n"
            "Used in `L2LiquidityAnalytic.expressions()`.\n"
            "Combines with levels and measures for combinatorial expansion.\n"
            "Example: sides=['Bid','Ask'] doubles the output columns.\n"
            "If you only need top-of-book, choose a single side.\n"
            "Side selection affects interpretation of imbalance.\n"
            "Output names include side tokens.",
        },
    )

    levels: Union[int, List[int]] = Field(
        ...,
        description="Book levels to query (1-based index). Can be a single int or a list of ints.",
        json_schema_extra={
            "long_doc": "Selects depth levels to include (1-based, 1 = best level).\n"
            "Accepts a single int or a list of levels.\n"
            "Each level expands into separate metric columns.\n"
            "Used in `L2LiquidityAnalytic` for column selection.\n"
            "Higher levels require more columns in the input schema.\n"
            "Large lists increase output width and compute time.\n"
            "Example: levels=[1,5,10] yields three depth levels.\n"
            "Ensure your L2 data includes at least max(levels).\n"
            "Output names include level numbers.",
        },
    )

    measures: Union[L2LiquidityMeasure, List[L2LiquidityMeasure]] = Field(
        ...,
        description="The specific measure to extract (e.g., 'Quantity', 'Price').",
        json_schema_extra={
            "long_doc": "Selects which liquidity measures to compute.\n"
            "Examples: Quantity, Price, NumOrders, CumQuantity.\n"
            "Each measure expands into separate columns per side/level.\n"
            "Used in `L2LiquidityAnalytic` to build expressions.\n"
            "Cum* measures aggregate across levels 1..N.\n"
            "Some measures require additional columns (e.g., NumOrders).\n"
            "Large measure lists increase output width.\n"
            "Measures control units (shares, price, orders).\n"
            "Output names include the measure token.",
        },
    )


class L2SpreadConfig(CombinatorialMetricConfig):
    """
    Configuration for Bid-Ask Spread analytics.
    """

    metric_type: Literal["L2_Spread"] = "L2_Spread"

    variant: Union[Literal["Abs", "BPS"], List[Literal["Abs", "BPS"]]] = Field(
        default=["BPS"],
        description="Spread variant: 'Abs' (Ask-Bid) or 'BPS' (Basis Points relative to Mid).",
        json_schema_extra={
            "long_doc": "Selects spread variant(s).\n"
            "Abs computes Ask - Bid in price units.\n"
            "BPS computes spread in basis points relative to Mid.\n"
            "Each variant expands into separate output columns.\n"
            "Used in `L2SpreadAnalytic`.\n"
            "BPS requires Mid price to be available.\n"
            "Use Abs for raw spreads, BPS for normalized metrics.\n"
            "You can enable both for comparative analysis.\n"
            "Output names include Abs/BPS token.",
        },
    )


L2ImbalanceMeasure = Literal["Quantity", "CumQuantity", "CumNotional", "Orders"]


class L2ImbalanceConfig(CombinatorialMetricConfig):
    """
    Configuration for Order Book Imbalance.
    Formula: (Bid - Ask) / (Bid + Ask)
    """

    metric_type: Literal["L2_Imbalance"] = "L2_Imbalance"

    levels: Union[int, List[int]] = Field(
        ...,
        description="Book levels to consider. If >1, usually implies cumulative imbalance up to that level.",
        json_schema_extra={
            "long_doc": "Selects depth levels for imbalance computation.\n"
            "If >1, imbalance may be computed on cumulative depth.\n"
            "Accepts a single level or a list of levels.\n"
            "Each level expands into separate output columns.\n"
            "Used in `L2ImbalanceAnalytic`.\n"
            "Higher levels smooth imbalance but require deeper book data.\n"
            "Example: levels=[1,5] yields two imbalance metrics.\n"
            "Ensure input has sufficient depth columns.\n"
            "Output names include level numbers.",
        },
    )

    measure: Union[L2ImbalanceMeasure, List[L2ImbalanceMeasure]] = Field(
        default="CumQuantity",
        description="The underlying measure to use for the imbalance calculation.",
        json_schema_extra={
            "long_doc": "Selects the underlying measure for imbalance.\n"
            "Options include CumQuantity, CumOrders, CumNotional.\n"
            "Each measure expands into separate columns.\n"
            "Used in `L2ImbalanceAnalytic` expressions.\n"
            "CumQuantity uses quantities, CumOrders uses order counts.\n"
            "CumNotional uses price * quantity.\n"
            "Choose based on your liquidity focus.\n"
            "Requires corresponding L2 columns.\n"
            "Output names include the measure token.",
        },
    )


class L2VolatilityConfig(CombinatorialMetricConfig):
    """
    Configuration for Price Volatility (Standard Deviation of log returns price series).
    """

    metric_type: Literal["L2_Volatility"] = "L2_Volatility"

    source: Union[Literal["Mid", "Bid", "Ask", "Last", "WeightedMid"], List[str]] = (
        Field(
            default="Mid",
            description="The price series to measure volatility on.",
            json_schema_extra={
                "long_doc": "Selects the price series for volatility calculations.\n"
                "Options: Mid, Bid, Ask, Last, WeightedMid.\n"
                "Each source expands into separate output columns.\n"
                "Used in `L2VolatilityAnalytic`.\n"
                "WeightedMid uses bid/ask quantities for weighting.\n"
                "Ensure the chosen price series exists in the input.\n"
                "Mid is usually the most stable choice.\n"
                "Changing source affects interpretation of volatility.\n"
                "Output names include source token.",
            },
        )
    )

    aggregations: List[AggregationMethod] = ["Std"]


class L2OHLCConfig(CombinatorialMetricConfig):
    """
    Configuration for OHLC bars computed from L2 price series.
    """

    metric_type: Literal["L2_OHLC"] = "L2_OHLC"

    source: Union[Literal["Mid", "Bid", "Ask", "WeightedMid"], List[str]] = Field(
        default="Mid",
        description="The price series to compute OHLC on.",
        json_schema_extra={
            "long_doc": "Selects the price series for OHLC.\n"
            "Options: Mid, Bid, Ask, WeightedMid.\n"
            "Each source expands into separate OHLC columns.\n"
            "Used in `L2OHLCAnalytic`.\n"
            "Ensure chosen price series exists in input.\n"
            "Mid is the common default for market-level OHLC.\n"
            "WeightedMid is sensitive to order book depth.\n"
            "Output names include source token.\n"
            "Changing source changes interpretation of OHLC.",
        },
    )

    open_mode: Literal["event", "prev_close"] = Field(
        default="event",
        description="How to compute Open for empty buckets: 'event' uses first event; "
        "'prev_close' uses previous Close and fills empty buckets.",
        json_schema_extra={
            "long_doc": "Controls how Open is defined when a bucket has no events.\n"
            "event: use the first event in the bucket as Open.\n"
            "prev_close: use previous Close to fill empty buckets.\n"
            "Used in `L2OHLCAnalytic` when constructing OHLC series.\n"
            "prev_close is useful for chart continuity.\n"
            "event is more faithful to actual activity.\n"
            "Choose based on downstream visualization needs.\n"
            "Empty buckets are common in illiquid symbols.\n"
            "Changing this affects Open and derived metrics.",
        },
    )


class L2AnalyticsConfig(BaseModel):
    """
    L2 analytics configuration.

    Controls order book metrics such as liquidity, spreads, imbalances,
    volatility, and OHLC. Each list expands into multiple metric columns.
    The L2 analytics pipeline selects the relevant book columns, computes
    per-event expressions (e.g., spreads, depth), and aggregates them into
    TimeBuckets using the specified aggregation methods. Configuration should
    reflect the depth available in the input order book and the desired
    granularity of output columns.
    """

    ENABLED: bool = True
    metric_prefix: Optional[str] = Field(
        None,
        description="Prefix for L2 metric columns.",
        json_schema_extra={
            "long_doc": "Prepended to all L2 output column names.\n"
            "Useful to namespace outputs when combining multiple passes.\n"
            "Example: metric_prefix='L2_' yields L2_SpreadAbsTWA.\n"
            "Applies to all L2 series generated by this pass.\n"
            "Implementation uses `BaseAnalytics.metric_prefix`.\n"
            "See `intraday_analytics/analytics_base.py` for naming rules.\n"
            "Changing the prefix alters column names and downstream joins.\n"
            "Keep stable prefixes in production pipelines.\n"
            "Leave empty to keep default module naming.\n"
        },
    )
    time_bucket_seconds: Optional[float] = None
    time_bucket_anchor: Literal["end", "start"] = "end"
    time_bucket_closed: Literal["right", "left"] = "right"

    liquidity: List[L2LiquidityConfig] = Field(
        default_factory=list,
        description="Liquidity metrics configuration.",
        json_schema_extra={
            "long_doc": "List of L2 liquidity metrics (depth, quantities, orders) to compute.\n"
            "Each entry expands by side, level, and measure.\n"
            "Example: sides=['Bid','Ask'], levels=[1,5], measures=['Quantity'].\n"
            "Generates BidQuantity1, AskQuantity1, BidQuantity5, AskQuantity5.\n"
            "Aggregation is controlled by the base `aggregations` field.\n"
            "Used in `L2LiquidityAnalytic` in `intraday_analytics/analytics/l2.py`.\n"
            "High levels and many measures can create many columns.\n"
            "Reduce levels or measures if output is too wide.\n"
            "Best for depth and queue metrics.\n"
            "Requires L2 book columns (BidPrice, BidQuantity, etc.).",
        },
    )
    spreads: List[L2SpreadConfig] = Field(
        default_factory=list,
        description="Spread metrics configuration.",
        json_schema_extra={
            "long_doc": "List of L2 spread metrics (absolute, bps) to compute.\n"
            "Variant controls Abs or BPS, and sides are implicit (best bid/ask).\n"
            "Example: variant=['Abs','BPS'] yields SpreadAbs and SpreadBPS.\n"
            "Aggregations apply over TimeBucket for each variant.\n"
            "Computed in `L2SpreadAnalytic` in `intraday_analytics/analytics/l2.py`.\n"
            "Requires best bid/ask columns in the L2 frame.\n"
            "BPS uses mid price to normalize spread.\n"
            "Consider fewer aggregations if you only need last or mean.\n"
            "Spread metrics are common for liquidity comparisons.\n"
        },
    )
    imbalances: List[L2ImbalanceConfig] = Field(
        default_factory=list,
        description="Imbalance metrics configuration.",
        json_schema_extra={
            "long_doc": "List of L2 imbalance metrics to compute (cum quantities, orders, notional).\n"
            "Each entry expands by measure and level.\n"
            "Example: levels=[1,5], measure='CumQuantity' yields ImbalanceCumQuantity1/5.\n"
            "Imbalance is typically (bid - ask) / (bid + ask).\n"
            "Computed in `L2ImbalanceAnalytic`.\n"
            "Requires L2 depth columns for both sides.\n"
            "Higher levels smooth imbalance but increase compute.\n"
            "Aggregations apply per TimeBucket.\n"
            "Use with caution for illiquid symbols with sparse books.\n"
            "Output names include level and measure to avoid ambiguity.",
        },
    )
    volatility: List[L2VolatilityConfig] = Field(
        default_factory=list,
        description="Volatility metrics configuration.",
        json_schema_extra={
            "long_doc": "List of L2 volatility metrics to compute.\n"
            "Selects price series (Mid, Bid, Ask, WeightedMid) and aggregation.\n"
            "Example: source='Mid' computes volatility over mid prices.\n"
            "Computed in `L2VolatilityAnalytic`.\n"
            "Aggregation defines whether you compute std/mean/etc across TimeBucket.\n"
            "Requires price series columns present in the L2 frame.\n"
            "Volatility is sensitive to bucket size (time_bucket_seconds).\n"
            "Consider longer buckets for stable estimates.\n"
            "Output names encode source and aggregation.",
        },
    )
    ohlc: List[L2OHLCConfig] = Field(
        default_factory=list,
        description="OHLC metrics configuration.",
        json_schema_extra={
            "long_doc": "List of L2 OHLC metrics to compute.\n"
            "Selects price series and open mode (event vs prev_close).\n"
            "Example: source='Mid' produces MidOpen/MidHigh/MidLow/MidClose.\n"
            "Open mode controls how empty buckets are filled.\n"
            "Computed in `L2OHLCAnalytic`.\n"
            "Requires price series columns present in the L2 frame.\n"
            "OHLC output is useful for charting and downstream indicators.\n"
            "Large numbers of OHLC configs can increase output width.\n"
            "Output names encode source and aggregation conventions.",
        },
    )

    levels: int = 10


# =============================
# Shared Helpers
# =============================


def _price_series(source: str) -> pl.Expr | None:
    if source == "Mid":
        return (pl.col("AskPrice1") + pl.col("BidPrice1")) / 2
    if source == "WeightedMid":
        return (
            pl.col("BidPrice1") * pl.col("AskQuantity1")
            + pl.col("AskPrice1") * pl.col("BidQuantity1")
        ) / (pl.col("AskQuantity1") + pl.col("BidQuantity1"))
    if source == "Bid":
        return pl.col("BidPrice1")
    if source == "Ask":
        return pl.col("AskPrice1")
    return None


def _liquidity_raw(side: str, level: int, measure: str) -> pl.Expr | None:
    if measure in [
        "Quantity",
        "Price",
        "NumOrders",
        "InsertAge",
        "LastMod",
        "SizeAhead",
    ]:
        return pl.col(f"{side}{measure}{level}")
    if measure == "CumQuantity":
        cols = [f"{side}Quantity{i}" for i in range(1, level + 1)]
        return pl.sum_horizontal(cols)
    if measure == "CumOrders":
        cols = [f"{side}NumOrders{i}" for i in range(1, level + 1)]
        return pl.sum_horizontal(cols)
    if measure == "CumNotional":
        notional_exprs = [
            pl.col(f"{side}Price{i}") * pl.col(f"{side}Quantity{i}")
            for i in range(1, level + 1)
        ]
        return pl.sum_horizontal(notional_exprs)
    return None


def _liquidity_raw_pattern(measure: str, suffix: str = "") -> str:
    suffix_part = suffix if suffix else ""
    return rf"^(?P<side>Bid|Ask)(?P<measure>{measure})(?P<level>\d+){suffix_part}$"


def _imbalance_raw_pattern(measure: str, suffix: str = "") -> str:
    suffix_part = suffix if suffix else ""
    return rf"^Imbalance(?P<measure>{measure})(?P<level>\d+){suffix_part}$"


def _imbalance_raw(level: int, measure: str) -> pl.Expr:
    if measure == "CumQuantity":
        bid_val = pl.sum_horizontal([f"BidQuantity{i}" for i in range(1, level + 1)])
        ask_val = pl.sum_horizontal([f"AskQuantity{i}" for i in range(1, level + 1)])
    elif measure == "Orders":
        bid_val = pl.sum_horizontal([f"BidNumOrders{i}" for i in range(1, level + 1)])
        ask_val = pl.sum_horizontal([f"AskNumOrders{i}" for i in range(1, level + 1)])
    elif measure == "CumNotional":
        bid_val = pl.sum_horizontal(
            [
                pl.col(f"BidPrice{i}") * pl.col(f"BidQuantity{i}")
                for i in range(1, level + 1)
            ]
        )
        ask_val = pl.sum_horizontal(
            [
                pl.col(f"AskPrice{i}") * pl.col(f"AskQuantity{i}")
                for i in range(1, level + 1)
            ]
        )
    else:
        bid_val = pl.col(f"BidQuantity{level}")
        ask_val = pl.col(f"AskQuantity{level}")
    return (bid_val - ask_val) / (bid_val + ask_val)


# =============================
# Last-Snapshot Analytics
# =============================


class L2LastLiquidityAnalytic(AnalyticSpec):
    MODULE = "l2_last"
    ConfigModel = L2LiquidityConfig

    @analytic_expression(
        "Price",
        pattern=_liquidity_raw_pattern("Price"),
        unit="XLOC",
    )
    def _expression_price(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "Price")

    @analytic_expression(
        "CumNotional",
        pattern=_liquidity_raw_pattern("CumNotional"),
        unit="XLOC",
    )
    def _expression_cumnotional(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "CumNotional")

    @analytic_expression(
        "Quantity",
        pattern=_liquidity_raw_pattern("Quantity"),
        unit="Shares",
    )
    def _expression_quantity(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "Quantity")

    @analytic_expression(
        "CumQuantity",
        pattern=_liquidity_raw_pattern("CumQuantity"),
        unit="Shares",
    )
    def _expression_cumquantity(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "CumQuantity")

    @analytic_expression(
        "SizeAhead",
        pattern=_liquidity_raw_pattern("SizeAhead"),
        unit="Shares",
    )
    def _expression_sizeahead(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "SizeAhead")

    @analytic_expression(
        "NumOrders",
        pattern=_liquidity_raw_pattern("NumOrders"),
        unit="Orders",
    )
    def _expression_numorders(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "NumOrders")

    @analytic_expression(
        "CumOrders",
        pattern=_liquidity_raw_pattern("CumOrders"),
        unit="Orders",
    )
    def _expression_cumorders(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "CumOrders")

    @analytic_expression(
        "InsertAge",
        pattern=_liquidity_raw_pattern("InsertAge"),
        unit="Nanoseconds",
    )
    def _expression_insertage(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "InsertAge")

    @analytic_expression(
        "LastMod",
        pattern=_liquidity_raw_pattern("LastMod"),
        unit="Nanoseconds",
    )
    def _expression_lastmod(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "LastMod")

    def expressions(
        self, ctx: AnalyticContext, config: L2LiquidityConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        side = variant["sides"]
        level = variant["levels"]
        measure = variant["measures"]

        expression_fn = self.EXPRESSIONS.get(measure)
        if expression_fn is None:
            return []
        base_expr = expression_fn(self, side, level)
        if base_expr is None:
            return []

        if config.market_states:
            base_expr = base_expr.filter(
                pl.col("MarketState").is_in(config.market_states)
            )

        expr = base_expr.last()
        alias = (
            config.output_name_pattern.format(**variant)
            if config.output_name_pattern
            else f"{side}{measure}{level}"
        )
        return [expr.alias(apply_metric_prefix(ctx, alias))]


class L2LastSpreadAnalytic(AnalyticSpec):
    MODULE = "l2_last"
    ConfigModel = L2SpreadConfig
    DOCS = [
        AnalyticDoc(
            pattern=r"^SpreadBps$",
            template="Best ask minus best bid in basis points using the last snapshot in the TimeBucket.",
            unit="BPS",
        )
    ]

    @analytic_expression(
        "Abs",
        pattern=r"^SpreadAbs$",
        unit="XLOC",
    )
    def _expression_abs(self):
        """Best ask minus best bid in absolute price terms using the last snapshot in the TimeBucket."""
        return pl.col("AskPrice1") - pl.col("BidPrice1")

    @analytic_expression(
        "BPS",
        pattern=r"^SpreadBps$",
        unit="BPS",
    )
    def _expression_bps(self):
        """Best ask minus best bid in basis points using the last snapshot in the TimeBucket."""
        return (
            20000
            * (pl.col("AskPrice1") - pl.col("BidPrice1"))
            / (pl.col("AskPrice1") + pl.col("BidPrice1"))
        )

    def expressions(
        self, ctx: AnalyticContext, config: L2SpreadConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        v_type = variant["variant"]
        expression_fn = self.EXPRESSIONS.get(v_type)
        if expression_fn is None:
            return []
        base_expr = expression_fn(self)

        if config.market_states:
            base_expr = base_expr.filter(
                pl.col("MarketState").is_in(config.market_states)
            )

        expr = base_expr.last()
        if config.output_name_pattern:
            alias = config.output_name_pattern.format(**variant)
        elif v_type == "BPS":
            alias = "SpreadBps"
        else:
            alias = f"Spread{v_type}"
        return [expr.alias(apply_metric_prefix(ctx, alias))]


class L2LastImbalanceAnalytic(AnalyticSpec):
    MODULE = "l2_last"
    ConfigModel = L2ImbalanceConfig
    DOCS = [
        AnalyticDoc(
            pattern=r"^(VolumeImbalance|OrdersImbalance)(?P<level>\d+)$",
            template="Order book imbalance for volume or orders up to level {level} using the last snapshot, expressed as a normalized difference between bid and ask.",
            unit="Imbalance",
        )
    ]

    @analytic_expression(
        "CumQuantity",
        pattern=_imbalance_raw_pattern("CumQuantity"),
        unit="Imbalance",
    )
    def _expression_cumquantity(self, level: int):
        """Order book imbalance for {measure} up to level {level} using the last snapshot, expressed as a normalized difference between bid and ask."""
        return _imbalance_raw(level, "CumQuantity")

    @analytic_expression(
        "Orders",
        pattern=_imbalance_raw_pattern("Orders"),
        unit="Imbalance",
    )
    def _expression_orders(self, level: int):
        """Order book imbalance for {measure} up to level {level} using the last snapshot, expressed as a normalized difference between bid and ask."""
        return _imbalance_raw(level, "Orders")

    @analytic_expression(
        "CumNotional",
        pattern=_imbalance_raw_pattern("CumNotional"),
        unit="Imbalance",
    )
    def _expression_cumnotional(self, level: int):
        """Order book imbalance for {measure} up to level {level} using the last snapshot, expressed as a normalized difference between bid and ask."""
        return _imbalance_raw(level, "CumNotional")

    def expressions(
        self, ctx: AnalyticContext, config: L2ImbalanceConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        level = variant["levels"]
        measure = variant["measure"]

        expression_fn = self.EXPRESSIONS.get(measure)
        if expression_fn is None:
            return []
        base_expr = expression_fn(self, level)

        if config.market_states:
            base_expr = base_expr.filter(
                pl.col("MarketState").is_in(config.market_states)
            )

        expr = base_expr.last()
        alias = (
            config.output_name_pattern.format(**variant)
            if config.output_name_pattern
            else f"Imbalance{measure}{level}"
        )
        return [expr.alias(apply_metric_prefix(ctx, alias))]


class L2LastOHLCAnalytic(AnalyticSpec):
    MODULE = "l2_last"
    ConfigModel = L2OHLCConfig

    @analytic_expression(
        "OHLC",
        pattern=r"^(?P<source>Bid|Ask|Mid|WeightedMid)(?P<ohlc>Open|High|Low|Close)(?P<openMode>C)?$",
        unit="XLOC",
        group="L2OHLC{source}",
        group_role="{ohlc}",
        group_semantics="ohlc_bar,ffill_non_naive",
    )
    def _expression_ohlc(self):
        """L2 {ohlc} for {source} price within the TimeBucket{openModeSuffix}."""
        return None

    def expressions(
        self, ctx: AnalyticContext, config: L2OHLCConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        source = variant["source"]
        p = _price_series(source)
        if p is None:
            return []

        expressions = []
        open_mode = variant.get("open_mode", "")
        open_mode_key = str(open_mode) if open_mode else ""
        open_mode_name = {
            "event": "",
            "prev_close": "C",
        }.get(
            open_mode_key,
            open_mode_key.replace("_", " ").title().replace(" ", ""),
        )
        for ohlc in ["Open", "High", "Low", "Close"]:
            variant_with_ohlc = {
                **variant,
                "ohlc": ohlc,
                "openMode": open_mode_name,
            }
            default_name = f"{source}{ohlc}"
            if ohlc == "Open":
                expr = p.first()
            elif ohlc == "High":
                expr = p.max()
            elif ohlc == "Low":
                expr = p.min()
            else:
                expr = p.last()

            alias = (
                config.output_name_pattern.format(**variant_with_ohlc)
                if config.output_name_pattern
                else default_name
            )
            expressions.append(expr.alias(apply_metric_prefix(ctx, alias)))
        return expressions


# =============================
# Time-Weighted Analytics
# =============================


def _twa(expr: pl.Expr, market_states: Optional[List[str]] = None) -> pl.Expr:
    if market_states:
        expr = expr.filter(pl.col("MarketState").is_in(market_states))
    return (expr * pl.col("DT")).sum() / pl.col("DT").sum()


class L2TWLiquidityAnalytic(AnalyticSpec):
    MODULE = "l2_tw"
    ConfigModel = L2LiquidityConfig

    @analytic_expression(
        "Price",
        pattern=_liquidity_raw_pattern("Price", "TWA"),
        unit="XLOC",
    )
    def _expression_price(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "Price")

    @analytic_expression(
        "CumNotional",
        pattern=_liquidity_raw_pattern("CumNotional", "TWA"),
        unit="XLOC",
    )
    def _expression_cumnotional(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "CumNotional")

    @analytic_expression(
        "Quantity",
        pattern=_liquidity_raw_pattern("Quantity", "TWA"),
        unit="Shares",
    )
    def _expression_quantity(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "Quantity")

    @analytic_expression(
        "CumQuantity",
        pattern=_liquidity_raw_pattern("CumQuantity", "TWA"),
        unit="Shares",
    )
    def _expression_cumquantity(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "CumQuantity")

    @analytic_expression(
        "SizeAhead",
        pattern=_liquidity_raw_pattern("SizeAhead", "TWA"),
        unit="Shares",
    )
    def _expression_sizeahead(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "SizeAhead")

    @analytic_expression(
        "NumOrders",
        pattern=_liquidity_raw_pattern("NumOrders", "TWA"),
        unit="Orders",
    )
    def _expression_numorders(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "NumOrders")

    @analytic_expression(
        "CumOrders",
        pattern=_liquidity_raw_pattern("CumOrders", "TWA"),
        unit="Orders",
    )
    def _expression_cumorders(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "CumOrders")

    @analytic_expression(
        "InsertAge",
        pattern=_liquidity_raw_pattern("InsertAge", "TWA"),
        unit="Nanoseconds",
    )
    def _expression_insertage(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "InsertAge")

    @analytic_expression(
        "LastMod",
        pattern=_liquidity_raw_pattern("LastMod", "TWA"),
        unit="Nanoseconds",
    )
    def _expression_lastmod(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "LastMod")

    def expressions(
        self, ctx: AnalyticContext, config: L2LiquidityConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        if "TWA" not in config.aggregations:
            return []
        side = variant["sides"]
        level = variant["levels"]
        measure = variant["measures"]

        expression_fn = self.EXPRESSIONS.get(measure)
        if expression_fn is None:
            return []
        raw = expression_fn(self, side, level)
        if raw is None:
            return []

        alias = (
            config.output_name_pattern.format(**variant)
            if config.output_name_pattern
            else f"{side}{measure}{level}TWA"
        )
        return [_twa(raw, config.market_states).alias(apply_metric_prefix(ctx, alias))]


class L2TWSpreadAnalytic(AnalyticSpec):
    MODULE = "l2_tw"
    ConfigModel = L2SpreadConfig
    DOCS = [
        AnalyticDoc(
            pattern=r"^SpreadRelTWA$",
            template="Time-weighted average of spread in relative terms within the TimeBucket.",
            unit="BPS",
        ),
        AnalyticDoc(
            pattern=r"^EventCount$",
            template="Number of L2 events in the TimeBucket.",
            unit="Orders",
        ),
        AnalyticDoc(
            pattern=r"^(?P<source>Mid|Ask|Bid)TWA$",
            template="Time-weighted average of {source} price within the TimeBucket.",
            unit="XLOC",
        ),
    ]

    @analytic_expression(
        "Abs",
        pattern=r"^SpreadAbsTWA$",
        unit="XLOC",
    )
    def _expression_abs(self):
        """Time-weighted average of spread in absolute price terms within the TimeBucket."""
        return pl.col("AskPrice1") - pl.col("BidPrice1")

    @analytic_expression(
        "BPS",
        pattern=r"^SpreadBpsTWA$",
        unit="BPS",
    )
    def _expression_bps(self):
        """Time-weighted average of spread in basis points within the TimeBucket."""
        return (
            20000
            * (pl.col("AskPrice1") - pl.col("BidPrice1"))
            / (pl.col("AskPrice1") + pl.col("BidPrice1"))
        )

    def expressions(
        self, ctx: AnalyticContext, config: L2SpreadConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        if "TWA" not in config.aggregations:
            return []
        v_type = variant["variant"]
        expression_fn = self.EXPRESSIONS.get(v_type)
        if expression_fn is None:
            return []
        raw = expression_fn(self)
        if config.output_name_pattern:
            alias = config.output_name_pattern.format(**variant)
        elif v_type == "BPS":
            alias = "SpreadBpsTWA"
        else:
            alias = f"Spread{v_type}TWA"
        return [_twa(raw, config.market_states).alias(apply_metric_prefix(ctx, alias))]


class L2TWImbalanceAnalytic(AnalyticSpec):
    MODULE = "l2_tw"
    ConfigModel = L2ImbalanceConfig

    @analytic_expression(
        "CumQuantity",
        pattern=_imbalance_raw_pattern("CumQuantity", "TWA"),
        unit="Imbalance",
    )
    def _expression_cumquantity(self, level: int):
        """Time-weighted average of order book imbalance for {measure} up to level {level}, expressed as a normalized difference between bid and ask."""
        return _imbalance_raw(level, "CumQuantity")

    @analytic_expression(
        "Orders",
        pattern=_imbalance_raw_pattern("Orders", "TWA"),
        unit="Imbalance",
    )
    def _expression_orders(self, level: int):
        """Time-weighted average of order book imbalance for {measure} up to level {level}, expressed as a normalized difference between bid and ask."""
        return _imbalance_raw(level, "Orders")

    @analytic_expression(
        "CumNotional",
        pattern=_imbalance_raw_pattern("CumNotional", "TWA"),
        unit="Imbalance",
    )
    def _expression_cumnotional(self, level: int):
        """Time-weighted average of order book imbalance for {measure} up to level {level}, expressed as a normalized difference between bid and ask."""
        return _imbalance_raw(level, "CumNotional")

    def expressions(
        self, ctx: AnalyticContext, config: L2ImbalanceConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        if "TWA" not in config.aggregations:
            return []
        level = variant["levels"]
        measure = variant["measure"]

        expression_fn = self.EXPRESSIONS.get(measure)
        if expression_fn is None:
            return []
        raw = expression_fn(self, level)
        alias = (
            config.output_name_pattern.format(**variant)
            if config.output_name_pattern
            else f"Imbalance{measure}{level}TWA"
        )
        return [_twa(raw, config.market_states).alias(apply_metric_prefix(ctx, alias))]


class L2TWVolatilityAnalytic(AnalyticSpec):
    MODULE = "l2_tw"
    ConfigModel = L2VolatilityConfig

    @analytic_expression(
        "Volatility",
        pattern=r"^L2Volatility(?P<source>Mid|Bid|Ask|WeightedMid)(?P<agg>First|Last|Min|Max|Mean|Sum|Median|Std)$",
        unit="Percentage",
    )
    def _expression_volatility(self):
        """Aggregation ({agg}) of log-returns of {source} price within TimeBucket; Std is annualized."""
        return None

    def expressions(
        self, ctx: AnalyticContext, config: L2VolatilityConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        source = variant["source"]
        p = _price_series(source)
        if p is None:
            return []

        log_ret = (p / p.shift(1)).log()

        agg_map = {
            "First": log_ret.first(),
            "Last": log_ret.last(),
            "Min": log_ret.min(),
            "Max": log_ret.max(),
            "Mean": log_ret.mean(),
            "Sum": log_ret.sum(),
            "Median": log_ret.median(),
            "Std": log_ret.std(),
        }

        expressions = []
        for agg in config.aggregations:
            if agg not in agg_map:
                logging.warning(f"Unsupported aggregation '{agg}' for L2 volatility.")
                continue

            expr = agg_map[agg]

            if agg == "Std":
                seconds_in_year = 252 * 24 * 60 * 60
                bucket_seconds = ctx.cache.get("time_bucket_seconds")
                if not bucket_seconds:
                    logging.warning("time_bucket_seconds missing for L2 volatility.")
                    continue
                factor = (seconds_in_year * pl.len() / bucket_seconds).sqrt()
                expr = expr * factor

            alias = (
                config.output_name_pattern.format(**variant, agg=agg)
                if config.output_name_pattern
                else f"L2Volatility{source}{agg}"
            )
            expressions.append(expr.alias(apply_metric_prefix(ctx, alias)))

        return expressions


# =============================
# Analytics Modules
# =============================


@register_analytics("l2", config_attr="l2_analytics")
class L2AnalyticsLast(BaseAnalytics):
    """
    Computes L2 order book analytics based on the last snapshot in each time bucket.
    """

    REQUIRES = ["l2"]

    def __init__(self, config: L2AnalyticsConfig):
        self.config = config
        super().__init__("l2last", {}, metric_prefix=config.metric_prefix)

    def compute(self) -> pl.LazyFrame:
        gcols = ["MIC", "ListingId", "Ticker", "TimeBucket", "CurrencyCode"]
        ctx = AnalyticContext(
            base_df=self.l2,
            cache={"metric_prefix": self.metric_prefix},
            context=self.context,
        )

        liquidity = L2LastLiquidityAnalytic()
        spread = L2LastSpreadAnalytic()
        imbalance = L2LastImbalanceAnalytic()
        ohlc = L2LastOHLCAnalytic()

        expressions: List[pl.Expr] = build_expressions(
            ctx,
            [
                (liquidity, self.config.liquidity),
                (spread, self.config.spreads),
                (imbalance, self.config.imbalances),
            ],
        )
        ohlc_specs = []

        for req in self.config.ohlc:
            for variant in req.expand():
                source = variant["source"]
                if source is None:
                    continue
                ohlc_exprs = ohlc.expressions(ctx, req, variant)
                if not ohlc_exprs:
                    continue
                names = self._ohlc_names(req, variant)
                expressions.extend(ohlc_exprs)
                ohlc_specs.append(
                    {
                        "open_mode": variant.get("open_mode", "event"),
                        "names": names,
                    }
                )

        if not (
            self.config.liquidity
            or self.config.spreads
            or self.config.imbalances
            or self.config.ohlc
        ):
            levels = list(range(1, self.config.levels + 1))
            default_configs = [
                (
                    liquidity,
                    [
                        L2LiquidityConfig(
                            sides=["Bid", "Ask"],
                            levels=levels,
                            measures=[
                                "Price",
                                "Quantity",
                                "NumOrders",
                                "CumQuantity",
                                "CumOrders",
                            ],
                        )
                    ],
                ),
                (
                    imbalance,
                    [
                        L2ImbalanceConfig(
                            levels=levels, measure=["CumQuantity", "Orders"]
                        )
                    ],
                ),
                (spread, [L2SpreadConfig(variant=["BPS"])]),
            ]
            expressions.extend(build_expressions(ctx, default_configs))

        expressions.append(
            pl.col("MarketState").last().alias(self.apply_prefix("MarketState"))
        )

        l2_last = self.l2.group_by(gcols).agg(expressions)

        if any(spec["open_mode"] == "prev_close" for spec in ohlc_specs):
            l2_last = self._ensure_dense_time_buckets(l2_last, gcols)
            for spec in ohlc_specs:
                if spec["open_mode"] != "prev_close":
                    continue
                l2_last = self._apply_prev_close_ohlc(l2_last, gcols, spec["names"])

        self.df = l2_last
        return l2_last

    def _ensure_dense_time_buckets(
        self, df: pl.LazyFrame, gcols: list[str]
    ) -> pl.LazyFrame:
        if not self.config.time_bucket_seconds:
            raise ValueError(
                "time_bucket_seconds must be set for OHLC prev_close mode."
            )
        group_cols = [c for c in gcols if c != "TimeBucket"]
        df = df.with_columns(pl.col("TimeBucket").cast(pl.Datetime("ns")))
        interval = int(self.config.time_bucket_seconds * 1e9)
        frequency = f"{interval}ns"

        ranges = (
            df.group_by(group_cols)
            .agg(
                pl.datetime_range(
                    start=pl.col("TimeBucket").min(),
                    end=pl.col("TimeBucket").max(),
                    interval=frequency,
                    closed="both",
                )
                .cast(pl.Datetime("ns"))
                .alias("TimeBucket")
            )
            .explode("TimeBucket")
        )

        return ranges.join(df, on=group_cols + ["TimeBucket"], how="left")


@register_analytics("l2tw", config_attr="l2_analytics")
class L2AnalyticsTW(BaseTWAnalytics):
    """
    Computes time-weighted average (TWA) analytics.
    """

    REQUIRES = ["l2"]

    def __init__(self, config: L2AnalyticsConfig):
        super().__init__(
            "l2tw",
            {},
            nanoseconds=(
                int(config.time_bucket_seconds * 1e9)
                if config.time_bucket_seconds
                else 0
            ),
            metric_prefix=config.metric_prefix,
        )
        self.config = config

    def tw_analytics(self, l2: pl.LazyFrame, **kwargs) -> pl.LazyFrame:
        l2 = l2.group_by(["ListingId", "TimeBucket"])
        ctx = AnalyticContext(
            base_df=l2,
            cache={
                "time_bucket_seconds": self.config.time_bucket_seconds,
                "metric_prefix": self.metric_prefix,
            },
            context=self.context,
        )

        liquidity = L2TWLiquidityAnalytic()
        spread = L2TWSpreadAnalytic()
        imbalance = L2TWImbalanceAnalytic()
        volatility = L2TWVolatilityAnalytic()

        expressions: List[pl.Expr] = build_expressions(
            ctx,
            [
                (spread, self.config.spreads),
                (liquidity, self.config.liquidity),
                (imbalance, self.config.imbalances),
                (volatility, self.config.volatility),
            ],
        )

        if not expressions:
            default_spread_config = [
                L2SpreadConfig(variant=["Abs", "BPS"], aggregations=["TWA"])
            ]
            default_liquidity_config = [
                L2LiquidityConfig(
                    sides=["Bid", "Ask"],
                    levels=list(range(1, self.config.levels + 1)),
                    measures=["Price", "Quantity"],
                    aggregations=["TWA"],
                )
            ]
            expressions.extend(
                build_expressions(
                    ctx,
                    [
                        (spread, default_spread_config),
                        (liquidity, default_liquidity_config),
                    ],
                )
            )
            expressions.extend(
                [
                    (
                        (pl.col("AskPrice1") - pl.col("BidPrice1"))
                        / (pl.col("AskPrice1") + pl.col("BidPrice1"))
                        * 20000
                    )
                    .mul(pl.col("DT"))
                    .sum()
                    .truediv(pl.col("DT").sum())
                    .alias(self.apply_prefix("SpreadRelTWA")),
                    (0.5 * (pl.col("AskPrice1") + pl.col("BidPrice1")))
                    .mul(pl.col("DT"))
                    .sum()
                    .truediv(pl.col("DT").sum())
                    .alias(self.apply_prefix("MidTWA")),
                    pl.col("AskPrice1")
                    .mul(pl.col("DT"))
                    .sum()
                    .truediv(pl.col("DT").sum())
                    .alias(self.apply_prefix("AskTWA")),
                    pl.col("BidPrice1")
                    .mul(pl.col("DT"))
                    .sum()
                    .truediv(pl.col("DT").sum())
                    .alias(self.apply_prefix("BidTWA")),
                ]
            )

        return l2.agg(expressions)
