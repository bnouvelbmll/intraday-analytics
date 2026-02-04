import logging
import polars as pl
from intraday_analytics.bases import BaseAnalytics, BaseTWAnalytics
from pydantic import BaseModel, Field
from typing import Optional, List, Union, Literal, Dict, Any

from .common import CombinatorialMetricConfig, Side, AggregationMethod
from intraday_analytics.analytics_base import AnalyticSpec, AnalyticContext, AnalyticDoc, analytic_handler
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
        ..., description="Side of the book to analyze (Bid, Ask)."
    )

    levels: Union[int, List[int]] = Field(
        ...,
        description="Book levels to query (1-based index). Can be a single int or a list of ints.",
    )

    measures: Union[L2LiquidityMeasure, List[L2LiquidityMeasure]] = Field(
        ..., description="The specific measure to extract (e.g., 'Quantity', 'Price')."
    )


class L2SpreadConfig(CombinatorialMetricConfig):
    """
    Configuration for Bid-Ask Spread analytics.
    """

    metric_type: Literal["L2_Spread"] = "L2_Spread"

    variant: Union[Literal["Abs", "BPS"], List[Literal["Abs", "BPS"]]] = Field(
        default=["BPS"],
        description="Spread variant: 'Abs' (Ask-Bid) or 'BPS' (Basis Points relative to Mid).",
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
    )

    measure: Union[L2ImbalanceMeasure, List[L2ImbalanceMeasure]] = Field(
        default="CumQuantity",
        description="The underlying measure to use for the imbalance calculation.",
    )


class L2VolatilityConfig(CombinatorialMetricConfig):
    """
    Configuration for Price Volatility (Standard Deviation of log returns price series).
    """

    metric_type: Literal["L2_Volatility"] = "L2_Volatility"

    source: Union[Literal["Mid", "Bid", "Ask", "Last", "WeightedMid"], List[str]] = (
        Field(default="Mid", description="The price series to measure volatility on.")
    )

    aggregations: List[AggregationMethod] = ["Std"]


class L2OHLCConfig(CombinatorialMetricConfig):
    """
    Configuration for OHLC bars computed from L2 price series.
    """

    metric_type: Literal["L2_OHLC"] = "L2_OHLC"

    source: Union[Literal["Mid", "Bid", "Ask", "WeightedMid"], List[str]] = Field(
        default="Mid", description="The price series to compute OHLC on."
    )

    open_mode: Literal["event", "prev_close"] = Field(
        default="event",
        description="How to compute Open for empty buckets: 'event' uses first event; "
        "'prev_close' uses previous Close and fills empty buckets.",
    )


class L2AnalyticsConfig(BaseModel):
    ENABLED: bool = True
    time_bucket_seconds: Optional[float] = None
    time_bucket_anchor: Literal["end", "start"] = "end"
    time_bucket_closed: Literal["right", "left"] = "right"

    liquidity: List[L2LiquidityConfig] = Field(default_factory=list)
    spreads: List[L2SpreadConfig] = Field(default_factory=list)
    imbalances: List[L2ImbalanceConfig] = Field(default_factory=list)
    volatility: List[L2VolatilityConfig] = Field(default_factory=list)
    ohlc: List[L2OHLCConfig] = Field(default_factory=list)

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
    if measure in ["Quantity", "Price", "NumOrders", "InsertAge", "LastMod", "SizeAhead"]:
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


def _imbalance_raw(level: int, measure: str) -> pl.Expr:
    if measure == "CumQuantity":
        bid_val = pl.sum_horizontal(
            [f"BidQuantity{i}" for i in range(1, level + 1)]
        )
        ask_val = pl.sum_horizontal(
            [f"AskQuantity{i}" for i in range(1, level + 1)]
        )
    elif measure == "Orders":
        bid_val = pl.sum_horizontal(
            [f"BidNumOrders{i}" for i in range(1, level + 1)]
        )
        ask_val = pl.sum_horizontal(
            [f"AskNumOrders{i}" for i in range(1, level + 1)]
        )
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

    @analytic_handler(
        "Price",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>Price|CumNotional)(?P<level>\d+)$",
        unit="XLOC",
    )
    def _handle_price(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "Price")

    @analytic_handler(
        "CumNotional",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>Price|CumNotional)(?P<level>\d+)$",
        unit="XLOC",
    )
    def _handle_cumnotional(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "CumNotional")

    @analytic_handler(
        "Quantity",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>Quantity|CumQuantity|SizeAhead)(?P<level>\d+)$",
        unit="Shares",
    )
    def _handle_quantity(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "Quantity")

    @analytic_handler(
        "CumQuantity",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>Quantity|CumQuantity|SizeAhead)(?P<level>\d+)$",
        unit="Shares",
    )
    def _handle_cumquantity(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "CumQuantity")

    @analytic_handler(
        "SizeAhead",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>Quantity|CumQuantity|SizeAhead)(?P<level>\d+)$",
        unit="Shares",
    )
    def _handle_sizeahead(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "SizeAhead")

    @analytic_handler(
        "NumOrders",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>NumOrders|CumOrders)(?P<level>\d+)$",
        unit="Orders",
    )
    def _handle_numorders(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "NumOrders")

    @analytic_handler(
        "CumOrders",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>NumOrders|CumOrders)(?P<level>\d+)$",
        unit="Orders",
    )
    def _handle_cumorders(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "CumOrders")

    @analytic_handler(
        "InsertAge",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>InsertAge|LastMod)(?P<level>\d+)$",
        unit="Nanoseconds",
    )
    def _handle_insertage(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "InsertAge")

    @analytic_handler(
        "LastMod",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>InsertAge|LastMod)(?P<level>\d+)$",
        unit="Nanoseconds",
    )
    def _handle_lastmod(self, side: str, level: int):
        """{side} {measure} at book level {level} (last snapshot in TimeBucket)."""
        return _liquidity_raw(side, level, "LastMod")

    def expressions(
        self, ctx: AnalyticContext, config: L2LiquidityConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        side = variant["sides"]
        level = variant["levels"]
        measure = variant["measures"]

        handler = self.HANDLERS.get(measure)
        if handler is None:
            return []
        base_expr = handler(self, side, level)
        if base_expr is None:
            return []

        if config.market_states:
            base_expr = base_expr.filter(pl.col("MarketState").is_in(config.market_states))

        expr = base_expr.last()
        alias = (
            config.output_name_pattern.format(**variant)
            if config.output_name_pattern
            else f"{side}{measure}{level}"
        )
        return [expr.alias(alias)]


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

    @analytic_handler(
        "Abs",
        pattern=r"^SpreadAbs$",
        unit="XLOC",
    )
    def _handle_abs(self):
        """Best ask minus best bid in absolute price terms using the last snapshot in the TimeBucket."""
        return pl.col("AskPrice1") - pl.col("BidPrice1")

    @analytic_handler(
        "BPS",
        pattern=r"^SpreadBPS$",
        unit="BPS",
    )
    def _handle_bps(self):
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
        handler = self.HANDLERS.get(v_type)
        if handler is None:
            return []
        base_expr = handler(self)

        if config.market_states:
            base_expr = base_expr.filter(pl.col("MarketState").is_in(config.market_states))

        expr = base_expr.last()
        alias = (
            config.output_name_pattern.format(**variant)
            if config.output_name_pattern
            else f"Spread{v_type}"
        )
        return [expr.alias(alias)]


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

    @analytic_handler(
        "CumQuantity",
        pattern=r"^Imbalance(?P<measure>CumQuantity|Orders|CumNotional)(?P<level>\d+)$",
        unit="Imbalance",
    )
    def _handle_cumquantity(self, level: int):
        """Order book imbalance for {measure} up to level {level} using the last snapshot, expressed as a normalized difference between bid and ask."""
        return _imbalance_raw(level, "CumQuantity")

    @analytic_handler(
        "Orders",
        pattern=r"^Imbalance(?P<measure>CumQuantity|Orders|CumNotional)(?P<level>\d+)$",
        unit="Imbalance",
    )
    def _handle_orders(self, level: int):
        """Order book imbalance for {measure} up to level {level} using the last snapshot, expressed as a normalized difference between bid and ask."""
        return _imbalance_raw(level, "Orders")

    @analytic_handler(
        "CumNotional",
        pattern=r"^Imbalance(?P<measure>CumQuantity|Orders|CumNotional)(?P<level>\d+)$",
        unit="Imbalance",
    )
    def _handle_cumnotional(self, level: int):
        """Order book imbalance for {measure} up to level {level} using the last snapshot, expressed as a normalized difference between bid and ask."""
        return _imbalance_raw(level, "CumNotional")

    def expressions(
        self, ctx: AnalyticContext, config: L2ImbalanceConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        level = variant["levels"]
        measure = variant["measure"]

        handler = self.HANDLERS.get(measure)
        if handler is None:
            return []
        base_expr = handler(self, level)

        if config.market_states:
            base_expr = base_expr.filter(pl.col("MarketState").is_in(config.market_states))

        expr = base_expr.last()
        alias = (
            config.output_name_pattern.format(**variant)
            if config.output_name_pattern
            else f"Imbalance{measure}{level}"
        )
        return [expr.alias(alias)]


class L2LastOHLCAnalytic(AnalyticSpec):
    MODULE = "l2_last"
    ConfigModel = L2OHLCConfig

    @analytic_handler(
        "OHLC",
        pattern=r"^(?P<source>Bid|Ask|Mid|WeightedMid)(?P<ohlc>Open|High|Low|Close)$",
        unit="XLOC",
        group="L2OHLC{source}",
        group_role="{ohlc}",
        group_semantics="ohlc_bar,ffill_non_naive",
    )
    def _handle_ohlc(self):
        """L2 {ohlc} for {source} price within the TimeBucket."""
        return None

    def expressions(
        self, ctx: AnalyticContext, config: L2OHLCConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        source = variant["source"]
        p = _price_series(source)
        if p is None:
            return []

        expressions = []
        for ohlc in ["Open", "High", "Low", "Close"]:
            variant_with_ohlc = {**variant, "ohlc": ohlc}
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
            expressions.append(expr.alias(alias))
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

    @analytic_handler(
        "Price",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>Price|CumNotional)(?P<level>\d+)TWA$",
        unit="XLOC",
    )
    def _handle_price(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "Price")

    @analytic_handler(
        "CumNotional",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>Price|CumNotional)(?P<level>\d+)TWA$",
        unit="XLOC",
    )
    def _handle_cumnotional(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "CumNotional")

    @analytic_handler(
        "Quantity",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>Quantity|CumQuantity|SizeAhead)(?P<level>\d+)TWA$",
        unit="Shares",
    )
    def _handle_quantity(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "Quantity")

    @analytic_handler(
        "CumQuantity",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>Quantity|CumQuantity|SizeAhead)(?P<level>\d+)TWA$",
        unit="Shares",
    )
    def _handle_cumquantity(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "CumQuantity")

    @analytic_handler(
        "SizeAhead",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>Quantity|CumQuantity|SizeAhead)(?P<level>\d+)TWA$",
        unit="Shares",
    )
    def _handle_sizeahead(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "SizeAhead")

    @analytic_handler(
        "NumOrders",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>NumOrders|CumOrders)(?P<level>\d+)TWA$",
        unit="Orders",
    )
    def _handle_numorders(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "NumOrders")

    @analytic_handler(
        "CumOrders",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>NumOrders|CumOrders)(?P<level>\d+)TWA$",
        unit="Orders",
    )
    def _handle_cumorders(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "CumOrders")

    @analytic_handler(
        "InsertAge",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>InsertAge|LastMod)(?P<level>\d+)TWA$",
        unit="Nanoseconds",
    )
    def _handle_insertage(self, side: str, level: int):
        """{side} {measure} at level {level}, time-weighted average over TimeBucket."""
        return _liquidity_raw(side, level, "InsertAge")

    @analytic_handler(
        "LastMod",
        pattern=r"^(?P<side>Bid|Ask)(?P<measure>InsertAge|LastMod)(?P<level>\d+)TWA$",
        unit="Nanoseconds",
    )
    def _handle_lastmod(self, side: str, level: int):
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

        handler = self.HANDLERS.get(measure)
        if handler is None:
            return []
        raw = handler(self, side, level)
        if raw is None:
            return []

        alias = (
            config.output_name_pattern.format(**variant)
            if config.output_name_pattern
            else f"{side}{measure}{level}TWA"
        )
        return [_twa(raw, config.market_states).alias(alias)]


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

    @analytic_handler(
        "Abs",
        pattern=r"^SpreadAbsTWA$",
        unit="XLOC",
    )
    def _handle_abs(self):
        """Time-weighted average of spread in absolute price terms within the TimeBucket."""
        return pl.col("AskPrice1") - pl.col("BidPrice1")

    @analytic_handler(
        "BPS",
        pattern=r"^SpreadBPSTWA$",
        unit="BPS",
    )
    def _handle_bps(self):
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
        handler = self.HANDLERS.get(v_type)
        if handler is None:
            return []
        raw = handler(self)
        alias = (
            config.output_name_pattern.format(**variant)
            if config.output_name_pattern
            else f"Spread{v_type}TWA"
        )
        return [_twa(raw, config.market_states).alias(alias)]


class L2TWImbalanceAnalytic(AnalyticSpec):
    MODULE = "l2_tw"
    ConfigModel = L2ImbalanceConfig

    @analytic_handler(
        "CumQuantity",
        pattern=r"^Imbalance(?P<measure>CumQuantity|Orders|CumNotional)(?P<level>\d+)TWA$",
        unit="Imbalance",
    )
    def _handle_cumquantity(self, level: int):
        """Time-weighted average of order book imbalance for {measure} up to level {level}, expressed as a normalized difference between bid and ask."""
        return _imbalance_raw(level, "CumQuantity")

    @analytic_handler(
        "Orders",
        pattern=r"^Imbalance(?P<measure>CumQuantity|Orders|CumNotional)(?P<level>\d+)TWA$",
        unit="Imbalance",
    )
    def _handle_orders(self, level: int):
        """Time-weighted average of order book imbalance for {measure} up to level {level}, expressed as a normalized difference between bid and ask."""
        return _imbalance_raw(level, "Orders")

    @analytic_handler(
        "CumNotional",
        pattern=r"^Imbalance(?P<measure>CumQuantity|Orders|CumNotional)(?P<level>\d+)TWA$",
        unit="Imbalance",
    )
    def _handle_cumnotional(self, level: int):
        """Time-weighted average of order book imbalance for {measure} up to level {level}, expressed as a normalized difference between bid and ask."""
        return _imbalance_raw(level, "CumNotional")

    def expressions(
        self, ctx: AnalyticContext, config: L2ImbalanceConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        if "TWA" not in config.aggregations:
            return []
        level = variant["levels"]
        measure = variant["measure"]

        handler = self.HANDLERS.get(measure)
        if handler is None:
            return []
        raw = handler(self, level)
        alias = (
            config.output_name_pattern.format(**variant)
            if config.output_name_pattern
            else f"Imbalance{measure}{level}TWA"
        )
        return [_twa(raw, config.market_states).alias(alias)]


class L2TWVolatilityAnalytic(AnalyticSpec):
    MODULE = "l2_tw"
    ConfigModel = L2VolatilityConfig

    @analytic_handler(
        "Volatility",
        pattern=r"^L2Volatility(?P<source>Mid|Bid|Ask|WeightedMid)(?P<agg>First|Last|Min|Max|Mean|Sum|Median|Std)$",
        unit="Percentage",
    )
    def _handle_volatility(self):
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
                logging.warning(
                    f"Unsupported aggregation '{agg}' for L2 volatility."
                )
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
            expressions.append(expr.alias(alias))

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
        super().__init__("l2last", {})

    def compute(self) -> pl.LazyFrame:
        gcols = ["MIC", "ListingId", "Ticker", "TimeBucket", "CurrencyCode"]
        ctx = AnalyticContext(base_df=self.l2, cache={}, context=self.context)

        expressions: List[pl.Expr] = []
        ohlc_specs = []

        liquidity = L2LastLiquidityAnalytic()
        spread = L2LastSpreadAnalytic()
        imbalance = L2LastImbalanceAnalytic()
        ohlc = L2LastOHLCAnalytic()

        for req in self.config.liquidity:
            for variant in req.expand():
                expressions.extend(liquidity.expressions(ctx, req, variant))

        for req in self.config.spreads:
            for variant in req.expand():
                expressions.extend(spread.expressions(ctx, req, variant))

        for req in self.config.imbalances:
            for variant in req.expand():
                expressions.extend(imbalance.expressions(ctx, req, variant))

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

        if not expressions:
            N = self.config.levels
            for i in range(1, N + 1):
                for side in ["Bid", "Ask"]:
                    expressions.append(pl.col(f"{side}Price{i}").last())
                    expressions.append(pl.col(f"{side}Quantity{i}").last())
                    expressions.append(pl.col(f"{side}NumOrders{i}").last())
                    expressions.append(
                        pl.sum_horizontal(
                            [f"{side}Quantity{j}" for j in range(1, i + 1)]
                        )
                        .last()
                        .alias(f"{side}CumQuantity{i}")
                    )
                    expressions.append(
                        pl.sum_horizontal(
                            [f"{side}NumOrders{j}" for j in range(1, i + 1)]
                        )
                        .last()
                        .alias(f"{side}CumOrders{i}")
                    )

            for i in range(1, N + 1):
                bid_cum = pl.sum_horizontal(
                    [f"BidQuantity{j}" for j in range(1, i + 1)]
                )
                ask_cum = pl.sum_horizontal(
                    [f"AskQuantity{j}" for j in range(1, i + 1)]
                )
                expressions.append(
                    ((bid_cum - ask_cum) / (bid_cum + ask_cum))
                    .last()
                    .alias(f"VolumeImbalance{i}")
                )

                bid_ord = pl.sum_horizontal(
                    [f"BidNumOrders{j}" for j in range(1, i + 1)]
                )
                ask_ord = pl.sum_horizontal(
                    [f"AskNumOrders{j}" for j in range(1, i + 1)]
                )
                expressions.append(
                    ((bid_ord - ask_ord) / (bid_ord + ask_ord))
                    .last()
                    .alias(f"OrdersImbalance{i}")
                )

            expressions.append(
                (
                    20000
                    * (pl.col("AskPrice1") - pl.col("BidPrice1"))
                    / (pl.col("AskPrice1") + pl.col("BidPrice1"))
                )
                .last()
                .alias("SpreadBps")
            )

        expressions.append(pl.col("MarketState").last())

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
            raise ValueError("time_bucket_seconds must be set for OHLC prev_close mode.")
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

    def _apply_prev_close_ohlc(
        self, df: pl.LazyFrame, gcols: list[str], names: dict[str, str]
    ) -> pl.LazyFrame:
        group_cols = [c for c in gcols if c != "TimeBucket"]
        open_col = names["Open"]
        high_col = names["High"]
        low_col = names["Low"]
        close_col = names["Close"]
        temp_col = f"__{close_col}_filled"

        no_event = (
            pl.col(open_col).is_null()
            & pl.col(high_col).is_null()
            & pl.col(low_col).is_null()
            & pl.col(close_col).is_null()
        )

        df = df.with_columns(
            pl.when(no_event)
            .then(pl.col(close_col).shift(1).over(group_cols))
            .otherwise(pl.col(close_col))
            .alias(temp_col)
        ).with_columns(pl.col(temp_col).forward_fill().over(group_cols).alias(temp_col))

        return df.with_columns(
            [
                pl.when(no_event)
                .then(pl.col(temp_col))
                .otherwise(pl.col(open_col))
                .alias(open_col),
                pl.when(no_event)
                .then(pl.col(temp_col))
                .otherwise(pl.col(high_col))
                .alias(high_col),
                pl.when(no_event)
                .then(pl.col(temp_col))
                .otherwise(pl.col(low_col))
                .alias(low_col),
                pl.col(temp_col).alias(close_col),
            ]
        ).drop(temp_col)

    @staticmethod
    def _ohlc_names(req: L2OHLCConfig, variant: dict) -> dict[str, str]:
        source = variant["source"]
        names = {}
        for ohlc in ["Open", "High", "Low", "Close"]:
            variant_with_ohlc = {**variant, "ohlc": ohlc}
            default_name = f"{source}{ohlc}"
            alias = (
                req.output_name_pattern.format(**variant_with_ohlc)
                if req.output_name_pattern
                else default_name
            )
            names[ohlc] = alias
        return names


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
            nanoseconds=int(config.time_bucket_seconds * 1e9),
        )
        self.config = config

    def tw_analytics(self, l2: pl.LazyFrame, **kwargs) -> pl.LazyFrame:
        l2 = l2.group_by(["ListingId", "TimeBucket"])
        ctx = AnalyticContext(
            base_df=l2,
            cache={"time_bucket_seconds": self.config.time_bucket_seconds},
            context=self.context,
        )

        expressions: List[pl.Expr] = []

        liquidity = L2TWLiquidityAnalytic()
        spread = L2TWSpreadAnalytic()
        imbalance = L2TWImbalanceAnalytic()
        volatility = L2TWVolatilityAnalytic()

        for req in self.config.spreads:
            for variant in req.expand():
                expressions.extend(spread.expressions(ctx, req, variant))

        for req in self.config.liquidity:
            for variant in req.expand():
                expressions.extend(liquidity.expressions(ctx, req, variant))

        for req in self.config.imbalances:
            for variant in req.expand():
                expressions.extend(imbalance.expressions(ctx, req, variant))

        for req in self.config.volatility:
            for variant in req.expand():
                expressions.extend(volatility.expressions(ctx, req, variant))

        if not expressions:
            expressions.append(
                (
                    (pl.col("AskPrice1") - pl.col("BidPrice1"))
                    / (pl.col("AskPrice1") + pl.col("BidPrice1"))
                    * 20000
                )
                .mul(pl.col("DT"))
                .sum()
                .truediv(pl.col("DT").sum())
                .alias("SpreadRelTWA")
            )
            expressions.append(
                (
                    0.5 * (pl.col("AskPrice1") + pl.col("BidPrice1"))
                )
                .mul(pl.col("DT"))
                .sum()
                .truediv(pl.col("DT").sum())
                .alias("MidTWA")
            )
            expressions.append(
                pl.col("AskPrice1")
                .mul(pl.col("DT"))
                .sum()
                .truediv(pl.col("DT").sum())
                .alias("AskTWA")
            )
            expressions.append(
                pl.col("BidPrice1")
                .mul(pl.col("DT"))
                .sum()
                .truediv(pl.col("DT").sum())
                .alias("BidTWA")
            )

        expressions.append(pl.col("EventTimestamp").len().alias("EventCount"))

        return l2.agg(expressions)

