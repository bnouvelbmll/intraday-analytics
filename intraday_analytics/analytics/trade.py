import polars as pl
from intraday_analytics.analytics_base import BaseAnalytics
from pydantic import BaseModel, Field
from typing import List, Union, Literal, Optional, Dict, Any

from .common import (
    CombinatorialMetricConfig,
    Side,
    AggregationMethod,
    apply_aggregation,
)
from .utils import apply_market_state_filter, apply_alias
from intraday_analytics.analytics_base import (
    AnalyticSpec,
    AnalyticContext,
    AnalyticDoc,
    analytic_expression,
    apply_metric_prefix,
    build_expressions,
)
from intraday_analytics.analytics_registry import register_analytics


# =============================
# Configuration Models
# =============================

TradeGenericMeasure = Literal[
    "Volume",
    "Count",
    "NotionalEUR",
    "NotionalUSD",
    "RetailCount",
    "BlockCount",
    "AuctionNotional",
    "OTCVolume",
    "VWAP",
    "OHLC",
    "AvgPrice",
    "MedianPrice",
]

SideWithTotal = Literal["Bid", "Ask", "Total", "Unknown"]


class TradeGenericConfig(CombinatorialMetricConfig):
    """
    General Trade Statistics (Volume, Counts, Notionals, Prices).
    """

    metric_type: Literal["Trade_Generic"] = "Trade_Generic"

    sides: Union[SideWithTotal, List[SideWithTotal]] = Field(
        default="Total", description="Filter trades by aggressor side."
    )

    measures: Union[TradeGenericMeasure, List[TradeGenericMeasure]] = Field(
        ..., description="The trade attribute to aggregate."
    )


DiscrepancyReference = Literal[
    "MidAtPrimary",
    "EBBO",
    "PreTradeMid",
    "MidPrice",
    "BestBid",
    "BestAsk",
    "BestBidAtVenue",
    "BestAskAtVenue",
    "BestBidAtPrimary",
    "BestAskAtPrimary",
]


class TradeDiscrepancyConfig(CombinatorialMetricConfig):
    """
    Price Discrepancy Analytics (in BPS).
    Calculates: 10000 * (TradePrice - ReferencePrice) / ReferencePrice
    """

    metric_type: Literal["Trade_Discrepancy"] = "Trade_Discrepancy"

    references: Union[DiscrepancyReference, List[DiscrepancyReference]] = Field(
        ..., description="Reference price to compare against."
    )
    sides: Union[SideWithTotal, List[SideWithTotal]] = Field(
        default="Total", description="Filter trades by aggressor side."
    )
    aggregations: List[AggregationMethod] = Field(
        default_factory=lambda: ["Mean"],
        description="Aggregations to apply to discrepancy series.",
    )


TradeFlagType = Literal[
    "NegotiatedTrade",
    "OddLotTrade",
    "BlockTrade",
    "CrossTrade",
    "AlgorithmicTrade",
    "IcebergExecution",
]
TradeFlagMeasure = Literal["Volume", "Count", "AvgNotional"]


class TradeFlagConfig(CombinatorialMetricConfig):
    """
    Analytics filtered by specific trade flags (e.g., Negotiated, Odd Lot).
    """

    metric_type: Literal["Trade_Flag"] = "Trade_Flag"

    flags: Union[TradeFlagType, List[TradeFlagType]] = Field(
        ..., description="Trade flag to filter by."
    )

    sides: Union[SideWithTotal, List[SideWithTotal]] = Field(
        default="Total",
        description="Filter by aggressor side (applied on top of flag).",
    )

    measures: Union[TradeFlagMeasure, List[TradeFlagMeasure]] = Field(
        ..., description="Measure to compute for the flagged trades."
    )


ImpactMeasure = Literal[
    "PreTradeElapsedTimeChg", "PostTradeElapsedTimeChg", "PricePoint"
]
VenueScope = Literal["Local", "Primary", "Venue"]


class TradeChangeConfig(CombinatorialMetricConfig):
    """
    Analytics related to trade impact or state change.
    """

    metric_type: Literal["Trade_Change"] = "Trade_Change"

    measures: Union[ImpactMeasure, List[ImpactMeasure]] = Field(
        ..., description="Base measure name."
    )

    scopes: Union[VenueScope, List[VenueScope]] = Field(
        default="Local", description="Scope of the measure (Local, Primary, Venue)."
    )


class TradeImpactConfig(CombinatorialMetricConfig):
    """
    Trade Impact and Execution Quality analytics (TCA).
    """

    metric_type: Literal["Trade_Impact"] = "Trade_Impact"

    variant: Union[
        Literal["EffectiveSpread", "RealizedSpread", "PriceImpact"], List[str]
    ] = Field(..., description="TCA analytic type.")

    horizon: Union[str, List[str]] = Field(
        default="1s",
        description="Time horizon for post-trade analysis (e.g., '100ms', '1s').",
    )

    reference_price_col: str = Field(
        default="PreTradeMid",
        description="Column name for the reference price (T0 benchmark), e.g., 'PreTradeMid', 'MidPrice'.",
    )


class TradeAnalyticsConfig(BaseModel):
    ENABLED: bool = True
    metric_prefix: Optional[str] = None
    generic_metrics: List[TradeGenericConfig] = Field(default_factory=list)
    discrepancy_metrics: List[TradeDiscrepancyConfig] = Field(default_factory=list)
    flag_metrics: List[TradeFlagConfig] = Field(default_factory=list)
    change_metrics: List[TradeChangeConfig] = Field(default_factory=list)
    impact_metrics: List[TradeImpactConfig] = Field(default_factory=list)

    # Legacy support
    enable_retail_imbalance: bool = False
    use_tagged_trades: bool = False
    tagged_trades_context_key: str = "trades_iceberg"


class RetailImbalanceConfig(BaseModel):
    ENABLED: bool = True


# =============================
# Analytic Specs
# =============================


class TradeGenericAnalytic(AnalyticSpec):
    MODULE = "trade"
    ConfigModel = TradeGenericConfig

    def _build_filters(self, config: TradeGenericConfig, side: str):
        cond = pl.lit(True)
        if side == "Bid":
            cond = pl.col("AggressorSide") == 2
        elif side == "Ask":
            cond = pl.col("AggressorSide") == 1
        if config.market_states:
            cond = cond & pl.col("MarketState").is_in(config.market_states)
        return cond

    @staticmethod
    def _filtered(cond, col):
        return pl.when(cond).then(col).otherwise(None)

    @staticmethod
    def _filtered_zero(cond, col):
        return pl.when(cond).then(col).otherwise(0)

    @analytic_expression(
        "Volume",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>Volume)$",
        unit="Shares",
    )
    def _expression_volume(self, cond):
        """Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS)."""
        return self._filtered_zero(cond, pl.col("Size")).sum()

    @analytic_expression(
        "Count",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>Count)$",
        unit="Trades",
    )
    def _expression_count(self, cond):
        """Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS)."""
        return self._filtered_zero(cond, 1).sum()

    @analytic_expression(
        "NotionalEUR",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>NotionalEUR)$",
        unit="EUR",
    )
    def _expression_notional_eur(self, cond):
        """Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS)."""
        return self._filtered_zero(cond, pl.col("TradeNotionalEUR")).sum()

    @analytic_expression(
        "NotionalUSD",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>NotionalUSD)$",
        unit="USD",
    )
    def _expression_notional_usd(self, cond):
        """Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS)."""
        return self._filtered_zero(cond, pl.col("TradeNotionalUSD")).sum()

    @analytic_expression("RetailCount")
    def _expression_retail_count(self, cond):
        return self._filtered_zero(
            cond, pl.when(pl.col("BMLLParticipantType") == "RETAIL").then(1).otherwise(0)
        ).sum()

    @analytic_expression("BlockCount")
    def _expression_block_count(self, cond):
        return self._filtered_zero(
            cond, pl.when(pl.col("IsBlock") == "Y").then(1).otherwise(0)
        ).sum()

    @analytic_expression("AuctionNotional")
    def _expression_auction_notional(self, cond):
        return self._filtered_zero(
            cond,
            pl.when(pl.col("Classification").str.contains("AUCTION"))
            .then(pl.col("TradeNotionalEUR"))
            .otherwise(0),
        ).sum()

    @analytic_expression("OTCVolume")
    def _expression_otc_volume(self, cond):
        return self._filtered_zero(
            cond,
            pl.when(pl.col("Classification").str.contains("OTC"))
            .then(pl.col("Size"))
            .otherwise(0),
        ).sum()

    @analytic_expression(
        "VWAP",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>VWAP|AvgPrice|MedianPrice)$",
        unit="XLOC",
    )
    def _expression_vwap(self, cond):
        """Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS)."""
        num = self._filtered_zero(cond, pl.col("LocalPrice") * pl.col("Size")).sum()
        den = self._filtered_zero(cond, pl.col("Size")).sum()
        return num / den

    @analytic_expression(
        "AvgPrice",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>VWAP|AvgPrice|MedianPrice)$",
        unit="XLOC",
    )
    def _expression_avg_price(self, cond):
        """Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS)."""
        return self._filtered(cond, pl.col("LocalPrice")).mean()

    @analytic_expression(
        "MedianPrice",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>VWAP|AvgPrice|MedianPrice)$",
        unit="XLOC",
    )
    def _expression_median_price(self, cond):
        """Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS)."""
        return self._filtered(cond, pl.col("LocalPrice")).median()

    @analytic_expression(
        "OHLC",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<ohlc>Open|High|Low|Close)$",
        unit="XLOC",
        group="TradeOHLC{side}",
        group_role="{ohlc}",
        group_semantics="ohlc_bar,ffill_non_naive",
    )
    def _expression_ohlc(
        self,
        cond,
        config: TradeGenericConfig,
        variant: Dict[str, Any],
        prefix: str,
    ):
        """Trade {ohlc} for {side} trades within the TimeBucket (LIT_CONTINUOUS)."""
        side = variant["sides"]
        base_prefix = (
            f"Trade{side}"
            if not config.output_name_pattern
            else config.output_name_pattern.format(**variant)
        )
        if side == "Total" and not config.output_name_pattern:
            base_prefix = ""
        prefix = f"{prefix}{base_prefix}" if prefix else base_prefix
        price_col = self._filtered(cond, pl.col("LocalPrice")).drop_nans()
        names = [f"{prefix}Open", f"{prefix}High", f"{prefix}Low", f"{prefix}Close"]
        return [
            price_col.first().alias(names[0]),
            price_col.max().alias(names[1]),
            price_col.min().alias(names[2]),
            price_col.last().alias(names[3]),
        ]

    def expressions(
        self, ctx: AnalyticContext, config: TradeGenericConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        side = variant["sides"]
        measure = variant["measures"]
        cond = self._build_filters(config, side)

        expression_fn = self.EXPRESSIONS.get(measure)
        if expression_fn is None:
            return []

        if measure == "OHLC":
            prefix = apply_metric_prefix(ctx, "")
            return expression_fn(self, cond, config, variant, prefix)

        expr = expression_fn(self, cond)
        default_name = f"Trade{side}{measure}"
        return [
            apply_alias(
                expr,
                config.output_name_pattern,
                variant,
                default_name,
                prefix=ctx.cache.get("metric_prefix"),
            )
        ]


class TradeDiscrepancyAnalytic(AnalyticSpec):
    MODULE = "trade"
    DOCS = [
        AnalyticDoc(
            pattern=r"^DiscrepancyTo(?P<reference>.+?)(?P<side>Bid|Ask)?(?P<agg>First|Last|Min|Max|Mean|Sum|Median|Std)?$",
            template="Price difference between LocalPrice and {reference} expressed in basis points for {side_or_total} trades; aggregated by {agg_or_mean} within the TimeBucket.",
            unit="BPS",
        )
    ]
    ConfigModel = TradeDiscrepancyConfig

    def expressions(
        self, ctx: AnalyticContext, config: TradeDiscrepancyConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        ref_name = variant["references"]
        side = variant["sides"]
        ref_map = {
            "MidAtPrimary": "PostTradeMidAtPrimary",
            "EBBO": "PostTradeMid",
            "PreTradeMid": "PreTradeMid",
            "MidPrice": "MidPrice",
            "BestBid": "BestBidPrice",
            "BestAsk": "BestAskPrice",
            "BestBidAtVenue": "BestBidPriceAtVenue",
            "BestAskAtVenue": "BestAskPriceAtVenue",
            "BestBidAtPrimary": "BestBidPriceAtPrimary",
            "BestAskAtPrimary": "BestAskPriceAtPrimary",
        }
        col_name = ref_map.get(ref_name)
        if not col_name:
            return []

        price_col = pl.col("LocalPrice")
        ref_col = pl.col(col_name)
        expr = 10000 * (price_col - ref_col) / ref_col

        if side == "Bid":
            expr = pl.when(pl.col("AggressorSide") == 2).then(expr).otherwise(None)
        elif side == "Ask":
            expr = pl.when(pl.col("AggressorSide") == 1).then(expr).otherwise(None)

        expr = apply_market_state_filter(expr, config.market_states)

        outputs = []
        for agg in config.aggregations:
            agg_expr = apply_aggregation(expr, agg)
            if agg_expr is None:
                continue
            side_suffix = "" if side == "Total" else f"{side}"
            default_name = (
                f"DiscrepancyTo{ref_name}{side_suffix}"
                if agg == "Mean" and config.output_name_pattern is None
                else f"DiscrepancyTo{ref_name}{side_suffix}{agg}"
            )
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


class TradeFlagAnalytic(AnalyticSpec):
    MODULE = "trade"
    ConfigModel = TradeFlagConfig

    @analytic_expression(
        "Volume",
        pattern=r"^(?P<flag>NegotiatedTrade|OddLotTrade|BlockTrade|CrossTrade|AlgorithmicTrade|IcebergExecution)(?P<measure>Volume)(?P<side>Bid|Ask)?$",
        unit="Shares",
    )
    def _expression_volume(self, cond):
        """Trades with {flag} filter; {measure} over {side_or_total} trades in the TimeBucket."""
        return pl.when(cond).then(pl.col("Size")).otherwise(0).sum()

    @analytic_expression(
        "Count",
        pattern=r"^(?P<flag>NegotiatedTrade|OddLotTrade|BlockTrade|CrossTrade|AlgorithmicTrade|IcebergExecution)(?P<measure>Count)(?P<side>Bid|Ask)?$",
        unit="Trades",
    )
    def _expression_count(self, cond):
        """Trades with {flag} filter; {measure} over {side_or_total} trades in the TimeBucket."""
        return pl.when(cond).then(1).otherwise(0).sum()

    @analytic_expression(
        "AvgNotional",
        pattern=r"^(?P<flag>NegotiatedTrade|OddLotTrade|BlockTrade|CrossTrade|AlgorithmicTrade|IcebergExecution)(?P<measure>AvgNotional)(?P<side>Bid|Ask)?$",
        unit="EUR",
    )
    def _expression_avg_notional(self, cond):
        """Trades with {flag} filter; {measure} over {side_or_total} trades in the TimeBucket."""
        num = pl.when(cond).then(pl.col("TradeNotionalEUR")).otherwise(0).sum()
        den = pl.when(cond).then(1).otherwise(0).sum()
        return num / den

    def expressions(
        self, ctx: AnalyticContext, config: TradeFlagConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        flag_name = variant["flags"]
        side = variant["sides"]
        measure = variant["measures"]

        if flag_name == "IcebergExecution":
            try:
                if "IcebergExecution" not in ctx.base_df.collect_schema().names():
                    return []
            except Exception:
                return []

        flag_map = {
            "NegotiatedTrade": pl.col("NegotiatedTrade") == "Y",
            "OddLotTrade": pl.col("LotType") == "Odd Lot",
            "BlockTrade": pl.col("IsBlock") == "Y",
            "CrossTrade": pl.col("CrossingTrade") == "Y",
            "AlgorithmicTrade": pl.col("AlgorithmicTrade") == "Y",
            "IcebergExecution": pl.col("IcebergExecution").fill_null(False),
        }
        flag = flag_map.get(flag_name)
        if flag is None:
            return []

        cond = flag
        if side == "Bid":
            cond = cond & (pl.col("AggressorSide") == 2)
        elif side == "Ask":
            cond = cond & (pl.col("AggressorSide") == 1)

        if config.market_states:
            cond = cond & pl.col("MarketState").is_in(config.market_states)

        expression_fn = self.EXPRESSIONS.get(measure)
        if expression_fn is None:
            return []
        expr = expression_fn(self, cond)

        default_name = f"{flag_name}{measure}"
        if side != "Total":
            default_name += f"{side}"
        return [
            apply_alias(
                expr,
                config.output_name_pattern,
                variant,
                default_name,
                prefix=ctx.cache.get("metric_prefix"),
            )
        ]


class TradeChangeAnalytic(AnalyticSpec):
    MODULE = "trade"
    DOCS = [
        AnalyticDoc(
            pattern=r"^PricePoint(?P<scope>AtPrimary|AtVenue)?$",
            template="Mean of PricePoint{scope_or_empty} over trades in the TimeBucket.",
            unit="Probability",
        ),
        AnalyticDoc(
            pattern=r"^(?P<measure>PreTradeElapsedTimeChg|PostTradeElapsedTimeChg)(?P<scope>AtPrimary|AtVenue)?$",
            template="Mean of {measure}{scope_or_empty} over trades in the TimeBucket.",
            unit="Nanoseconds",
        ),
    ]
    ConfigModel = TradeChangeConfig

    def expressions(
        self, ctx: AnalyticContext, config: TradeChangeConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        base_measure = variant["measures"]
        scope = variant["scopes"]
        suffix = ""
        if scope == "Primary":
            suffix = "AtPrimary"
        elif scope == "Venue":
            suffix = "AtVenue"
        col_name = f"{base_measure}{suffix}"

        expr = pl.col(col_name)
        expr = apply_market_state_filter(expr, config.market_states)

        return [
            apply_alias(
                expr.mean(),
                config.output_name_pattern,
                variant,
                col_name,
                prefix=ctx.cache.get("metric_prefix"),
            )
        ]


class TradeImpactAnalytic(AnalyticSpec):
    MODULE = "trade"
    ConfigModel = TradeImpactConfig

    @analytic_expression(
        "EffectiveSpread",
        pattern=r"^(?P<metric>EffectiveSpread|RealizedSpread|PriceImpact)(?P<horizon>.+)$",
        unit="XLOC",
    )
    def _expression_effective_spread(self, config: TradeImpactConfig):
        """Mean {metric} at horizon {horizon} per TimeBucket, using reference price from configuration."""
        price = pl.col("LocalPrice")
        current_mid = pl.col(config.reference_price_col)
        return 2 * (price - current_mid).abs()

    @analytic_expression(
        "RealizedSpread",
        pattern=r"^(?P<metric>EffectiveSpread|RealizedSpread|PriceImpact)(?P<horizon>.+)$",
        unit="XLOC",
    )
    def _expression_realized_spread(self, config: TradeImpactConfig):
        """Mean {metric} at horizon {horizon} per TimeBucket, using reference price from configuration."""
        side_sign = pl.when(pl.col("AggressorSide") == 1).then(1).otherwise(-1)
        price = pl.col("LocalPrice")
        future_mid = pl.col("PostTradeMid")
        return 2 * side_sign * (price - future_mid)

    @analytic_expression(
        "PriceImpact",
        pattern=r"^(?P<metric>EffectiveSpread|RealizedSpread|PriceImpact)(?P<horizon>.+)$",
        unit="XLOC",
    )
    def _expression_price_impact(self, config: TradeImpactConfig):
        """Mean {metric} at horizon {horizon} per TimeBucket, using reference price from configuration."""
        side_sign = pl.when(pl.col("AggressorSide") == 1).then(1).otherwise(-1)
        future_mid = pl.col("PostTradeMid")
        current_mid = pl.col(config.reference_price_col)
        return 2 * side_sign * (future_mid - current_mid)

    def expressions(
        self, ctx: AnalyticContext, config: TradeImpactConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        metric_type = variant["variant"]
        horizon = variant["horizon"]

        expression_fn = self.EXPRESSIONS.get(metric_type)
        if expression_fn is None:
            return []
        expr = expression_fn(self, config)

        expr = apply_market_state_filter(expr, config.market_states)
        default_name = f"{metric_type}{horizon}"
        return [
            apply_alias(
                expr.mean(),
                config.output_name_pattern,
                variant,
                default_name,
                prefix=ctx.cache.get("metric_prefix"),
            )
        ]


class TradeDefaultAnalytic(AnalyticSpec):
    MODULE = "trade"
    DOCS = [
        AnalyticDoc(
            pattern=r"^VWAP$",
            template="Volume-weighted average price over trades in the TimeBucket.",
            unit="XLOC",
        ),
        AnalyticDoc(
            pattern=r"^VolumeWeightedPricePlacement$",
            template="Volume-weighted price placement over trades in the TimeBucket.",
            unit="XLOC",
        ),
        AnalyticDoc(
            pattern=r"^Volume$",
            template="Sum of trade volume over trades in the TimeBucket.",
            unit="Shares",
        ),
        AnalyticDoc(
            pattern=r"^(?P<ohlc>Open|High|Low|Close)$",
            template="{ohlc} trade price over trades in the TimeBucket.",
            unit="XLOC",
            group="TradeOHLCTotal",
            group_role="{ohlc}",
            group_semantics="ohlc_bar,ffill_non_naive",
        ),
    ]
    REGISTER = True

    def expressions(
        self, ctx: AnalyticContext, config: BaseModel, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        return self.default_expressions(ctx)

    def default_expressions(self, ctx: AnalyticContext) -> List[pl.Expr]:
        return [
            (
                (pl.col("LocalPrice") * pl.col("Size")).sum() / pl.col("Size").sum()
            ).alias("VWAP"),
            (
                ((pl.col("PricePoint").clip(0, 1) * 2 - 1) * pl.col("Size")).sum()
                / pl.col("Size").sum()
            ).alias("VolumeWeightedPricePlacement"),
            pl.col("Size").sum().alias("Volume"),
            pl.col("LocalPrice").drop_nans().first().alias("Open"),
            pl.col("LocalPrice").drop_nans().last().alias("Close"),
            pl.col("LocalPrice").drop_nans().max().alias("High"),
            pl.col("LocalPrice").drop_nans().min().alias("Low"),
        ]


# =============================
# Retail Imbalance
# =============================


class RetailImbalanceDoc(AnalyticSpec):
    MODULE = "retail_imbalance"
    DOCS = [
        AnalyticDoc(
            pattern=r"^RetailTradeImbalance$",
            template="Retail trade imbalance as the net retail notional divided by total retail notional in the TimeBucket.",
            unit="Imbalance",
        )
    ]


@register_analytics("retail_imbalance", config_attr="retail_imbalance_analytics")
class RetailImbalanceAnalytics(BaseAnalytics):
    REQUIRES = ["trades"]

    def __init__(self, config: RetailImbalanceConfig):
        self.config = config
        super().__init__("retail_imbalance", {})

    def compute(self) -> pl.LazyFrame:
        if not self.config.ENABLED:
            return pl.DataFrame(
                schema={"ListingId": pl.Int64, "TimeBucket": pl.Datetime("ns")}
            ).lazy()

        gcols = ["MIC", "ListingId", "Ticker", "TimeBucket"]
        df = (
            self.trades.filter(pl.col("Classification") == "LIT_CONTINUOUS")
            .filter(pl.col("BMLLParticipantType") == "RETAIL")
            .group_by(gcols)
            .agg(
                (
                    (
                        pl.when(pl.col("AggressorSide") == 1)
                        .then(pl.col("TradeNotionalEUR"))
                        .otherwise(-pl.col("TradeNotionalEUR"))
                    ).sum()
                    / pl.col("TradeNotionalEUR").sum()
                ).alias("RetailTradeImbalance")
            )
        )
        self.df = df
        return df


# =============================
# Analytics Module
# =============================


@register_analytics("trade", config_attr="trade_analytics")
class TradeAnalytics(BaseAnalytics):
    """
    Computes trade-based analytics for continuous trading segments.
    """

    REQUIRES = ["trades"]

    def __init__(self, config: TradeAnalyticsConfig):
        self.config = config
        super().__init__(
            "trades",
            {},
            metric_prefix=config.metric_prefix,
        )

    def compute(self) -> pl.LazyFrame:
        base_df = self.trades
        if self.config.use_tagged_trades:
            tagged = self.context.get(self.config.tagged_trades_context_key)
            if tagged is not None:
                base_df = tagged

        if isinstance(base_df, pl.DataFrame):
            base_df = base_df.lazy()

        base_df = base_df.filter(pl.col("Classification") == "LIT_CONTINUOUS")
        gcols = ["MIC", "ListingId", "Ticker", "TimeBucket"]

        ctx = AnalyticContext(
            base_df=base_df,
            cache={"metric_prefix": self.metric_prefix},
            context=self.context,
        )
        
        analytic_specs = [
            (TradeGenericAnalytic(), self.config.generic_metrics),
            (TradeDiscrepancyAnalytic(), self.config.discrepancy_metrics),
            (TradeFlagAnalytic(), self.config.flag_metrics),
            (TradeChangeAnalytic(), self.config.change_metrics),
            (TradeImpactAnalytic(), self.config.impact_metrics),
        ]

        expressions = build_expressions(ctx, analytic_specs)

        if not expressions:
            expressions.extend(TradeDefaultAnalytic().default_expressions(ctx))

        expressions.append(pl.col("MarketState").last())

        df = base_df.group_by(gcols).agg(expressions)

        self.df = df
        return df