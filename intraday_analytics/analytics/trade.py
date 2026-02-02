import polars as pl
from intraday_analytics.pipeline import BaseAnalytics
from pydantic import BaseModel, Field
from typing import List, Union, Literal, Optional, Dict, Callable
from .common import CombinatorialMetricConfig, Side, AggregationMethod
from .utils import apply_market_state_filter, apply_alias, MetricGenerator

# --- Configuration Models ---

# 1. Generic Trade Metrics (Volume, Count, VWAP, OHLC)
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


# 2. Discrepancy Metrics
DiscrepancyReference = Literal[
    "MidAtPrimary",
    "EBBO",
    "BestBid",
    "BestAsk",
    "BestBidAtVenue",
    "BestAskAtVenue",
    "BestBidAtPrimary",
    "BestAskAtPrimary",
]


class TradeDiscrepancyConfig(CombinatorialMetricConfig):
    """
    Price Discrepancy Metrics (in BPS).
    Calculates: 10000 * (TradePrice - ReferencePrice) / ReferencePrice
    """

    metric_type: Literal["Trade_Discrepancy"] = "Trade_Discrepancy"

    references: Union[DiscrepancyReference, List[DiscrepancyReference]] = Field(
        ..., description="Reference price to compare against."
    )


# 3. Flag-Based Metrics (Negotiated, OddLot, etc.)
TradeFlagType = Literal[
    "NegotiatedTrade", "OddLotTrade", "BlockTrade", "CrossTrade", "AlgorithmicTrade"
]
TradeFlagMeasure = Literal["Volume", "Count", "AvgNotional"]


class TradeFlagConfig(CombinatorialMetricConfig):
    """
    Metrics filtered by specific trade flags (e.g., Negotiated, Odd Lot).
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


# 4. Impact / Change Metrics
ImpactMeasure = Literal[
    "PreTradeElapsedTimeChg", "PostTradeElapsedTimeChg", "PricePoint"
]
VenueScope = Literal["Local", "Primary", "Venue"]  # Local=Default/None suffix


class TradeChangeConfig(CombinatorialMetricConfig):
    """
    Metrics related to trade impact or state change.
    """

    metric_type: Literal["Trade_Change"] = "Trade_Change"

    measures: Union[ImpactMeasure, List[ImpactMeasure]] = Field(
        ..., description="Base measure name."
    )

    scopes: Union[VenueScope, List[VenueScope]] = Field(
        default="Local", description="Scope of the measure (Local, Primary, Venue)."
    )


# 5. TCA / Impact (Existing)
class TradeImpactConfig(CombinatorialMetricConfig):
    """
    Trade Impact and Execution Quality metrics (TCA).
    """

    metric_type: Literal["Trade_Impact"] = "Trade_Impact"

    variant: Union[
        Literal["EffectiveSpread", "RealizedSpread", "PriceImpact"], List[str]
    ] = Field(..., description="TCA metric type.")

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
    generic_metrics: List[TradeGenericConfig] = Field(default_factory=list)
    discrepancy_metrics: List[TradeDiscrepancyConfig] = Field(default_factory=list)
    flag_metrics: List[TradeFlagConfig] = Field(default_factory=list)
    change_metrics: List[TradeChangeConfig] = Field(default_factory=list)
    impact_metrics: List[TradeImpactConfig] = Field(default_factory=list)

    # Legacy support
    enable_retail_imbalance: bool = True


class TradeAnalytics(BaseAnalytics):
    """
    Computes trade-based analytics for continuous trading segments.
    """

    REQUIRES = ["trades"]  # (trades_plus)

    def __init__(self, config: TradeAnalyticsConfig):
        self.config = config
        super().__init__(
            "trades",
            {},  # Dynamic schema
        )

    def compute(self) -> pl.LazyFrame:
        # Base filter: LIT_CONTINUOUS is standard
        base_df = self.trades.filter(pl.col("Classification") == "LIT_CONTINUOUS")
        gcols = ["MIC", "ListingId", "Ticker", "TimeBucket"]

        generator = MetricGenerator(base_df)
        expressions = []

        expressions.extend(
            generator.generate(self.config.generic_metrics, self._compute_generic)
        )
        expressions.extend(
            generator.generate(
                self.config.discrepancy_metrics, self._compute_discrepancy
            )
        )
        expressions.extend(
            generator.generate(self.config.flag_metrics, self._compute_flag)
        )
        expressions.extend(
            generator.generate(self.config.change_metrics, self._compute_change)
        )
        expressions.extend(
            generator.generate(self.config.impact_metrics, self._compute_impact)
        )

        # --- Legacy / Default Metrics (if config is empty) ---
        if not expressions:
            expressions.extend(self._get_default_metrics())

        # Always add MarketState
        expressions.append(pl.col("MarketState").last())

        # Execute Main Aggregation
        df = base_df.group_by(gcols).agg(expressions)

        # --- Retail Imbalance ---
        if self.config.enable_retail_imbalance:
            df = self._compute_retail_imbalance(df, gcols)

        self.df = df
        return self.df

    def _compute_generic(self, req, variant):
        side = variant["sides"]
        measure = variant["measures"]

        # Filter by Side
        cond = pl.lit(True)
        if side == "Bid":
            cond = pl.col("AggressorSide") == 2
        elif side == "Ask":
            cond = pl.col("AggressorSide") == 1

        # Apply MarketState filter to condition if needed
        if req.market_states:
            cond = cond & pl.col("MarketState").is_in(req.market_states)

        # Helper to apply condition
        def filtered(col):
            return (
                pl.when(cond).then(col).otherwise(None)
            )  # Use None for aggregations that ignore nulls like mean/median

        def filtered_zero(col):
            return pl.when(cond).then(col).otherwise(0)

        expr = None
        if measure == "Volume":
            expr = filtered_zero(pl.col("Size")).sum()
        elif measure == "Count":
            expr = filtered_zero(1).sum()
        elif measure == "NotionalEUR":
            expr = filtered_zero(pl.col("TradeNotionalEUR")).sum()
        elif measure == "NotionalUSD":
            expr = filtered_zero(pl.col("TradeNotionalUSD")).sum()
        elif measure == "RetailCount":
            expr = filtered_zero(
                pl.when(pl.col("BMLLParticipantType") == "RETAIL").then(1).otherwise(0)
            ).sum()
        elif measure == "BlockCount":
            expr = filtered_zero(
                pl.when(pl.col("IsBlock") == "Y").then(1).otherwise(0)
            ).sum()
        elif measure == "AuctionNotional":
            expr = filtered_zero(
                pl.when(pl.col("Classification").str.contains("AUCTION"))
                .then(pl.col("TradeNotionalEUR"))
                .otherwise(0)
            ).sum()
        elif measure == "OTCVolume":
            expr = filtered_zero(
                pl.when(pl.col("Classification").str.contains("OTC"))
                .then(pl.col("Size"))
                .otherwise(0)
            ).sum()
        elif measure == "VWAP":
            # VWAP = Sum(Price * Size) / Sum(Size)
            # We need to filter both numerator and denominator
            # Note: This simple aggregation assumes we can do it in one go.
            # But VWAP is (Sum(P*V) / Sum(V)).
            # If we return a single expression, it must be the result.
            num = filtered_zero(pl.col("LocalPrice") * pl.col("Size")).sum()
            den = filtered_zero(pl.col("Size")).sum()
            expr = num / den
        elif measure == "AvgPrice":
            expr = filtered(pl.col("LocalPrice")).mean()
        elif measure == "MedianPrice":
            expr = filtered(pl.col("LocalPrice")).median()
        elif measure == "OHLC":
            # OHLC returns a list of expressions, but generator expects one.
            # We handle this special case by returning a struct or we need to change generator.
            # Or we can just return None here and handle OHLC separately?
            # Or better, we can return a list of expressions?
            # The generator currently expects a single expression.
            # Let's hack it: return a list, and update generator to handle lists.
            prefix = (
                f"Trade{side}"
                if not req.output_name_pattern
                else req.output_name_pattern.format(**variant)
            )
            if side == "Total" and not req.output_name_pattern:
                prefix = ""

            price_col = filtered(pl.col("LocalPrice")).drop_nans()
            return [
                price_col.first().alias(f"{prefix}Open"),
                price_col.max().alias(f"{prefix}High"),
                price_col.min().alias(f"{prefix}Low"),
                price_col.last().alias(f"{prefix}Close"),
            ]

        if expr is not None:
            default_name = f"Trade{side}{measure}"
            return apply_alias(expr, req.output_name_pattern, variant, default_name)
        return None

    def _compute_discrepancy(self, req, variant):
        ref_name = variant["references"]
        ref_map = {
            "MidAtPrimary": "PostTradeMidAtPrimary",
            "EBBO": "PostTradeMid",
            "BestBid": "BestBidPrice",
            "BestAsk": "BestAskPrice",
            "BestBidAtVenue": "BestBidPriceAtVenue",
            "BestAskAtVenue": "BestAskPriceAtVenue",
            "BestBidAtPrimary": "BestBidPriceAtPrimary",
            "BestAskAtPrimary": "BestAskPriceAtPrimary",
        }
        col_name = ref_map.get(ref_name)
        if col_name:
            price_col = pl.col("LocalPrice")
            ref_col = pl.col(col_name)
            expr = 10000 * (price_col - ref_col) / ref_col

            expr = apply_market_state_filter(expr, req.market_states)

            default_name = f"DiscrepancyTo{ref_name}"
            return apply_alias(
                expr.mean(), req.output_name_pattern, variant, default_name
            )
        return None

    def _compute_flag(self, req, variant):
        flag_name = variant["flags"]
        side = variant["sides"]
        measure = variant["measures"]

        flag_map = {
            "NegotiatedTrade": pl.col("NegotiatedTrade") == "Y",
            "OddLotTrade": pl.col("LotType") == "Odd Lot",
            "BlockTrade": pl.col("IsBlock") == "Y",
            "CrossTrade": pl.col("CrossingTrade") == "Y",
            "AlgorithmicTrade": pl.col("AlgorithmicTrade") == "Y",
        }
        flag = flag_map.get(flag_name)

        cond = flag
        if side == "Bid":
            cond = cond & (pl.col("AggressorSide") == 2)
        elif side == "Ask":
            cond = cond & (pl.col("AggressorSide") == 1)

        if req.market_states:
            cond = cond & pl.col("MarketState").is_in(req.market_states)

        def filtered_zero(col):
            return pl.when(cond).then(col).otherwise(0)

        expr = None
        if measure == "Volume":
            expr = filtered_zero(pl.col("Size")).sum()
        elif measure == "Count":
            expr = filtered_zero(1).sum()
        elif measure == "AvgNotional":
            num = filtered_zero(pl.col("TradeNotionalEUR")).sum()
            den = filtered_zero(1).sum()
            expr = num / den

        if expr is not None:
            default_name = f"{flag_name}{measure}"
            if side != "Total":
                default_name += f"_{side}"
            return apply_alias(expr, req.output_name_pattern, variant, default_name)
        return None

    def _compute_change(self, req, variant):
        base_measure = variant["measures"]
        scope = variant["scopes"]
        suffix = ""
        if scope == "Primary":
            suffix = "AtPrimary"
        elif scope == "Venue":
            suffix = "AtVenue"
        col_name = f"{base_measure}{suffix}"

        expr = pl.col(col_name)
        expr = apply_market_state_filter(expr, req.market_states)

        return apply_alias(expr.mean(), req.output_name_pattern, variant, col_name)

    def _compute_impact(self, req, variant):
        metric_type = variant["variant"]
        horizon = variant["horizon"]

        side_sign = pl.when(pl.col("AggressorSide") == 1).then(1).otherwise(-1)
        price = pl.col("LocalPrice")
        future_mid = pl.col("PostTradeMid")
        current_mid = pl.col(req.reference_price_col)

        expr = None
        if metric_type == "EffectiveSpread":
            expr = 2 * (price - current_mid).abs()
        elif metric_type == "RealizedSpread":
            expr = 2 * side_sign * (price - future_mid)
        elif metric_type == "PriceImpact":
            expr = 2 * side_sign * (future_mid - current_mid)

        if expr is not None:
            expr = apply_market_state_filter(expr, req.market_states)
            default_name = f"{metric_type}{horizon}"
            return apply_alias(
                expr.mean(), req.output_name_pattern, variant, default_name
            )
        return None

    def _get_default_metrics(self):
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

    def _compute_retail_imbalance(self, df, gcols):
        df2 = (
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
        return df.join(df2, on=gcols, how="left")
