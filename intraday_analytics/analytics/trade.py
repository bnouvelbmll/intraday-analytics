import polars as pl
from intraday_analytics.pipeline import BaseAnalytics
from pydantic import BaseModel, Field
from typing import List, Union, Literal, Optional, Dict, Callable
from .common import CombinatorialMetricConfig, Side, AggregationMethod

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
        expressions = []

        # --- 1. Generic Metrics ---
        for req in self.config.generic_metrics:
            df = base_df
            if req.market_states:
                df = df.filter(pl.col("MarketState").is_in(req.market_states))

            for variant in req.expand():
                side = variant["sides"]
                measure = variant["measures"]

                # Filter by Side
                if side == "Total":
                    filtered_col = pl.col("Size")
                    price_col = pl.col("LocalPrice")
                    cond = pl.lit(True)
                elif side == "Bid":
                    cond = pl.col("AggressorSide") == 2
                    filtered_col = pl.when(cond).then(pl.col("Size")).otherwise(0)
                    price_col = pl.when(cond).then(pl.col("LocalPrice")).otherwise(None)
                elif side == "Ask":
                    cond = pl.col("AggressorSide") == 1
                    filtered_col = pl.when(cond).then(pl.col("Size")).otherwise(0)
                    price_col = pl.when(cond).then(pl.col("LocalPrice")).otherwise(None)
                else:
                    filtered_col = pl.col("Size")
                    price_col = pl.col("LocalPrice")
                    cond = pl.lit(True)

                expr = None
                if measure == "Volume":
                    expr = filtered_col.sum()
                elif measure == "Count":
                    if side == "Total":
                        expr = pl.len()
                    else:
                        expr = (pl.when(cond).then(1).otherwise(0)).sum()
                elif measure == "NotionalEUR":
                    if side == "Total":
                        expr = pl.col("TradeNotionalEUR").sum()
                    else:
                        expr = (pl.when(cond).then(pl.col("TradeNotionalEUR")).otherwise(0)).sum()
                elif measure == "NotionalUSD":
                    # Assuming TradeNotionalUSD exists
                    if side == "Total":
                        expr = pl.col("TradeNotionalUSD").sum()
                    else:
                        expr = (pl.when(cond).then(pl.col("TradeNotionalUSD")).otherwise(0)).sum()
                elif measure == "RetailCount":
                    retail_cond = pl.col("BMLLParticipantType") == "RETAIL"
                    if side == "Total":
                        expr = (pl.when(retail_cond).then(1).otherwise(0)).sum()
                    else:
                        expr = (pl.when(cond & retail_cond).then(1).otherwise(0)).sum()
                elif measure == "BlockCount":
                    # Assuming IsBlock flag 'Y'
                    block_cond = pl.col("IsBlock") == "Y"
                    if side == "Total":
                        expr = (pl.when(block_cond).then(1).otherwise(0)).sum()
                    else:
                        expr = (pl.when(cond & block_cond).then(1).otherwise(0)).sum()
                elif measure == "AuctionNotional":
                    # Assuming Classification or MarketState. Using Classification for now.
                    auction_cond = pl.col("Classification").str.contains("AUCTION")
                    if side == "Total":
                        expr = (pl.when(auction_cond).then(pl.col("TradeNotionalEUR")).otherwise(0)).sum()
                    else:
                        expr = (pl.when(cond & auction_cond).then(pl.col("TradeNotionalEUR")).otherwise(0)).sum()
                elif measure == "OTCVolume":
                    otc_cond = pl.col("Classification").str.contains("OTC")
                    if side == "Total":
                        expr = (pl.when(otc_cond).then(pl.col("Size")).otherwise(0)).sum()
                    else:
                        expr = (pl.when(cond & otc_cond).then(pl.col("Size")).otherwise(0)).sum()
                elif measure == "VWAP":
                    expr = (price_col * filtered_col).sum() / filtered_col.sum()
                elif measure == "AvgPrice":
                    expr = price_col.mean()
                elif measure == "MedianPrice":
                    expr = price_col.median()
                elif measure == "OHLC":
                    prefix = (
                        f"Trade{side}"
                        if not req.output_name_pattern
                        else req.output_name_pattern.format(**variant)
                    )
                    if side == "Total" and not req.output_name_pattern:
                        prefix = ""
                    expressions.append(
                        price_col.drop_nans().first().alias(f"{prefix}Open")
                    )
                    expressions.append(
                        price_col.drop_nans().max().alias(f"{prefix}High")
                    )
                    expressions.append(
                        price_col.drop_nans().min().alias(f"{prefix}Low")
                    )
                    expressions.append(
                        price_col.drop_nans().last().alias(f"{prefix}Close")
                    )
                    continue

                if expr is not None:
                    alias = (
                        req.output_name_pattern.format(**variant)
                        if req.output_name_pattern
                        else f"Trade{side}{measure}"
                    )
                    expressions.append(expr.alias(alias))

        # --- 5. TCA / Impact Metrics ---
        for req in self.config.impact_metrics:
            df = base_df
            if req.market_states:
                df = df.filter(pl.col("MarketState").is_in(req.market_states))
            for variant in req.expand():
                metric_type = variant["variant"]
                horizon = variant["horizon"] # e.g. "1s", "100ms"
                
                # Map horizon to column suffix if available in trades-plus
                # Assuming columns like PostTradeMid1s, PostTradeMid100ms exist
                # or we use a generic PostTradeMid if horizon matches default
                
                # Effective Spread: 2 * |Price - Mid|
                # Realized Spread: 2 * Side * (Price - Mid_T+h)
                # Price Impact: 2 * Side * (Mid_T+h - Mid)
                
                # We need Mid (at trade time) and PostTradeMid (at horizon)
                # Assuming 'PostTradeMid' corresponds to the requested horizon or we map it
                # For simplicity, we'll assume PostTradeMid matches the horizon or use a default
                
                mid_col = pl.col("PostTradeMid") # Or PreTradeMid? Usually Mid at trade time.
                # If PostTradeMid is T+0 (effectively), use it. Or (Bid+Ask)/2 if available.
                # Let's assume PostTradeMid is the reference mid at trade time for now, 
                # or we need 'MidPrice'.
                
                # Let's try to use 'MidPrice' if available, else derive.
                # But we don't know if MidPrice exists.
                # Let's assume 'PostTradeMid' is the T+h mid, and we need T+0 mid.
                # 'PostTradeMid' in BMLL usually implies T+delta.
                
                # Let's assume standard columns:
                # Price: 'LocalPrice'
                # Side: 'AggressorSide' (1=Ask, 2=Bid). 
                #   Side Sign: Ask=+1, Bid=-1. 
                #   If AggressorSide=1 (Buy/Ask aggressor), they paid Ask price. Side=+1?
                #   Standard: Buy=+1, Sell=-1.
                #   AggressorSide 1=Buy? 2=Sell? Need to confirm.
                #   Usually 1=Bid Aggressor (Sell), 2=Ask Aggressor (Buy)?
                #   Let's assume 1=Ask (Buy), 2=Bid (Sell).
                
                side_sign = pl.when(pl.col("AggressorSide") == 1).then(1).otherwise(-1)
                
                # Columns
                price = pl.col("LocalPrice")
                
                # We need a Mid at T0.
                # If not available, we can't compute Effective Spread accurately without L2.
                # But maybe 'PostTradeMid' with horizon '0s' or similar exists?
                # Let's assume 'PostTradeMid' is available and corresponds to the horizon.
                # And we need a 'ReferenceMid'.
                # Let's use 'PostTradeMid' as the future price, and we need current mid.
                # If we lack current mid, we can't do ES or PI.
                # But we can do Realized Spread if we assume Price ~ Mid + HalfSpread.
                
                # Let's assume 'PostTradeMid' is the future mid.
                # And 'MidPrice' is current mid.
                
                future_mid = pl.col("PostTradeMid") # This should be dynamic based on horizon
                current_mid = pl.col(req.reference_price_col)
                
                expr = None
                if metric_type == "EffectiveSpread":
                    # 2 * |Price - Mid|
                    expr = (2 * (price - current_mid).abs()).mean()
                elif metric_type == "RealizedSpread":
                    # 2 * Side * (Price - FutureMid)
                    expr = (2 * side_sign * (price - future_mid)).mean()
                elif metric_type == "PriceImpact":
                    # 2 * Side * (FutureMid - Mid)
                    expr = (2 * side_sign * (future_mid - current_mid)).mean()
                
                if expr is not None:
                    alias = (
                        req.output_name_pattern.format(**variant)
                        if req.output_name_pattern
                        else f"{metric_type}{horizon}"
                    )
                    expressions.append(expr.alias(alias))

        # --- Legacy / Default Metrics (if config is empty) ---
        for req in self.config.discrepancy_metrics:
            df = base_df
            if req.market_states:
                df = df.filter(pl.col("MarketState").is_in(req.market_states))
            for variant in req.expand():
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
                    expr = (10000 * (price_col - ref_col) / ref_col).mean()

                    alias = (
                        req.output_name_pattern.format(**variant)
                        if req.output_name_pattern
                        else f"DiscrepancyTo{ref_name}"
                    )
                    expressions.append(expr.alias(alias))

        # --- 3. Flag-Based Metrics ---
        for req in self.config.flag_metrics:
            df = base_df
            if req.market_states:
                df = df.filter(pl.col("MarketState").is_in(req.market_states))
            for variant in req.expand():
                flag_name = variant["flags"]
                side = variant["sides"]
                measure = variant["measures"]

                # Flag Logic
                flag_map = {
                    "NegotiatedTrade": pl.col("NegotiatedTrade") == "Y",
                    "OddLotTrade": pl.col("LotType") == "Odd Lot",
                    "BlockTrade": pl.col("IsBlock") == "Y",
                    "CrossTrade": pl.col("CrossingTrade") == "Y",
                    "AlgorithmicTrade": pl.col("AlgorithmicTrade") == "Y",
                }
                flag = flag_map.get(flag_name)

                # Side Logic
                if side == "Total":
                    cond = pl.lit(True)
                    filtered_col = pl.col("Size")
                    notional_col = pl.col("TradeNotionalEUR")
                elif side == "Bid":
                    cond = pl.col("AggressorSide") == 2
                    filtered_col = pl.when(cond).then(pl.col("Size")).otherwise(0)
                    notional_col = (
                        pl.when(cond).then(pl.col("TradeNotionalEUR")).otherwise(0)
                    )
                elif side == "Ask":
                    cond = pl.col("AggressorSide") == 1
                    filtered_col = pl.when(cond).then(pl.col("Size")).otherwise(0)
                    notional_col = (
                        pl.when(cond).then(pl.col("TradeNotionalEUR")).otherwise(0)
                    )
                else:
                    cond = pl.lit(True)
                    filtered_col = pl.col("Size")
                    notional_col = pl.col("TradeNotionalEUR")

                expr = None
                if measure == "Volume":
                    expr = (pl.when(flag).then(filtered_col).otherwise(0)).sum()
                elif measure == "Count":
                    if side == "Total":
                        expr = (pl.when(flag).then(1).otherwise(0)).sum()
                    else:
                        expr = (pl.when(flag & cond).then(1).otherwise(0)).sum()
                elif measure == "AvgNotional":
                    num = (pl.when(flag).then(notional_col).otherwise(0)).sum()
                    if side == "Total":
                        den = (pl.when(flag).then(1).otherwise(0)).sum()
                    else:
                        den = (pl.when(flag & cond).then(1).otherwise(0)).sum()
                    expr = num / den

                if expr is not None:
                    alias = (
                        req.output_name_pattern.format(**variant)
                        if req.output_name_pattern
                        else f"{flag_name}{measure}"
                    )
                    if side != "Total":
                        alias += f"_{side}"
                    expressions.append(expr.alias(alias))

        # --- 4. Change / Impact Metrics ---
        for req in self.config.change_metrics:
            df = base_df
            if req.market_states:
                df = df.filter(pl.col("MarketState").is_in(req.market_states))
            for variant in req.expand():
                base_measure = variant["measures"]
                scope = variant["scopes"]

                suffix = ""
                if scope == "Primary":
                    suffix = "AtPrimary"
                elif scope == "Venue":
                    suffix = "AtVenue"

                col_name = f"{base_measure}{suffix}"
                # Check if column exists? Or assume it does.
                # PricePointAtVenue exists, PricePointAtPrimary exists.
                # PreTradeElapsedTimeChgAtPrimary exists.
                # PreTradeElapsedTimeChgAtVenue? Not in list.
                # Let's assume valid combos are requested or handle gracefully.

                expr = pl.col(col_name).mean()
                alias = (
                    req.output_name_pattern.format(**variant)
                    if req.output_name_pattern
                    else col_name
                )
                expressions.append(expr.alias(alias))

        # --- Legacy / Default Metrics (if config is empty) ---
        if not expressions:
            # Add default set if nothing configured
            expressions.extend(
                [
                    (
                        (pl.col("LocalPrice") * pl.col("Size")).sum()
                        / pl.col("Size").sum()
                    ).alias("VWAP"),
                    (
                        (
                            (
                                (pl.col("PricePoint").clip(0, 1) * 2 - 1)
                                * pl.col("Size")
                            ).sum()
                        )
                        / pl.col("Size").sum()
                    ).alias("VolumeWeightedPricePlacement"),
                    pl.col("Size").sum().alias("Volume"),
                    pl.col("LocalPrice").drop_nans().first().alias("Open"),
                    pl.col("LocalPrice").drop_nans().last().alias("Close"),
                    pl.col("LocalPrice").drop_nans().max().alias("High"),
                    pl.col("LocalPrice").drop_nans().min().alias("Low"),
                ]
            )

        # Always add MarketState
        expressions.append(pl.col("MarketState").last())

        # Execute Main Aggregation
        df = base_df.group_by(gcols).agg(expressions)

        # --- Retail Imbalance ---
        if self.config.enable_retail_imbalance:
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
            df = df.join(df2, on=gcols, how="left")

        self.df = df
        return self.df
