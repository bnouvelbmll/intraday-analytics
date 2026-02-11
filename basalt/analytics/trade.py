import polars as pl
from basalt.analytics_base import BaseAnalytics
from pydantic import BaseModel, Field, model_validator, ConfigDict
from typing import List, Union, Literal, Optional, Dict, Any

from .common import (
    CombinatorialMetricConfig,
    Side,
    AggregationMethod,
    apply_aggregation,
)
from .utils import apply_market_state_filter, apply_alias
from basalt.analytics_base import (
    AnalyticSpec,
    AnalyticContext,
    AnalyticDoc,
    analytic_expression,
    apply_metric_prefix,
    build_expressions,
)
from basalt.analytics_registry import register_analytics


# =============================
# Configuration Models
# =============================

TradeGenericMeasure = Literal[
    "Volume",
    "Count",
    "Notional",
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
        default="Total",
        description="Filter trades by aggressor side.",
        json_schema_extra={
            "long_doc": "Selects aggressor side(s) for trade metrics.\n"
            "Options: Bid, Ask, Total, Unknown.\n"
            "Each side expands into separate output columns.\n"
            "Used in `TradeGenericAnalytic` to filter AggressorSide.\n"
            "Total aggregates across all sides.\n"
            "Unknown includes trades without a clear aggressor.\n"
            "Combines with measures and aggregations for expansion.\n"
            "Example: sides=['Bid','Ask'] doubles output columns.\n"
            "Output names include side tokens.",
        },
    )

    measures: Union[TradeGenericMeasure, List[TradeGenericMeasure]] = Field(
        ...,
        description="The trade attribute to aggregate.",
        json_schema_extra={
            "long_doc": "Selects which trade attributes to aggregate.\n"
            "Examples: Volume, VWAP, Count, Notional, NotionalEUR.\n"
            "Each measure expands into separate output columns.\n"
            "Used in `TradeGenericAnalytic` expressions.\n"
            "Some measures require specific input columns (NotionalEUR).\n"
            "OHLC expands into multiple columns (Open/High/Low/Close).\n"
            "Large measure lists increase output width.\n"
            "Ensure input schema contains required columns.\n"
            "Output names include measure tokens.",
        },
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
        ...,
        description="Reference price to compare against.",
        json_schema_extra={
            "long_doc": "Selects reference prices for discrepancy metrics.\n"
            "Examples: PreTradeMid, BestBid, BestAskAtPrimary.\n"
            "Each reference expands into separate output columns.\n"
            "Used in `TradeDiscrepancyAnalytic` to compute BPS differences.\n"
            "Requires corresponding reference columns in input.\n"
            "If missing, outputs may be null.\n"
            "Combine with sides and aggregations for expansion.\n"
            "Output names include reference tokens.\n"
            "Discrepancies are in basis points.",
        },
    )
    sides: Union[SideWithTotal, List[SideWithTotal]] = Field(
        default="Total",
        description="Filter trades by aggressor side.",
        json_schema_extra={
            "long_doc": "Selects aggressor side(s) for discrepancy metrics.\n"
            "Options: Bid, Ask, Total, Unknown.\n"
            "Each side expands into separate output columns.\n"
            "Used in `TradeDiscrepancyAnalytic` filtering.\n"
            "Total aggregates across all sides.\n"
            "Combine with references for full expansion.\n"
            "Example: sides=['Bid','Ask'] yields bid and ask discrepancies.\n"
            "Output names include side tokens.\n"
            "Side selection affects sign interpretation.",
        },
    )
    aggregations: List[AggregationMethod] = Field(
        default_factory=lambda: ["Mean"],
        description="Aggregations to apply to discrepancy series.",
        json_schema_extra={
            "long_doc": "Selects aggregation operators for discrepancy series.\n"
            "Default Mean provides average BPS deviation.\n"
            "You can add Min/Max to capture extremes.\n"
            "Each aggregation adds output columns.\n"
            "Used in `TradeDiscrepancyAnalytic`.\n"
            "Be cautious with too many aggregations (wide output).\n"
            "Aggregation names append to output columns.\n"
            "Ensure your downstream expects these names.\n"
            "This overrides the base aggregations list.",
        },
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
        ...,
        description="Trade flag to filter by.",
        json_schema_extra={
            "long_doc": "Selects trade flags to filter trades.\n"
            "Examples: NegotiatedTrade, BlockTrade, IcebergExecution.\n"
            "Each flag expands into separate output columns.\n"
            "Used in `TradeFlagAnalytic` to filter flagged trades.\n"
            "Requires flag columns in the input data.\n"
            "If flag columns are missing, outputs may be empty.\n"
            "Combine with sides and measures for expansion.\n"
            "Output names include flag tokens.\n"
            "Flags are dataset-specific; verify availability.",
        },
    )

    sides: Union[SideWithTotal, List[SideWithTotal]] = Field(
        default="Total",
        description="Filter by aggressor side (applied on top of flag).",
        json_schema_extra={
            "long_doc": "Selects aggressor side(s) after flag filtering.\n"
            "Options: Bid, Ask, Total, Unknown.\n"
            "Each side expands into separate output columns.\n"
            "Applied after flag filter in `TradeFlagAnalytic`.\n"
            "Total aggregates across all sides.\n"
            "Combine with measures for expansion.\n"
            "Output names include side tokens.\n"
            "Side filtering can reduce noisy signals.\n"
            "Requires AggressorSide in input.",
        },
    )

    measures: Union[TradeFlagMeasure, List[TradeFlagMeasure]] = Field(
        ...,
        description="Measure to compute for the flagged trades.",
        json_schema_extra={
            "long_doc": "Selects measures for flagged trades.\n"
            "Options: Volume, Count, AvgNotional.\n"
            "Each measure expands into separate output columns.\n"
            "Used in `TradeFlagAnalytic`.\n"
            "AvgNotional requires trade notional columns.\n"
            "Combine with flags and sides for expansion.\n"
            "Output names include measure tokens.\n"
            "Measures control units (shares, trades, notional).\n"
            "Ensure input includes required columns.",
        },
    )


ImpactMeasure = Literal[
    "PreTradeElapsedTimeChg", "PostTradeElapsedTimeChg", "PricePoint"
]
VenueScope = Literal["Local", "Primary", "Venue"]


class TradeChangeConfig(CombinatorialMetricConfig):
    """
    Analytics related to trade impact or state change.

    Change metrics capture how price-related measures evolve within the
    TimeBucket across different scopes (Local, Primary, Venue). Use this to
    quantify microstructure shifts rather than absolute levels.
    """

    metric_type: Literal["Trade_Change"] = "Trade_Change"

    measures: Union[ImpactMeasure, List[ImpactMeasure]] = Field(
        ...,
        description="Base measure name.",
        json_schema_extra={
            "long_doc": "Selects base measures for change metrics.\n"
            "Options include PreTradeElapsedTimeChg, PostTradeElapsedTimeChg, PricePoint.\n"
            "Each measure expands into separate output columns.\n"
            "Used in `TradeChangeAnalytic`.\n"
            "Requires the corresponding columns in input data.\n"
            "If missing, outputs may be null.\n"
            "Combine with scopes for expansion.\n"
            "Output names include measure tokens.\n"
            "Measures control units (time or price).",
        },
    )

    scopes: Union[VenueScope, List[VenueScope]] = Field(
        default="Local",
        description="Scope of the measure (Local, Primary, Venue).",
        json_schema_extra={
            "long_doc": "Selects scope for change metrics.\n"
            "Local uses venue-local prices; Primary uses primary venue.\n"
            "Venue can use an explicit venue reference if available.\n"
            "Each scope expands into separate output columns.\n"
            "Used in `TradeChangeAnalytic` when selecting columns.\n"
            "Requires corresponding scoped price columns in input.\n"
            "If missing, outputs may be null.\n"
            "Output names include scope tokens.\n"
            "Scope changes interpretation of price movement.",
        },
    )


class TradeImpactConfig(CombinatorialMetricConfig):
    """
    Trade impact and execution quality analytics (TCA).

    Computes common transaction cost analysis metrics such as EffectiveSpread,
    RealizedSpread, and PriceImpact across configurable horizons. The analytics
    compare trade prices to reference prices (pre- and post-trade) to quantify
    execution quality and market impact. The computation proceeds by selecting
    a reference price column, aligning it with each trade, applying horizon
    offsets when needed, and aggregating the resulting per-trade values within
    the TimeBucket.
    """

    metric_type: Literal["Trade_Impact"] = "Trade_Impact"

    variant: Union[
        Literal["EffectiveSpread", "RealizedSpread", "PriceImpact"], List[str]
    ] = Field(
        ...,
        description="TCA analytic type.",
        json_schema_extra={
            "long_doc": "Selects trade impact/TCA metric variants.\n"
            "EffectiveSpread compares trade price to pre-trade reference.\n"
            "RealizedSpread compares to post-trade reference.\n"
            "PriceImpact measures movement in the reference price.\n"
            "Each variant expands into separate output columns.\n"
            "Used in `TradeImpactAnalytic`.\n"
            "Requires reference price columns.\n"
            "Combine with horizon for multiple time windows.\n"
            "Output names include variant tokens.",
        },
    )

    horizon: Union[str, List[str]] = Field(
        default="1s",
        description="Time horizon for post-trade analysis (e.g., '100ms', '1s').",
        json_schema_extra={
            "long_doc": "Selects time horizon(s) for impact metrics.\n"
            "Examples: '100ms', '1s', '5s'.\n"
            "Each horizon expands into separate output columns.\n"
            "Used in `TradeImpactAnalytic` to pick post-trade reference.\n"
            "Short horizons capture immediate impact.\n"
            "Longer horizons capture decay and drift.\n"
            "Requires post-trade reference prices at those horizons.\n"
            "Output names include horizon tokens.\n"
            "Invalid horizons may yield null outputs.",
        },
    )

    reference_price_col: str = Field(
        default="PreTradeMid",
        description="Column name for the reference price (T0 benchmark), e.g., 'PreTradeMid', 'MidPrice'.",
        json_schema_extra={
            "long_doc": "Selects the base reference price column for impact metrics.\n"
            "Common choices: PreTradeMid, MidPrice.\n"
            "Used by `TradeImpactAnalytic` as the T0 benchmark.\n"
            "Must exist in the input trade frame.\n"
            "If missing, impact metrics may be null.\n"
            "Choose a reference aligned with your execution model.\n"
            "Changing this affects sign and magnitude of impact.\n"
            "Ensure reference prices are computed upstream.\n"
            "Output names include reference tokens.",
        },
    )


class RetailImbalanceConfig(BaseModel):
    """
    Retail imbalance configuration.

    Computes imbalance of retail flows using retail participant labels.
    """

    ENABLED: bool = Field(
        True,
        description="Enable retail imbalance analytics.",
        json_schema_extra={
            "long_doc": "Enables or disables retail imbalance metrics.\n"
            "When enabled, computes net retail notional / total retail notional.\n"
            "Uses BMLLParticipantType == 'RETAIL' to identify retail trades.\n"
            "Computed in `RetailImbalanceAnalytics`.\n"
            "Requires TradeNotionalEUR and participant type columns.\n"
            "If inputs are missing, outputs may be empty.\n"
            "Sensitive to classification quality.\n"
            "Useful for flow imbalance analysis.\n"
            "Disable to save compute if you do not use retail signals.",
        },
    )


class TradeAnalyticsConfig(BaseModel):
    """
    Trade analytics configuration.

    Defines which trade metrics are produced: generic aggregates, discrepancy,
    flags, change metrics, impact/TCA, and optional retail imbalance. Each list
    entry expands into multiple metric columns. The trade analytics pipeline
    filters trades to the relevant classifications, applies side and reference
    filters, computes metric expressions, and aggregates within each TimeBucket.
    Many metrics require specific trade fields (e.g., notional, reference
    prices), so configuration should match the available input schema.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "trade",
                "tier": "core",
                "desc": "Core: trade aggregates (OHLC, volume, VWAP, etc.).",
                "outputs": ["Volume", "VWAP", "OHLC", "Notional"],
                "schema_keys": ["trade"],
            }
        }
    )

    ENABLED: bool = True
    metric_prefix: Optional[str] = Field(
        None,
        description="Prefix for trade metric columns.",
        json_schema_extra={
            "long_doc": "Prepended to all trade output column names.\n"
            "Useful to separate trade outputs from other modules.\n"
            "Example: 'TR_' yields TR_TradeVolumeSum.\n"
            "Applies to all trade metrics configured in this pass.\n"
            "Implemented by `BaseAnalytics.metric_prefix`.\n"
            "See `basalt/analytics_base.py` for naming logic.\n"
            "Changing the prefix changes column names and downstream joins.\n"
            "Keep stable across runs for consistent outputs.\n"
            "Leave empty to use module default naming.\n"
        },
    )
    generic_metrics: List[TradeGenericConfig] = Field(
        default_factory=list,
        description="Generic trade metrics configuration.",
        json_schema_extra={
            "long_doc": "Configures standard trade aggregates (volume, VWAP, etc).\n"
            "Each entry expands by side, measure, and aggregations.\n"
            "Example: sides=['Total'], measures=['Volume'] yields TradeTotalVolume.\n"
            "Aggregations can add Sum/Mean/Last variants per series.\n"
            "Computed in `TradeGenericAnalytic`.\n"
            "Use this for baseline trade activity metrics.\n"
            "The number of output columns grows with list sizes.\n"
            "Keep configs minimal for large universes.\n"
            "Input must include trade size and price columns.\n"
        },
    )
    discrepancy_metrics: List[TradeDiscrepancyConfig] = Field(
        default_factory=list,
        description="Trade discrepancy metrics configuration.",
        json_schema_extra={
            "long_doc": "Configures discrepancy metrics relative to reference prices.\n"
            "Examples: deviation from mid or primary best bid/ask.\n"
            "Each entry expands by reference and side.\n"
            "Computed in `TradeDiscrepancyAnalytic`.\n"
            "Requires reference columns (PreTradeMid, BestBidPrice, etc.).\n"
            "If references are missing, metrics may be null.\n"
            "Use to quantify execution quality and price impact.\n"
            "Large configs can be expensive due to multiple references.\n"
            "Output names include reference tokens for clarity.",
        },
    )
    flag_metrics: List[TradeFlagConfig] = Field(
        default_factory=list,
        description="Trade flag metrics configuration.",
        json_schema_extra={
            "long_doc": "Configures flag-based trade aggregates (e.g., negotiated, off-book).\n"
            "Each entry filters trades by a specific flag.\n"
            "Computed in `TradeFlagAnalytic`.\n"
            "Useful for market structure analysis (auction/off-book splits).\n"
            "Requires trade flag columns in the input.\n"
            "If flags are missing, output may be empty.\n"
            "Aggregation applies per TimeBucket.\n"
            "Multiple flags increase output width.\n"
            "Output names include flag identifiers.",
        },
    )
    change_metrics: List[TradeChangeConfig] = Field(
        default_factory=list,
        description="Trade change metrics configuration.",
        json_schema_extra={
            "long_doc": "Configures change metrics on trade series (e.g., deltas).\n"
            "Computes changes over time within the bucket.\n"
            "Used in `TradeChangeAnalytic`.\n"
            "Useful for momentum and regime change analysis.\n"
            "Requires consistent time ordering within each bucket.\n"
            "May be sensitive to sparse trading periods.\n"
            "Aggregation controls how changes are summarized.\n"
            "Keep configs small to reduce output columns.\n"
            "Output names include change method tokens.",
        },
    )
    impact_metrics: List[TradeImpactConfig] = Field(
        default_factory=list,
        description="Trade impact metrics configuration.",
        json_schema_extra={
            "long_doc": "Configures impact/TCA metrics including horizon and reference price.\n"
            "Examples include price impact at 100ms or 1s horizons.\n"
            "Computed in `TradeImpactAnalytic`.\n"
            "Requires post-trade reference price columns.\n"
            "If reference prices are missing, metrics may be null.\n"
            "Horizon controls how far to look ahead from each trade.\n"
            "Longer horizons increase sensitivity to market moves.\n"
            "Use sparingly due to compute cost.\n"
            "Output names include horizon and reference tokens.",
        },
    )
    retail_imbalance: RetailImbalanceConfig = Field(
        default_factory=RetailImbalanceConfig,
        description="Retail imbalance configuration.",
        json_schema_extra={
            "long_doc": "Controls retail imbalance analytics.\n"
            "When enabled, computes net retail notional / total retail notional.\n"
            "Uses BMLLParticipantType == 'RETAIL' to identify retail trades.\n"
            "Computed in `RetailImbalanceAnalytics`.\n"
            "Requires BMLLParticipantType and TradeNotionalEUR columns.\n"
            "If participant type is missing, output will be empty.\n"
            "Useful for flow imbalance analysis.\n"
            "Sensitive to classification quality.\n"
            "Output column is RetailTradeImbalance.",
        },
    )

    # Legacy support
    enable_retail_imbalance: bool = Field(
        False,
        description="Legacy flag to enable retail imbalance.",
        json_schema_extra={
            "long_doc": "If true, forces `retail_imbalance.ENABLED`.\n"
            "Kept for backward compatibility with older configs.\n"
            "Prefer using `retail_imbalance.ENABLED` instead.\n"
            "Both flags can coexist; this one takes precedence.\n"
            "Implementation: `TradeAnalyticsConfig._propagate_retail_imbalance`.\n"
            "No effect if `retail_imbalance` is missing.\n"
            "Will be deprecated in a future release.\n"
            "This flag does not change metric definitions.\n"
            "Only controls enabling behavior.",
        },
    )
    use_tagged_trades: bool = Field(
        False,
        description="Use trades tagged by preprocessing (e.g., iceberg).",
        json_schema_extra={
            "long_doc": "If true, uses context trades from `tagged_trades_context_key`.\n"
            "This allows preprocessing modules (iceberg) to tag trades.\n"
            "Implementation: `TradeAnalytics.compute()` selects tagged frame.\n"
            "Requires preprocessing pass to populate the context.\n"
            "If the key is missing, falls back to raw trades.\n"
            "Useful when you want analytics on iceberg-identified trades.\n"
            "Be careful: tagging may reduce sample size.\n"
            "Use only when preprocessing is enabled.\n"
            "This option changes the input universe of trades.",
        },
    )
    tagged_trades_context_key: str = Field(
        "trades_iceberg",
        description="Context key for tagged trades.",
        json_schema_extra={
            "long_doc": "If `use_tagged_trades` is enabled, this selects the tagged frame.\n"
            "Preprocessing modules must write to this key in the pipeline context.\n"
            "Default key matches iceberg tagging configuration.\n"
            "If you change this, ensure preprocessing uses the same key.\n"
            "Used in `TradeAnalytics.compute()`.\n"
            "If the key is missing, raw trades are used instead.\n"
            "This is a configuration-level link between passes.\n"
            "Changing the key requires coordination across passes.\n"
            "Key names are arbitrary but should be descriptive.",
        },
    )

    @model_validator(mode="after")
    def _propagate_retail_imbalance(self) -> "TradeAnalyticsConfig":
        if self.enable_retail_imbalance and self.retail_imbalance is not None:
            self.retail_imbalance.ENABLED = True
        return self


# =============================
# Analytic Specs
# =============================


class TradeGenericAnalytic(AnalyticSpec):
    MODULE = "trade"
    ConfigModel = TradeGenericConfig

    def _build_filters(self, config: TradeGenericConfig, side: str):
        cond = pl.col("Size").is_not_null()
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
        """
        Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS).

        Why:
            To measure the total number of shares traded.

        Interest:
            Volume is the most fundamental measure of market activity and liquidity.

        Usage:
            Used in almost all market analysis, from VWAP calculations to momentum indicators.
        """
        return self._filtered_zero(cond, pl.col("Size")).sum()

    @analytic_expression(
        "Count",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>Count)$",
        unit="Trades",
    )
    def _expression_count(self, cond):
        """
        Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS).

        Why:
            To count the number of individual trade executions.

        Interest:
            Trade count (frequency) complements volume. High count with low volume suggests retail activity; low count with high volume suggests institutional blocks.

        Usage:
            Used to analyze trading intensity and participant mix.
        """
        return self._filtered_zero(cond, 1).sum()

    @analytic_expression(
        "Notional",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>Notional)$",
        unit="EUR",
    )
    def _expression_notional(self, cond):
        """
        Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS).

        Why:
            To measure the total monetary value traded.

        Interest:
            Allows comparison of activity across instruments with different price levels.

        Usage:
            Used for dollar-volume weighting and liquidity assessment.
        """
        return self._filtered_zero(cond, pl.col("TradeNotionalEUR")).sum()

    @analytic_expression(
        "NotionalEUR",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>NotionalEUR)$",
        unit="EUR",
    )
    def _expression_notional_eur(self, cond):
        """
        Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS).

        Why:
            To measure the total monetary value traded in EUR.

        Interest:
            Standardized currency metric for European markets.

        Usage:
            Used for cross-market comparisons in a common currency.
        """
        return self._filtered_zero(cond, pl.col("TradeNotionalEUR")).sum()

    @analytic_expression(
        "NotionalUSD",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>NotionalUSD)$",
        unit="USD",
    )
    def _expression_notional_usd(self, cond):
        """
        Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS).

        Why:
            To measure the total monetary value traded in USD.

        Interest:
            Standardized currency metric for global comparisons.

        Usage:
            Used for cross-market comparisons in a common currency.
        """
        return self._filtered_zero(cond, pl.col("TradeNotionalUSD")).sum()

    @analytic_expression("RetailCount")
    def _expression_retail_count(self, cond):
        """
        Count of retail trades within the TimeBucket.

        Why:
            To specifically track activity identified as retail.

        Interest:
            Retail flow often has different characteristics (smaller size, less informed) than institutional flow.

        Usage:
            Used to estimate retail participation rates.
        """
        return self._filtered_zero(
            cond,
            pl.when(pl.col("BMLLParticipantType") == "RETAIL").then(1).otherwise(0),
        ).sum()

    @analytic_expression("BlockCount")
    def _expression_block_count(self, cond):
        """
        Count of block trades within the TimeBucket.

        Why:
            To track large-scale institutional executions.

        Interest:
            Block trades represent significant liquidity events and often carry information content.

        Usage:
            Used to identify institutional activity and potential price turning points.
        """
        return self._filtered_zero(
            cond, pl.when(pl.col("IsBlock") == "Y").then(1).otherwise(0)
        ).sum()

    @analytic_expression("AuctionNotional")
    def _expression_auction_notional(self, cond):
        """
        Notional value traded in auctions within the TimeBucket.

        Why:
            To separate continuous trading volume from auction volume.

        Interest:
            Auctions (open/close) are critical liquidity events. Monitoring their size helps understand daily liquidity profiles.

        Usage:
            Used to analyze the "smile" of liquidity distribution throughout the day.
        """
        return self._filtered_zero(
            cond,
            pl.when(pl.col("Classification").str.contains("AUCTION"))
            .then(pl.col("TradeNotionalEUR"))
            .otherwise(0),
        ).sum()

    @analytic_expression("OTCVolume")
    def _expression_otc_volume(self, cond):
        """
        Volume of OTC trades reported within the TimeBucket.

        Why:
            To capture off-book liquidity.

        Interest:
            Significant volume often happens off-exchange. Ignoring OTC misses a large part of the market picture.

        Usage:
            Used to estimate total market size and dark liquidity.
        """
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
        """
        Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS).

        Why:
            To calculate the Volume Weighted Average Price.

        Interest:
            VWAP is the standard benchmark for execution quality. It represents the "fair" price for the volume traded.

        Usage:
            Used to benchmark execution algorithms and assess price trends.
        """
        num = self._filtered_zero(cond, pl.col("LocalPrice") * pl.col("Size")).sum()
        den = self._filtered_zero(cond, pl.col("Size")).sum()
        return num / den

    @analytic_expression(
        "AvgPrice",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>VWAP|AvgPrice|MedianPrice)$",
        unit="XLOC",
    )
    def _expression_avg_price(self, cond):
        """
        Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS).

        Why:
            To calculate the simple average trade price.

        Interest:
            Unlike VWAP, this treats every trade equally regardless of size. Useful for spotting small-trade price deviations.

        Usage:
            Used in comparison with VWAP to detect size-dependent pricing.
        """
        return self._filtered(cond, pl.col("LocalPrice")).mean()

    @analytic_expression(
        "MedianPrice",
        pattern=r"^Trade(?P<side>Total|Bid|Ask)(?P<measure>VWAP|AvgPrice|MedianPrice)$",
        unit="XLOC",
    )
    def _expression_median_price(self, cond):
        """
        Trade {measure} for {side} trades within the TimeBucket (LIT_CONTINUOUS).

        Why:
            To find the median trade price.

        Interest:
            Robust to outliers (extreme prices) compared to the mean.

        Usage:
            Used as a robust central tendency measure for price.
        """
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
        """
        Trade {ohlc} for {side} trades within the TimeBucket (LIT_CONTINUOUS).

        Why:
            To construct standard Open-High-Low-Close bars from trade data.

        Interest:
            The foundation of technical analysis. Captures the price range and direction within the interval.

        Usage:
            Used for charting, volatility calculation, and technical indicators.
        """
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
            description="""
Why:
    To measure how far trade prices deviate from a reference benchmark (like the mid-price or primary market best bid/ask).

Interest:
    Large discrepancies indicate high transaction costs, poor execution quality, or market fragmentation.

Usage:
    Used for Best Execution analysis and to monitor price efficiency across venues.
""",
        )
    ]
    ConfigModel = TradeDiscrepancyConfig

    def expressions(
        self,
        ctx: AnalyticContext,
        config: TradeDiscrepancyConfig,
        variant: Dict[str, Any],
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
        """
        Trades with {flag} filter; {measure} over {side_or_total} trades in the TimeBucket.

        Why:
            To measure the volume of trades that meet specific criteria (e.g., negotiated, odd-lot).

        Interest:
            Different trade types have different market impacts. For example, negotiated trades often have less immediate price impact than auto-matched trades.

        Usage:
            Used to segment market activity by execution mechanism.
        """
        return pl.when(cond).then(pl.col("Size")).otherwise(0).sum()

    @analytic_expression(
        "Count",
        pattern=r"^(?P<flag>NegotiatedTrade|OddLotTrade|BlockTrade|CrossTrade|AlgorithmicTrade|IcebergExecution)(?P<measure>Count)(?P<side>Bid|Ask)?$",
        unit="Trades",
    )
    def _expression_count(self, cond):
        """
        Trades with {flag} filter; {measure} over {side_or_total} trades in the TimeBucket.

        Why:
            To count the number of trades meeting specific criteria.

        Interest:
            High counts of specific trade types (like odd lots) can indicate retail participation or specific algorithmic strategies.

        Usage:
            Used for granular market structure analysis.
        """
        return pl.when(cond).then(1).otherwise(0).sum()

    @analytic_expression(
        "AvgNotional",
        pattern=r"^(?P<flag>NegotiatedTrade|OddLotTrade|BlockTrade|CrossTrade|AlgorithmicTrade|IcebergExecution)(?P<measure>AvgNotional)(?P<side>Bid|Ask)?$",
        unit="EUR",
    )
    def _expression_avg_notional(self, cond):
        """
        Trades with {flag} filter; {measure} over {side_or_total} trades in the TimeBucket.

        Why:
            To calculate the average size (in currency) of specific trade types.

        Interest:
            Helps characterize the participants using these trade types (e.g., large average notional for blocks vs small for odd lots).

        Usage:
            Used to profile market participants and execution channels.
        """
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
            description="""
Why:
    To measure where trades occur relative to the bid-ask spread.

Interest:
    A value of 0 means trading at the bid, 1 means at the ask, and 0.5 means at the mid. Values outside [0, 1] indicate trading through the spread.

Usage:
    Used to assess aggressor behavior and execution quality.
""",
        ),
        AnalyticDoc(
            pattern=r"^(?P<measure>PreTradeElapsedTimeChg|PostTradeElapsedTimeChg)(?P<scope>AtPrimary|AtVenue)?$",
            template="Mean of {measure}{scope_or_empty} over trades in the TimeBucket.",
            unit="Nanoseconds",
            description="""
Why:
    To measure the time elapsed since the last price change before or after a trade.

Interest:
    Short elapsed times suggest high-frequency activity or rapid price discovery. Long times suggest stable prices.

Usage:
    Used to analyze market speed and price stability around trades.
""",
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
        """
        Mean {metric} at horizon {horizon} per TimeBucket, using reference price from configuration.

        Why:
            To measure the cost of trading relative to the "fair" mid-price at the time of trade.

        Interest:
            Effective spread captures the immediate cost paid by the aggressor, including both the quoted spread and any price improvement or impact.

        Usage:
            The primary metric for execution cost analysis (TCA).
        """
        price = pl.col("LocalPrice")
        current_mid = pl.col(config.reference_price_col)
        return 2 * (price - current_mid).abs()

    @analytic_expression(
        "RealizedSpread",
        pattern=r"^(?P<metric>EffectiveSpread|RealizedSpread|PriceImpact)(?P<horizon>.+)$",
        unit="XLOC",
    )
    def _expression_realized_spread(self, config: TradeImpactConfig):
        """
        Mean {metric} at horizon {horizon} per TimeBucket, using reference price from configuration.

        Why:
            To measure the profit of the liquidity provider (market maker) after a certain time horizon.

        Interest:
            Realized spread accounts for adverse selection. If the price moves against the market maker after the trade, the realized spread is lower than the effective spread.

        Usage:
            Used to assess market maker profitability and order flow toxicity.
        """
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
        """
        Mean {metric} at horizon {horizon} per TimeBucket, using reference price from configuration.

        Why:
            To measure how much the price moved in the direction of the trade after execution.

        Interest:
            Price impact reflects the information content of the trade and the market's resilience. High impact means the trade moved the market significantly.

        Usage:
            Used to estimate market depth and the cost of large orders.
        """
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


@register_analytics("retail_imbalance", config_attr="trade_analytics")
class RetailImbalanceAnalytics(BaseAnalytics):
    REQUIRES = ["trades"]

    def __init__(self, config: TradeAnalyticsConfig):
        self.config = config.retail_imbalance
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
