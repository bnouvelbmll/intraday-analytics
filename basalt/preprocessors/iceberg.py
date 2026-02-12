import polars as pl
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Union, Literal, Dict, Any, Optional

from basalt.analytics_base import BaseAnalytics
from basalt.analytics_base import (
    CombinatorialMetricConfig,
    AnalyticSpec,
    AnalyticContext,
    AnalyticDoc,
    analytic_expression,
    build_expressions,
)
from basalt.analytics_registry import register_analytics
from basalt.analytics.utils import apply_alias


IcebergMeasure = Literal[
    "ExecutionVolume",
    "ExecutionCount",
    "AverageSize",
    "RevealedPeakCount",
    "AveragePeakCount",
    "OrderImbalance",
]
IcebergSide = Literal["Bid", "Ask", "Total"]


class IcebergMetricConfig(CombinatorialMetricConfig):
    """
    Iceberg Execution Analytics (Volumes, Counts, Average Size, Imbalance).
    """

    metric_type: Literal["Iceberg"] = Field(
        "Iceberg",
        description="Internal metric family identifier for iceberg analytics.",
    )

    measures: Union[IcebergMeasure, List[IcebergMeasure]] = Field(
        ...,
        description="Iceberg measure to compute.",
        json_schema_extra={
            "long_doc": "Selects iceberg measures to compute.\n"
            "Examples: ExecutionVolume, ExecutionCount, AverageSize.\n"
            "Each measure expands into separate output columns.\n"
            "Used in `IcebergExecutionAnalytic` expressions.\n"
            "Some measures require additional iceberg linking stages.\n"
            "Combine with sides for expansion.\n"
            "Large measure lists increase output width.\n"
            "Ensure required fields exist in the input.\n"
            "Output names include measure tokens.",
        },
    )
    sides: Union[IcebergSide, List[IcebergSide]] = Field(
        default="Total",
        description="Side filter (Bid, Ask, or Total).",
        json_schema_extra={
            "long_doc": "Selects side(s) for iceberg measures.\n"
            "Options: Bid, Ask, Total.\n"
            "Each side expands into separate output columns.\n"
            "Used in `IcebergExecutionAnalytic` filtering.\n"
            "Total aggregates both sides.\n"
            "Combine with measures for expansion.\n"
            "Bid/Ask are useful for directional analysis.\n"
            "Output names include side tokens.\n"
            "Side selection affects interpretation of imbalance.",
        },
    )


class IcebergAnalyticsConfig(BaseModel):
    """
    Iceberg analytics configuration.

    Controls iceberg detection, matching, and metrics. Includes trade tagging
    and peak linking options for reconstructing iceberg events. The iceberg
    pipeline identifies candidate iceberg sequences from L3 data, optionally
    links peaks, matches trades to iceberg events when enabled, and aggregates
    resulting metrics within TimeBuckets. Configuration should be tuned to the
    timing and size characteristics of the venue to avoid over- or under-matching.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "iceberg",
                "tier": "pre",
                "desc": "Preprocessing: detect iceberg executions; run before trade analytics.",
                "outputs": ["IcebergExecution"],
                "schema_keys": ["iceberg"],
            }
        }
    )

    ENABLED: bool = Field(
        True,
        description="Enable or disable iceberg analytics for this pass.",
    )
    metric_prefix: Optional[str] = Field(
        None,
        description="Prefix for iceberg metric columns.",
        json_schema_extra={
            "long_doc": "Prepended to all iceberg output columns.\n"
            "Useful to separate iceberg metrics from other modules.\n"
            "Example: 'IC_' yields IC_IcebergExecutionVolume.\n"
            "Applies to all iceberg metrics in this pass.\n"
            "Implemented by `BaseAnalytics.metric_prefix`.\n"
            "See `basalt/analytics_base.py` for naming rules.\n"
            "Changing the prefix changes column names and downstream joins.\n"
            "Keep stable for production outputs.\n"
            "Leave empty to use default module naming.\n"
        },
    )
    metrics: List[IcebergMetricConfig] = Field(
        default_factory=list,
        description="Iceberg execution metrics configuration.",
        json_schema_extra={
            "long_doc": "Defines which iceberg execution measures to compute.\n"
            "Each entry expands by side and measure.\n"
            "Example: measures=['ExecutionVolume'], sides=['Total'].\n"
            "Computed in `IcebergExecutionAnalytic`.\n"
            "Requires iceberg detection to identify relevant executions.\n"
            "Aggregation is per TimeBucket.\n"
            "Many measures will increase output width.\n"
            "Use carefully for large universes.\n"
            "Output names include iceberg measure tokens.",
        },
    )

    tag_trades: bool = Field(
        True,
        description="Tag trades using detected iceberg events.",
        json_schema_extra={
            "long_doc": "If true, iceberg detection tags trades and stores them in the context.\n"
            "Tagged trades are used by downstream modules (e.g., trade analytics).\n"
            "This enables metrics on iceberg-identified trades.\n"
            "Requires the trade match stage to be enabled.\n"
            "If disabled, downstream modules cannot access tagged trades.\n"
            "Implementation: `IcebergAnalytics` writes to context.\n"
            "Works best when trade data is available and aligned.\n"
            "Tagging may reduce sample size significantly.\n"
            "Use with `tagged_trades_context_key`.",
        },
    )
    trade_tag_context_key: str = Field(
        "trades_iceberg",
        description="Context key for tagged trades.",
        json_schema_extra={
            "long_doc": "Key used to store tagged trades in the pipeline context.\n"
            "Downstream modules read from this key when `use_tagged_trades` is enabled.\n"
            "Must match the value in trade analytics config.\n"
            "Changing the key requires coordination across passes.\n"
            "If the key is missing, tagged trades are not found.\n"
            "Default matches the iceberg module name.\n"
            "Useful when multiple taggers are present.\n"
            "Keep keys descriptive and stable.\n"
            "This is a config-level contract between passes.",
        },
    )
    match_tolerance_seconds: float = Field(
        5e-4,
        gt=0,
        description="Time tolerance for matching trades to iceberg events.",
        json_schema_extra={
            "long_doc": "Time window for matching trades to iceberg events.\n"
            "Example: 5e-4 = 0.5ms tolerance.\n"
            "Smaller tolerances reduce matches and false positives.\n"
            "Larger tolerances increase matches but may add noise.\n"
            "Used by the iceberg trade matching stage.\n"
            "Requires precise timestamps in both trades and L3 data.\n"
            "If clocks are skewed, matches may degrade.\n"
            "Consider venue latency when choosing this value.\n"
            "Affects tagged trade counts and downstream analytics.",
        },
    )
    price_match: bool = Field(
        True,
        description="Require price match when linking iceberg trades.",
        json_schema_extra={
            "long_doc": "If true, requires price equality between trade and iceberg candidate.\n"
            "If false, matches on timing alone (less strict).\n"
            "Price matching reduces false positives.\n"
            "However, it may miss cases with price rounding.\n"
            "Used in trade matching logic within iceberg analytics.\n"
            "Disable only if price quality is known to be noisy.\n"
            "Consider combining with size tolerance adjustments.\n"
            "Changing this affects tagged trade counts.\n"
            "This does not affect iceberg metric definitions.",
        },
    )
    min_revealed_qty: float = Field(
        0.0,
        ge=0.0,
        description="Minimum revealed quantity to qualify as iceberg.",
        json_schema_extra={
            "long_doc": "Minimum revealed quantity to classify a candidate as iceberg.\n"
            "Higher thresholds reduce noise and false positives.\n"
            "Lower thresholds increase sensitivity to small icebergs.\n"
            "Applied before generating iceberg metrics.\n"
            "Useful for filtering micro trades and small events.\n"
            "If set too high, you may miss legitimate events.\n"
            "This affects both tagging and metrics.\n"
            "Consider instrument liquidity when choosing values.\n"
            "Units are in shares.",
        },
    )
    avg_size_source: Literal["RevealedQty", "ExecutionSize", "OldSize"] = Field(
        "RevealedQty",
        description="Source used for average size calculations.",
        json_schema_extra={
            "long_doc": "Controls which field contributes to iceberg average size metrics.\n"
            "RevealedQty uses displayed quantity; ExecutionSize uses trade sizes.\n"
            "OldSize can be used for L3 update-based sizing.\n"
            "Applied in iceberg metric expressions.\n"
            "Choose based on your data quality and definitions.\n"
            "Changing this affects AverageSize and related outputs.\n"
            "Ensure the chosen field exists in the L3 schema.\n"
            "If missing, results may be null.\n"
            "Use ExecutionSize for execution-focused metrics.",
        },
    )
    imbalance_source: Literal["ExecutionSize", "RevealedQty"] = Field(
        "ExecutionSize",
        description="Source used for iceberg imbalance metrics.",
        json_schema_extra={
            "long_doc": "Controls the source used for iceberg imbalance metrics.\n"
            "ExecutionSize focuses on executed volume imbalance.\n"
            "RevealedQty focuses on displayed quantity imbalance.\n"
            "Applied in iceberg imbalance expressions.\n"
            "Choose based on whether you care about trading vs display.\n"
            "Ensure the chosen field exists in the input schema.\n"
            "Changing this affects iceberg imbalance outputs.\n"
            "RevealedQty can be noisier for hidden liquidity.\n"
            "Outputs are normalized imbalance values.",
        },
    )
    trade_match_enabled: bool = Field(
        True,
        description="Enable trade matching against iceberg candidates.",
        json_schema_extra={
            "long_doc": "If false, iceberg detection skips trade matching stage.\n"
            "Disabling may speed up processing but removes tagged trades.\n"
            "Iceberg metrics that rely on trade linking may be affected.\n"
            "Use when you only need iceberg event counts.\n"
            "If disabled, `tag_trades` has no effect.\n"
            "Useful for quick scans or when trade data is missing.\n"
            "Consider enabling for production analytics.\n"
            "This affects downstream trade analytics and tagging.\n"
            "Does not change the iceberg detection stage.",
        },
    )
    trade_match_tolerance_seconds: float = Field(
        5e-4,
        gt=0,
        description="Trade matching time tolerance.",
        json_schema_extra={
            "long_doc": "Used when aligning trades to iceberg events.\n"
            "Defines acceptable time window for a trade to match an iceberg event.\n"
            "Smaller values are stricter; larger values increase matches.\n"
            "Requires accurate timestamps.\n"
            "This is separate from `match_tolerance_seconds`.\n"
            "Applied in the trade matching stage only.\n"
            "Affects tagging and any trade-linked iceberg metrics.\n"
            "Use venue latency as a guide for selection.\n"
            "Large values may introduce false positives.",
        },
    )
    trade_match_price_match: bool = Field(
        True,
        description="Require price match for trade linking.",
        json_schema_extra={
            "long_doc": "When false, allows mismatched prices if within time tolerance.\n"
            "Price matching reduces false positives at the cost of fewer matches.\n"
            "If your data has rounding differences, consider disabling.\n"
            "Applied during trade matching stage.\n"
            "Affects tagging and trade-linked metrics.\n"
            "Does not affect iceberg detection itself.\n"
            "Combine with size tolerance if disabling price match.\n"
            "Set false only with evidence of price mismatch issues.\n"
            "May increase noise in tagged trades.",
        },
    )
    trade_match_exclude_exact: bool = Field(
        True,
        description="Exclude exact matches when linking iceberg trades.",
        json_schema_extra={
            "long_doc": "If true, avoids linking when trade exactly matches a candidate.\n"
            "Useful when exact matches represent trivial or duplicated events.\n"
            "Prevents over-counting in iceberg linkage.\n"
            "Disable if you want strict exact matching.\n"
            "Applied in trade matching stage.\n"
            "Can reduce tagged trade counts.\n"
            "Consider data quality before enabling.\n"
            "No effect on iceberg detection stage.\n"
            "Only affects trade linking.",
        },
    )
    trade_match_size_tolerance: float = Field(
        0.0,
        ge=0.0,
        description="Allowed size tolerance for trade matching.",
        json_schema_extra={
            "long_doc": "Higher tolerances allow looser size matching to iceberg candidates.\n"
            "Use when size fields are noisy or rounded.\n"
            "Set to 0 for exact size matching.\n"
            "Applied during trade matching stage.\n"
            "Larger tolerances increase match rate but may add noise.\n"
            "Consider using with price matching enabled.\n"
            "Units are in shares (same as size columns).\n"
            "Impacts tagging and trade-linked metrics.\n"
            "Does not affect iceberg detection.",
        },
    )
    trade_match_trade_types: List[str] = Field(
        default_factory=lambda: ["LIT", "DARK"],
        description="Trade types eligible for iceberg matching.",
        json_schema_extra={
            "long_doc": "Trade types considered when linking iceberg events to trades.\n"
            "Example: ['LIT'] to restrict to lit trades only.\n"
            "Uses BMLL trade type classifications.\n"
            "Applied as a filter before matching.\n"
            "Restricting types can reduce noise.\n"
            "Ensure trade type values match your dataset.\n"
            "If types are missing, matches may be empty.\n"
            "Affects tagging and trade-linked metrics.\n"
            "Does not affect iceberg detection stage.",
        },
    )
    trade_match_classifications: List[str] = Field(
        default_factory=list,
        description="Trade classifications eligible for iceberg matching.",
        json_schema_extra={
            "long_doc": "Filter on BMLL classification codes.\n"
            "Allows restricting matches to specific classification labels.\n"
            "If empty, all classifications are allowed.\n"
            "Useful for excluding auctions or special trades.\n"
            "Applied before matching stage.\n"
            "Requires classification columns in trades data.\n"
            "If missing, this filter is ignored.\n"
            "Affects tagging and trade-linked metrics.\n"
            "Does not affect iceberg detection.",
        },
    )
    trade_match_require_side: bool = Field(
        True,
        description="Require aggressor side for trade matching.",
        json_schema_extra={
            "long_doc": "If true, mismatched or missing side will prevent linkage.\n"
            "Ensures that trade side matches inferred iceberg side.\n"
            "If aggressor side is missing, match may be dropped.\n"
            "Useful for reducing false positives.\n"
            "If your data lacks side, consider disabling.\n"
            "Applied in the matching stage.\n"
            "Affects tagged trade counts.\n"
            "Does not affect iceberg detection.\n"
            "Only affects trade linkage.",
        },
    )
    enable_peak_linking: bool = Field(
        True,
        description="Enable peak linking for iceberg events.",
        json_schema_extra={
            "long_doc": "Links sequential peaks to improve iceberg event reconstruction.\n"
            "Helps group related peaks into a single iceberg event.\n"
            "Useful for long-running iceberg orders with multiple peaks.\n"
            "If disabled, each peak is treated independently.\n"
            "Applied after iceberg detection stage.\n"
            "May increase compute cost for large datasets.\n"
            "Affects iceberg metrics that depend on peak counts.\n"
            "Disable for simpler event counts.\n"
            "Requires stable timestamps for linking.",
        },
    )
    peak_link_tolerance_seconds: float = Field(
        5e-4,
        gt=0,
        description="Time tolerance for peak linking.",
        json_schema_extra={
            "long_doc": "Controls how close peaks must be to link.\n"
            "Smaller tolerances produce fewer links.\n"
            "Larger tolerances merge more peaks into a single iceberg.\n"
            "Used only if `enable_peak_linking` is true.\n"
            "Requires accurate timing in L3 data.\n"
            "Affects peak-count related metrics.\n"
            "Consider venue latency when choosing this value.\n"
            "Large values may over-merge unrelated peaks.\n"
            "Changing this affects iceberg counts.",
        },
    )
    peak_link_price_match: bool = Field(
        True,
        description="Require price match for peak linking.",
        json_schema_extra={
            "long_doc": "If false, allows peak linking even with price differences.\n"
            "Price matching reduces false linking across price levels.\n"
            "Disable only if price changes within an iceberg are expected.\n"
            "Used only when `enable_peak_linking` is true.\n"
            "Affects reconstructed iceberg events and peak counts.\n"
            "If price data is noisy, consider disabling.\n"
            "Changing this affects iceberg metrics involving peaks.\n"
            "Does not affect trade matching stage.\n"
            "Only applies to peak linking.",
        },
    )


def _duration_str(seconds: float) -> str:
    ns = max(1, int(seconds * 1e9))
    return f"{ns}ns"


class IcebergExecutionAnalytic(AnalyticSpec):
    MODULE = "iceberg"
    ConfigModel = IcebergMetricConfig
    DOCS = [
        AnalyticDoc(
            pattern=r"^IcebergExecutionVolume(?P<side>Bid|Ask)?$",
            template="Sum of iceberg execution volume for {side_or_total} within the TimeBucket.",
            unit="Shares",
        ),
        AnalyticDoc(
            pattern=r"^IcebergExecutionCount(?P<side>Bid|Ask)?$",
            template="Count of iceberg executions for {side_or_total} within the TimeBucket.",
            unit="Trades",
        ),
        AnalyticDoc(
            pattern=r"^IcebergAverageSize(?P<side>Bid|Ask)?$",
            template="Mean iceberg revealed size for {side_or_total} within the TimeBucket.",
            unit="Shares",
        ),
        AnalyticDoc(
            pattern=r"^IcebergRevealedPeakCount(?P<side>Bid|Ask)?$",
            template="Count of revealed iceberg peaks for {side_or_total} within the TimeBucket.",
            unit="Peaks",
        ),
        AnalyticDoc(
            pattern=r"^IcebergAveragePeakCount(?P<side>Bid|Ask)?$",
            template="Average cumulative revealed peak count per active iceberg for {side_or_total} within the TimeBucket.",
            unit="Peaks",
        ),
        AnalyticDoc(
            pattern=r"^IcebergOrderImbalance$",
            template="Normalized imbalance of iceberg execution volume between bid and ask within the TimeBucket.",
            unit="Imbalance",
        ),
    ]

    @analytic_expression(
        "ExecutionVolume",
        pattern=r"^IcebergExecutionVolume(?P<side>Bid|Ask)?$",
        unit="Shares",
    )
    def _expression_exec_volume(self, cond):
        """Sum of iceberg execution volume for {side_or_total} within the TimeBucket."""
        return pl.when(cond).then(pl.col("ExecutionSize")).otherwise(0).sum()

    @analytic_expression(
        "ExecutionCount",
        pattern=r"^IcebergExecutionCount(?P<side>Bid|Ask)?$",
        unit="Trades",
    )
    def _expression_exec_count(self, cond):
        """Count of iceberg executions for {side_or_total} within the TimeBucket."""
        return pl.when(cond).then(1).otherwise(0).sum()

    @analytic_expression(
        "AverageSize",
        pattern=r"^IcebergAverageSize(?P<side>Bid|Ask)?$",
        unit="Shares",
    )
    def _expression_avg_size(self, cond, size_col: str):
        """Mean iceberg revealed size for {side_or_total} within the TimeBucket."""
        return pl.when(cond).then(pl.col(size_col)).otherwise(None).mean()

    @analytic_expression(
        "RevealedPeakCount",
        pattern=r"^IcebergRevealedPeakCount(?P<side>Bid|Ask)?$",
        unit="Peaks",
    )
    def _expression_peak_count(self, cond):
        """Count of revealed iceberg peaks for {side_or_total} within the TimeBucket."""
        return pl.when(cond).then(1).otherwise(0).sum()

    @analytic_expression(
        "AveragePeakCount",
        pattern=r"^IcebergAveragePeakCount(?P<side>Bid|Ask)?$",
        unit="Peaks",
    )
    def _expression_avg_peak_count(self, cond):
        """Average cumulative revealed peak count per active iceberg for {side_or_total} within the TimeBucket."""
        return pl.when(cond).then(pl.col("IcebergEventCount")).otherwise(None).mean()

    @analytic_expression(
        "OrderImbalance",
        pattern=r"^IcebergOrderImbalance$",
        unit="Imbalance",
    )
    def _expression_imbalance(self, source_col: str):
        """Normalized imbalance of iceberg execution volume between bid and ask within the TimeBucket."""
        bid = pl.when(pl.col("Side") == 1).then(pl.col(source_col)).otherwise(0).sum()
        ask = pl.when(pl.col("Side") == 2).then(pl.col(source_col)).otherwise(0).sum()
        return (bid - ask) / (bid + ask)

    def expressions(
        self, ctx: AnalyticContext, config: IcebergMetricConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        measure = variant["measures"]
        side = variant["sides"]

        if measure == "OrderImbalance" and side != "Total":
            return []

        cond = pl.lit(True)
        if side == "Bid":
            cond = pl.col("Side") == 1
        elif side == "Ask":
            cond = pl.col("Side") == 2

        expression_fn = self.EXPRESSIONS.get(measure)
        if expression_fn is None:
            return []

        if measure == "AverageSize":
            base_expr = expression_fn(self, cond, ctx.cache["avg_size_col"])
        elif measure == "AveragePeakCount":
            return []
        elif measure == "OrderImbalance":
            base_expr = expression_fn(self, ctx.cache["imbalance_col"])
        else:
            base_expr = expression_fn(self, cond)

        default_name = f"Iceberg{measure}"
        if measure != "OrderImbalance" and side != "Total":
            default_name = f"{default_name}{side}"
        return [
            apply_alias(
                base_expr,
                config.output_name_pattern,
                variant,
                default_name,
                prefix=ctx.cache.get("metric_prefix"),
            )
        ]


@register_analytics("iceberg", config_attr="iceberg_analytics")
class IcebergAnalytics(BaseAnalytics):
    """
    Computes iceberg execution analytics and optionally tags trades with iceberg executions.
    """

    REQUIRES = ["l3", "trades"]

    def __init__(self, config: IcebergAnalyticsConfig):
        self.config = config
        super().__init__("iceberg", {}, metric_prefix=config.metric_prefix)

    def _iceberg_execution_df(self) -> pl.LazyFrame:
        base = self._l3_execution_df()
        return base.with_columns(
            RevealedQty=(pl.col("ExecutionSize") - pl.col("OldSize")).clip(0)
        ).filter(pl.col("RevealedQty") > self.config.min_revealed_qty)

    def _l3_execution_df(self) -> pl.LazyFrame:
        base = self.l3.filter(pl.col("LobAction") == 3).filter(
            pl.col("ExecutionSize") > 0
        )
        return base.select(
            [
                "ListingId",
                "TimeBucket",
                "EventTimestamp",
                "Side",
                "ExecutionSize",
                "ExecutionPrice",
                "OldSize",
                "OldPrice",
                "OrderID",
            ]
        )

    def _infer_trade_side(self, trades: pl.LazyFrame) -> pl.LazyFrame:
        trade_cols = trades.collect_schema().names()
        if "AggressorSide" in trade_cols:
            trades = trades.with_columns(
                TradeSide=pl.col("AggressorSide").cast(pl.Int64)
            )
        else:
            trades = trades.with_columns(TradeSide=pl.lit(None))

        mid = None
        if "PreTradeMid" in trade_cols:
            mid = pl.col("PreTradeMid")
        elif "MidPrice" in trade_cols:
            mid = pl.col("MidPrice")
        elif "BestBidPrice" in trade_cols and "BestAskPrice" in trade_cols:
            mid = (pl.col("BestBidPrice") + pl.col("BestAskPrice")) / 2

        if mid is not None:
            trades = trades.with_columns(
                TradeSide=pl.when(pl.col("TradeSide").is_in([1, 2]))
                .then(pl.col("TradeSide"))
                .when(pl.col("LocalPrice") > mid)
                .then(1)
                .when(pl.col("LocalPrice") < mid)
                .then(2)
                .otherwise(None)
            )
        return trades

    def _trade_match_tag(self, exec_base: pl.LazyFrame) -> pl.LazyFrame:
        trades = self.trades
        if isinstance(trades, pl.DataFrame):
            trades = trades.lazy()

        trades = self._infer_trade_side(trades)

        trade_cols = trades.collect_schema().names()
        if "BMLLTradeType" in trade_cols and self.config.trade_match_trade_types:
            trades = trades.filter(
                pl.col("BMLLTradeType").is_in(self.config.trade_match_trade_types)
            )
        if "Classification" in trade_cols and self.config.trade_match_classifications:
            trades = trades.filter(
                pl.col("Classification").is_in(self.config.trade_match_classifications)
            )

        trades = trades.with_columns(
            L3Side=pl.when(pl.col("TradeSide") == 1)
            .then(2)
            .when(pl.col("TradeSide") == 2)
            .then(1)
            .otherwise(None)
        )

        exec_sorted = exec_base.sort("EventTimestamp")
        trades_sorted = trades.sort("TradeTimestamp")

        joined = trades_sorted.join_asof(
            exec_sorted,
            left_on="TradeTimestamp",
            right_on="EventTimestamp",
            by="ListingId",
            strategy="backward",
            tolerance=_duration_str(self.config.trade_match_tolerance_seconds),
        )

        cond = pl.col("ExecutionSize") > 0
        if self.config.trade_match_price_match:
            cond = cond & (pl.col("ExecutionPrice") == pl.col("LocalPrice"))
        if self.config.trade_match_require_side:
            cond = cond & (pl.col("L3Side") == pl.col("Side"))

        if self.config.trade_match_exclude_exact:
            cond = cond & (
                (pl.col("ExecutionSize") - pl.col("Size")).abs()
                > self.config.trade_match_size_tolerance
            )

        return joined.with_columns(IcebergTradeMatch=cond.fill_null(False))

    def _tag_trades(
        self, iceberg_exec: pl.LazyFrame, exec_base: pl.LazyFrame
    ) -> pl.LazyFrame:
        trades = self.trades
        if isinstance(trades, pl.DataFrame):
            trades = trades.lazy()

        exec_sorted = iceberg_exec.sort("EventTimestamp")
        trades_sorted = trades.sort("TradeTimestamp")

        joined = trades_sorted.join_asof(
            exec_sorted,
            left_on="TradeTimestamp",
            right_on="EventTimestamp",
            by="ListingId",
            strategy="backward",
            tolerance=_duration_str(self.config.match_tolerance_seconds),
        )

        cond = pl.col("RevealedQty") > self.config.min_revealed_qty
        if self.config.price_match:
            cond = cond & (pl.col("ExecutionPrice") == pl.col("LocalPrice"))

        joined = joined.with_columns(IcebergL3Revealed=cond.fill_null(False))

        if self.config.trade_match_enabled:
            trade_match = self._trade_match_tag(exec_base)
            joined = joined.join(
                trade_match.select(
                    ["ListingId", "TradeTimestamp", "IcebergTradeMatch"]
                ),
                on=["ListingId", "TradeTimestamp"],
                how="left",
            )
        else:
            joined = joined.with_columns(IcebergTradeMatch=pl.lit(False))

        return joined.with_columns(
            IcebergExecution=(
                pl.col("IcebergL3Revealed") | pl.col("IcebergTradeMatch")
            ).fill_null(False),
            IcebergTag=pl.when(
                pl.col("IcebergL3Revealed") & pl.col("IcebergTradeMatch")
            )
            .then(pl.lit("BOTH"))
            .when(pl.col("IcebergL3Revealed"))
            .then(pl.lit("L3_REVEALED"))
            .when(pl.col("IcebergTradeMatch"))
            .then(pl.lit("TRADE_MATCH"))
            .otherwise(None),
        )

    def _iceberg_link_map(self, iceberg_exec: pl.LazyFrame) -> pl.DataFrame:
        inserts = self.l3.filter(pl.col("LobAction") == 2).select(
            [
                "ListingId",
                "EventTimestamp",
                "Side",
                "Price",
                "OrderID",
            ]
        )
        removes = iceberg_exec.select(
            [
                "ListingId",
                "EventTimestamp",
                "Side",
                "ExecutionPrice",
                "OldPrice",
                "OrderID",
            ]
        )

        joined = inserts.sort("EventTimestamp").join_asof(
            removes.sort("EventTimestamp"),
            left_on="EventTimestamp",
            right_on="EventTimestamp",
            by=["ListingId", "Side"],
            strategy="backward",
            tolerance=_duration_str(self.config.peak_link_tolerance_seconds),
        )

        if self.config.peak_link_price_match:
            joined = joined.filter(
                (pl.col("Price") == pl.col("ExecutionPrice"))
                | (pl.col("Price") == pl.col("OldPrice"))
            )

        pairs = joined.select(
            [
                "ListingId",
                pl.col("OrderID_right").alias("RemoveOrderID"),
                pl.col("OrderID").alias("InsertOrderID"),
            ]
        ).drop_nulls()

        pairs_df = pairs.collect()
        if pairs_df.is_empty():
            return pl.DataFrame(
                schema={
                    "ListingId": pl.Int64,
                    "OrderID": pl.Int64,
                    "IcebergId": pl.Int64,
                }
            )

        rows = pairs_df.to_dicts()
        parent: dict[tuple, tuple] = {}

        def _find(x):
            parent.setdefault(x, x)
            if parent[x] != x:
                parent[x] = _find(parent[x])
            return parent[x]

        def _union(a, b):
            ra, rb = _find(a), _find(b)
            if ra != rb:
                parent[rb] = ra

        for row in rows:
            a = (row["ListingId"], row["RemoveOrderID"])
            b = (row["ListingId"], row["InsertOrderID"])
            _union(a, b)

        iceberg_ids = {}
        id_map = {}
        current_id = 0
        for node in parent.keys():
            root = _find(node)
            if root not in id_map:
                id_map[root] = current_id
                current_id += 1
            iceberg_ids[node] = id_map[root]

        return pl.DataFrame(
            {
                "ListingId": [k[0] for k in iceberg_ids.keys()],
                "OrderID": [k[1] for k in iceberg_ids.keys()],
                "IcebergId": list(iceberg_ids.values()),
            }
        )

    def compute(self) -> pl.LazyFrame:
        exec_base = self._l3_execution_df()
        iceberg_exec = self._iceberg_execution_df()

        if self.config.enable_peak_linking:
            mapping = self._iceberg_link_map(iceberg_exec)
            iceberg_exec = iceberg_exec.join(
                mapping.lazy(), on=["ListingId", "OrderID"], how="left"
            ).with_columns(IcebergId=pl.col("IcebergId").cast(pl.Int64))
            iceberg_exec = iceberg_exec.sort("EventTimestamp").with_columns(
                IcebergEventCount=pl.when(pl.col("IcebergId").is_not_null())
                .then(pl.col("IcebergId").cum_count().over("IcebergId") + 1)
                .otherwise(None)
            )

        if self.config.tag_trades:
            tagged_trades = self._tag_trades(iceberg_exec, exec_base)
            self.context[self.config.trade_tag_context_key] = tagged_trades

        ctx = AnalyticContext(
            base_df=iceberg_exec,
            cache={
                "avg_size_col": self.config.avg_size_source,
                "imbalance_col": self.config.imbalance_source,
                "metric_prefix": self.metric_prefix,
            },
            context=self.context,
        )

        analytic = IcebergExecutionAnalytic()
        expressions = build_expressions(ctx, [(analytic, self.config.metrics)])

        if not expressions:
            default_cfg = [
                IcebergMetricConfig(
                    measures=[
                        "ExecutionVolume",
                        "ExecutionCount",
                        "AverageSize",
                        "RevealedPeakCount",
                        "AveragePeakCount",
                        "OrderImbalance",
                    ],
                    sides="Total",
                    aggregations=["Sum"],
                )
            ]
            expressions = build_expressions(ctx, [(analytic, default_cfg)])

        gcols = ["ListingId", "TimeBucket"]
        df = iceberg_exec.group_by(gcols).agg(expressions)

        requested_measures = {
            measure
            for cfg in self.config.metrics
            for measure in (
                [cfg.measures] if isinstance(cfg.measures, str) else cfg.measures
            )
        }

        if (
            "AveragePeakCount" in requested_measures
            and "IcebergEventCount" in iceberg_exec.collect_schema().names()
        ):
            peak_avg = (
                iceberg_exec.drop_nulls(["IcebergId"])
                .group_by(["ListingId", "TimeBucket", "IcebergId"])
                .agg(pl.col("IcebergEventCount").max().alias("_IcebergPeakMax"))
                .group_by(["ListingId", "TimeBucket"])
                .agg(
                    pl.col("_IcebergPeakMax")
                    .mean()
                    .alias(self.apply_prefix("IcebergAveragePeakCount"))
                )
            )
            df = df.join(peak_avg, on=gcols, how="left")
        elif "AveragePeakCount" in requested_measures:
            df = df.with_columns(
                pl.lit(None).alias(self.apply_prefix("IcebergAveragePeakCount"))
            )

        self.df = df
        return df
