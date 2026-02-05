import polars as pl
from pydantic import BaseModel, Field
from typing import List, Union, Literal, Dict, Any, Optional

from intraday_analytics.analytics_base import BaseAnalytics
from .common import CombinatorialMetricConfig
from intraday_analytics.analytics_base import (
    AnalyticSpec,
    AnalyticContext,
    AnalyticDoc,
    analytic_expression,
    build_expressions,
)
from intraday_analytics.analytics_registry import register_analytics
from .utils import apply_alias


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

    metric_type: Literal["Iceberg"] = "Iceberg"

    measures: Union[IcebergMeasure, List[IcebergMeasure]] = Field(
        ..., description="Iceberg measure to compute."
    )
    sides: Union[IcebergSide, List[IcebergSide]] = Field(
        default="Total", description="Side filter (Bid, Ask, or Total)."
    )


class IcebergAnalyticsConfig(BaseModel):
    ENABLED: bool = True
    metric_prefix: Optional[str] = None
    metrics: List[IcebergMetricConfig] = Field(default_factory=list)

    tag_trades: bool = True
    trade_tag_context_key: str = "trades_iceberg"
    match_tolerance_seconds: float = Field(5e-4, gt=0)
    price_match: bool = True
    min_revealed_qty: float = Field(0.0, ge=0.0)
    avg_size_source: Literal["RevealedQty", "ExecutionSize", "OldSize"] = "RevealedQty"
    imbalance_source: Literal["ExecutionSize", "RevealedQty"] = "ExecutionSize"
    trade_match_enabled: bool = True
    trade_match_tolerance_seconds: float = Field(5e-4, gt=0)
    trade_match_price_match: bool = True
    trade_match_exclude_exact: bool = True
    trade_match_size_tolerance: float = Field(0.0, ge=0.0)
    trade_match_trade_types: List[str] = Field(default_factory=lambda: ["LIT", "DARK"])
    trade_match_classifications: List[str] = Field(default_factory=list)
    trade_match_require_side: bool = True
    enable_peak_linking: bool = True
    peak_link_tolerance_seconds: float = Field(5e-4, gt=0)
    peak_link_price_match: bool = True


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
        return (
            base.with_columns(
                RevealedQty=(pl.col("ExecutionSize") - pl.col("OldSize")).clip(0)
            )
            .filter(pl.col("RevealedQty") > self.config.min_revealed_qty)
        )

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
            trades = trades.filter(pl.col("BMLLTradeType").is_in(self.config.trade_match_trade_types))
        if "Classification" in trade_cols and self.config.trade_match_classifications:
            trades = trades.filter(pl.col("Classification").is_in(self.config.trade_match_classifications))

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

    def _tag_trades(self, iceberg_exec: pl.LazyFrame, exec_base: pl.LazyFrame) -> pl.LazyFrame:
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
                trade_match.select(["ListingId", "TradeTimestamp", "IcebergTradeMatch"]),
                on=["ListingId", "TradeTimestamp"],
                how="left",
            )
        else:
            joined = joined.with_columns(IcebergTradeMatch=pl.lit(False))

        return joined.with_columns(
            IcebergExecution=(
                pl.col("IcebergL3Revealed") | pl.col("IcebergTradeMatch")
            ).fill_null(False),
            IcebergTag=pl.when(pl.col("IcebergL3Revealed") & pl.col("IcebergTradeMatch"))
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
            ).with_columns(
                IcebergId=pl.col("IcebergId").cast(pl.Int64)
            )
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

        if "AveragePeakCount" in requested_measures and "IcebergEventCount" in iceberg_exec.collect_schema().names():
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