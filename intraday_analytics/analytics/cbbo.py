import polars as pl
from pydantic import BaseModel, Field
from typing import List, Literal, Optional, Dict, Any

from intraday_analytics.analytics_base import (
    BaseAnalytics,
    AnalyticSpec,
    AnalyticContext,
    analytic_expression,
    apply_metric_prefix,
)
from intraday_analytics.analytics_registry import register_analytics


CBBOMeasure = Literal["TimeAtCBB", "TimeAtCBO", "QuantityAtCBB", "QuantityAtCBO"]
CBBOQuantityAgg = Literal["TWMean", "Min", "Max", "Median"]


class CBBOAnalyticsConfig(BaseModel):
    ENABLED: bool = True
    metric_prefix: Optional[str] = None
    measures: List[CBBOMeasure] = Field(
        default_factory=lambda: [
            "TimeAtCBB",
            "TimeAtCBO",
            "QuantityAtCBB",
            "QuantityAtCBO",
        ]
    )
    quantity_aggregations: List[CBBOQuantityAgg] = Field(
        default_factory=lambda: ["TWMean", "Min", "Max", "Median"]
    )


class CBBOAnalytic(AnalyticSpec):
    MODULE = "cbbo"
    ConfigModel = CBBOAnalyticsConfig

    @analytic_expression(
        "TimeAtCBB",
        pattern=r"^TimeAtCBB$",
        unit="Ratio",
    )
    def _expression_time_at_cbb(self, ctx: AnalyticContext) -> pl.Expr:
        """Share of time spent at CBB (best bid matches CBBO bid) within the TimeBucket."""
        dt = ctx.cache["dt"]
        dt_match = ctx.cache["dt_match_bid"]
        denom = dt.sum()
        numer = dt_match.sum()
        return pl.when(denom > 0).then(numer / denom).otherwise(None)

    @analytic_expression(
        "TimeAtCBO",
        pattern=r"^TimeAtCBO$",
        unit="Ratio",
    )
    def _expression_time_at_cbo(self, ctx: AnalyticContext) -> pl.Expr:
        """Share of time spent at CBO (best ask matches CBBO ask) within the TimeBucket."""
        dt = ctx.cache["dt"]
        dt_match = ctx.cache["dt_match_ask"]
        denom = dt.sum()
        numer = dt_match.sum()
        return pl.when(denom > 0).then(numer / denom).otherwise(None)

    @analytic_expression(
        "QuantityAtCBB",
        pattern=r"^QuantityAtCBB(?:Min|Max|Median)?$",
        unit="Shares",
    )
    def _expression_quantity_at_cbb(
        self, ctx: AnalyticContext, agg: CBBOQuantityAgg
    ) -> tuple[pl.Expr, str] | None:
        """Quantity available at CBB within the TimeBucket (aggregation varies by suffix)."""
        qty = ctx.cache["qty_match_bid"]
        dt = ctx.cache["dt"]
        if agg == "TWMean":
            denom = dt.sum()
            numer = (dt * qty).sum()
            return pl.when(denom > 0).then(numer / denom).otherwise(None), "QuantityAtCBB"
        if agg == "Min":
            return qty.min(), "QuantityAtCBBMin"
        if agg == "Max":
            return qty.max(), "QuantityAtCBBMax"
        if agg == "Median":
            return qty.median(), "QuantityAtCBBMedian"
        return None

    @analytic_expression(
        "QuantityAtCBO",
        pattern=r"^QuantityAtCBO(?:Min|Max|Median)?$",
        unit="Shares",
    )
    def _expression_quantity_at_cbo(
        self, ctx: AnalyticContext, agg: CBBOQuantityAgg
    ) -> tuple[pl.Expr, str] | None:
        """Quantity available at CBO within the TimeBucket (aggregation varies by suffix)."""
        qty = ctx.cache["qty_match_ask"]
        dt = ctx.cache["dt"]
        if agg == "TWMean":
            denom = dt.sum()
            numer = (dt * qty).sum()
            return pl.when(denom > 0).then(numer / denom).otherwise(None), "QuantityAtCBO"
        if agg == "Min":
            return qty.min(), "QuantityAtCBOMin"
        if agg == "Max":
            return qty.max(), "QuantityAtCBOMax"
        if agg == "Median":
            return qty.median(), "QuantityAtCBOMedian"
        return None

    def expressions(
        self, ctx: AnalyticContext, config: CBBOAnalyticsConfig, variant: Dict[str, Any]
    ) -> List[pl.Expr]:
        measure = variant["measures"]
        expression_fn = self.EXPRESSIONS.get(measure)
        if expression_fn is None:
            return []

        if measure in ("QuantityAtCBB", "QuantityAtCBO"):
            outputs: List[pl.Expr] = []
            for agg in config.quantity_aggregations:
                result = expression_fn(self, ctx, agg)
                if result is None:
                    continue
                expr, name = result
                outputs.append(expr.alias(apply_metric_prefix(ctx, name)))
            return outputs

        expr = expression_fn(self, ctx)
        return [expr.alias(apply_metric_prefix(ctx, measure))]


@register_analytics("cbbo", config_attr="cbbo_analytics", needs_ref=True)
class CBBOAnalytics(BaseAnalytics):
    """
    Computes CBBO alignment metrics by joining L2 with millisecond CBBO via InstrumentId.
    """

    REQUIRES = ["l2", "cbbo"]

    def __init__(self, ref: pl.DataFrame, config: CBBOAnalyticsConfig):
        self.ref = ref
        self.config = config
        super().__init__("cbbo", {}, metric_prefix=config.metric_prefix)

    def compute(self) -> pl.LazyFrame:
        if self.ref is None:
            raise ValueError("CBBOAnalytics requires ref with InstrumentId.")

        l2 = self.l2.lazy() if isinstance(self.l2, pl.DataFrame) else self.l2
        cbbo = self.cbbo.lazy() if isinstance(self.cbbo, pl.DataFrame) else self.cbbo

        ref_cols = self.ref.select(["ListingId", "InstrumentId"]).lazy()

        l2 = (
            l2.join(ref_cols, on="ListingId", how="left")
            .with_columns(
                EventTimestampMs=pl.col("EventTimestamp").dt.truncate("1ms")
            )
            .sort(["ListingId", "EventTimestamp"])
            .with_columns(
                NextEventTimestamp=pl.col("EventTimestamp").shift(-1).over("ListingId")
            )
            .with_columns(
                DT=(pl.col("NextEventTimestamp") - pl.col("EventTimestamp"))
                .dt.total_nanoseconds()
                .clip(0, 10**12)
            )
        )

        cbbo = (
            cbbo.join(ref_cols, on="ListingId", how="left")
            .with_columns(
                EventTimestampMs=pl.col("EventTimestamp").dt.truncate("1ms")
            )
            .select(
                [
                    "InstrumentId",
                    "EventTimestampMs",
                    pl.col("BidPrice1").alias("CBBOBidPrice1"),
                    pl.col("AskPrice1").alias("CBBOAskPrice1"),
                ]
            )
        )

        joined = l2.join(
            cbbo,
            on=["InstrumentId", "EventTimestampMs"],
            how="left",
        )

        match_bid = (pl.col("BidPrice1") == pl.col("CBBOBidPrice1")).fill_null(False)
        match_ask = (pl.col("AskPrice1") == pl.col("CBBOAskPrice1")).fill_null(False)

        ctx = AnalyticContext(
            base_df=joined,
            cache={
                "dt": pl.col("DT"),
                "dt_match_bid": pl.col("DT") * match_bid.cast(pl.Int64),
                "dt_match_ask": pl.col("DT") * match_ask.cast(pl.Int64),
                "qty_match_bid": pl.col("BidQuantity1") * match_bid.cast(pl.Int64),
                "qty_match_ask": pl.col("AskQuantity1") * match_ask.cast(pl.Int64),
                "metric_prefix": self.metric_prefix,
            },
            context=self.context,
        )

        analytic = CBBOAnalytic()
        expressions: list[pl.Expr] = []
        for measure in self.config.measures:
            variant = {"measures": measure}
            expressions.extend(analytic.expressions(ctx, self.config, variant))

        gcols = ["ListingId", "TimeBucket"]
        df = joined.group_by(gcols).agg(expressions)
        self.df = df
        return df
