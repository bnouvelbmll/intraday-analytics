import polars as pl
from pydantic import BaseModel, Field
from typing import List, Literal

from intraday_analytics.analytics_base import BaseAnalytics
from intraday_analytics.analytics_registry import register_analytics


CBBOMeasure = Literal["TimeAtCBB", "TimeAtCBO", "QuantityAtCBB", "QuantityAtCBO"]
CBBOQuantityAgg = Literal["TWMean", "Min", "Max", "Median"]


class CBBOAnalyticsConfig(BaseModel):
    ENABLED: bool = True
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


@register_analytics("cbbo", config_attr="cbbo_analytics", needs_ref=True)
class CBBOAnalytics(BaseAnalytics):
    """
    Computes CBBO alignment metrics by joining L2 with millisecond CBBO via InstrumentId.
    """

    REQUIRES = ["l2", "cbbo"]

    def __init__(self, ref: pl.DataFrame, config: CBBOAnalyticsConfig):
        self.ref = ref
        self.config = config
        super().__init__("cbbo", {})

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

        dt = pl.col("DT")
        dt_match_bid = dt * match_bid.cast(pl.Int64)
        dt_match_ask = dt * match_ask.cast(pl.Int64)
        qty_match_bid = (pl.col("BidQuantity1") * match_bid.cast(pl.Int64))
        qty_match_ask = (pl.col("AskQuantity1") * match_ask.cast(pl.Int64))

        expressions: list[pl.Expr] = []

        if "TimeAtCBB" in self.config.measures:
            denom = dt.sum()
            numer = dt_match_bid.sum()
            expressions.append(
                pl.when(denom > 0).then(numer / denom).otherwise(None).alias("TimeAtCBB")
            )

        if "TimeAtCBO" in self.config.measures:
            denom = dt.sum()
            numer = dt_match_ask.sum()
            expressions.append(
                pl.when(denom > 0).then(numer / denom).otherwise(None).alias("TimeAtCBO")
            )

        if "QuantityAtCBB" in self.config.measures:
            for agg in self.config.quantity_aggregations:
                if agg == "TWMean":
                    denom = dt.sum()
                    numer = (dt * qty_match_bid).sum()
                    expr = pl.when(denom > 0).then(numer / denom).otherwise(None)
                    name = "QuantityAtCBB"
                elif agg == "Min":
                    expr = qty_match_bid.min()
                    name = "QuantityAtCBBMin"
                elif agg == "Max":
                    expr = qty_match_bid.max()
                    name = "QuantityAtCBBMax"
                elif agg == "Median":
                    expr = qty_match_bid.median()
                    name = "QuantityAtCBBMedian"
                else:
                    continue
                expressions.append(expr.alias(name))

        if "QuantityAtCBO" in self.config.measures:
            for agg in self.config.quantity_aggregations:
                if agg == "TWMean":
                    denom = dt.sum()
                    numer = (dt * qty_match_ask).sum()
                    expr = pl.when(denom > 0).then(numer / denom).otherwise(None)
                    name = "QuantityAtCBO"
                elif agg == "Min":
                    expr = qty_match_ask.min()
                    name = "QuantityAtCBOMin"
                elif agg == "Max":
                    expr = qty_match_ask.max()
                    name = "QuantityAtCBOMax"
                elif agg == "Median":
                    expr = qty_match_ask.median()
                    name = "QuantityAtCBOMedian"
                else:
                    continue
                expressions.append(expr.alias(name))

        gcols = ["ListingId", "TimeBucket"]
        df = joined.group_by(gcols).agg(expressions)
        self.df = df
        return df
