import logging
from typing import Optional, List

import polars as pl
from pydantic import BaseModel

from intraday_analytics.analytics_base import BaseAnalytics
from intraday_analytics.analytics_registry import register_analytics

logger = logging.getLogger(__name__)


class TradeCharacteristicsConfig(BaseModel):
    """
    Configuration for trade characteristics analytics.
    """

    ENABLED: bool = True
    metric_prefix: Optional[str] = None
    time_bucket_seconds: Optional[float] = None


@register_analytics(
    "trade_characteristics", config_attr="trade_characteristics_analytics"
)
class TradeCharacteristicsAnalytics(BaseAnalytics):
    """
    Lightweight data quality and structure characteristics for trades.
    """

    REQUIRES = ["trades"]

    def __init__(self, config: TradeCharacteristicsConfig):
        self.config = config
        super().__init__("trade_characteristics", metric_prefix=config.metric_prefix)

    def compute(self) -> pl.LazyFrame:
        if not self.config.ENABLED:
            return pl.DataFrame(
                schema={"ListingId": pl.Int64, "TimeBucket": pl.Datetime("ns")}
            ).lazy()

        trades = self.trades
        if isinstance(trades, pl.DataFrame):
            trades = trades.lazy()

        schema = trades.collect_schema()
        if "TimeBucket" not in schema.names():
            if self.config.time_bucket_seconds and "TradeTimestamp" in schema.names():
                trades = trades.with_columns(
                    TimeBucket=pl.col("TradeTimestamp").dt.truncate(
                        f"{int(self.config.time_bucket_seconds)}s"
                    )
                )
                schema = trades.collect_schema()
            else:
                logger.warning(
                    "Trade characteristics skipped: TimeBucket not available."
                )
                return pl.DataFrame(
                    schema={"ListingId": pl.Int64, "TimeBucket": pl.Datetime("ns")}
                ).lazy()

        gcols = [
            c
            for c in ["MIC", "ListingId", "Ticker", "TimeBucket"]
            if c in schema.names()
        ]

        expressions: List[pl.Expr] = [
            pl.len().alias(self.apply_prefix("TradeEventCount"))
        ]

        if "Size" in schema.names():
            expressions.append(
                pl.col("Size").sum().alias(self.apply_prefix("TradeVolume"))
            )
            expressions.append(
                (pl.col("Size") == 0).any().alias(self.apply_prefix("TradeHasZeroSize"))
            )
            expressions.append(
                (pl.col("Size") < 0)
                .any()
                .alias(self.apply_prefix("TradeHasCancellations"))
            )

        if "AggressorSide" in schema.names():
            if schema["AggressorSide"] == pl.String:
                unknown = pl.col("AggressorSide") == "UNKNOWN"
            else:
                unknown = pl.col("AggressorSide").is_null() | (
                    pl.col("AggressorSide") == 0
                )
            expressions.append(
                unknown.any().alias(self.apply_prefix("TradeHasUnknownAggressorSide"))
            )
            expressions.append(
                (~unknown).any().alias(self.apply_prefix("TradeHasKnownAggressorSide"))
            )

        if "BMLLTradeType" in schema.names():
            expressions.append(
                (pl.col("BMLLTradeType") == "UNKNOWN")
                .any()
                .alias(self.apply_prefix("TradeHasUnknownType"))
            )

        if "MarketState" in schema.names():
            expressions.append(
                pl.col("MarketState")
                .is_in(["UNKNOWN", "NOT_APPLICABLE"])
                .any()
                .alias(self.apply_prefix("TradeHasUnknownMarketState"))
            )

        if "BMLLTradeType" in schema.names():
            expressions.append(
                (pl.col("BMLLTradeType") == "LIT")
                .any()
                .alias(self.apply_prefix("TradeHasLitTrades"))
            )

        if (
            "PublicationTimestamp" in schema.names()
            and "TradeTimestamp" in schema.names()
        ):
            eq_pub = pl.col("PublicationTimestamp") == pl.col("TradeTimestamp")
            expressions.append(
                eq_pub.mean().alias(
                    self.apply_prefix("TradePubEqualsTradeTimestampRatio")
                )
            )
            expressions.append(
                (
                    (pl.col("PublicationTimestamp") - pl.col("TradeTimestamp"))
                    .dt.total_microseconds()
                    .mean()
                    / 1e6
                ).alias(self.apply_prefix("TradePubTradeTimestampAvgDiffS"))
            )

        if "ExecutionVenue" in schema.names():
            expressions.append(
                (pl.col("ExecutionVenue").n_unique() > 1).alias(
                    self.apply_prefix("TradeHasMultipleExecutionVenues")
                )
            )

        for col, name in [
            ("CrossingTrade", "TradeHasCrossingTrade"),
            ("NegotiatedTrade", "TradeHasNegotiatedTrade"),
            ("AlgorithmicTrade", "TradeHasAlgorithmicTrade"),
            ("IsBlock", "TradeHasBlockTrade"),
        ]:
            if col in schema.names():
                expressions.append(
                    (pl.col(col) == "Y").any().alias(self.apply_prefix(name))
                )

        if "TradeId" in schema.names():
            expressions.append(
                (pl.col("TradeId").is_not_null() & (pl.col("TradeId") != ""))
                .any()
                .alias(self.apply_prefix("TradeHasTradeId"))
            )

        df = trades.group_by(gcols).agg(expressions)
        self.df = df
        return df
