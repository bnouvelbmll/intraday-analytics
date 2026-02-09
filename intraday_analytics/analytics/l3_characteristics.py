import logging
from typing import Optional, List

import polars as pl
from pydantic import BaseModel

from intraday_analytics.analytics_base import BaseAnalytics
from intraday_analytics.analytics_registry import register_analytics

logger = logging.getLogger(__name__)


class L3CharacteristicsConfig(BaseModel):
    """
    Configuration for L3 characteristics analytics.
    """

    ENABLED: bool = True
    metric_prefix: Optional[str] = None
    time_bucket_seconds: Optional[float] = None


@register_analytics("l3_characteristics", config_attr="l3_characteristics_analytics")
class L3CharacteristicsAnalytics(BaseAnalytics):
    """
    Lightweight data quality and structure characteristics for L3 data.
    """

    REQUIRES = ["l3"]

    def __init__(self, config: L3CharacteristicsConfig):
        self.config = config
        super().__init__("l3_characteristics", metric_prefix=config.metric_prefix)

    def compute(self) -> pl.LazyFrame:
        if not self.config.ENABLED:
            return pl.DataFrame(
                schema={"ListingId": pl.Int64, "TimeBucket": pl.Datetime("ns")}
            ).lazy()

        l3 = self.l3
        if isinstance(l3, pl.DataFrame):
            l3 = l3.lazy()

        schema = l3.collect_schema()
        if "TimeBucket" not in schema.names():
            if self.config.time_bucket_seconds and "EventTimestamp" in schema.names():
                l3 = l3.with_columns(
                    TimeBucket=pl.col("EventTimestamp").dt.truncate(
                        f"{int(self.config.time_bucket_seconds)}s"
                    )
                )
                schema = l3.collect_schema()
            else:
                logger.warning("L3 characteristics skipped: TimeBucket not available.")
                return pl.DataFrame(
                    schema={"ListingId": pl.Int64, "TimeBucket": pl.Datetime("ns")}
                ).lazy()

        gcols = [
            c
            for c in ["MIC", "ListingId", "Ticker", "TimeBucket"]
            if c in schema.names()
        ]

        expressions: List[pl.Expr] = [pl.len().alias(self.apply_prefix("L3EventCount"))]

        exec_col = "ExecutionSize" if "ExecutionSize" in schema.names() else None
        if exec_col:
            exec_expr = pl.col(exec_col) > 0
            expressions.append(
                exec_expr.sum().alias(self.apply_prefix("L3ExecutionCount"))
            )
            expressions.append(
                exec_expr.any().alias(self.apply_prefix("L3HasExecution"))
            )

        if "OrderExecuted" in schema.names():
            expressions.append(
                pl.col("OrderExecuted")
                .cast(pl.Boolean)
                .any()
                .alias(self.apply_prefix("L3HasOrderExecuted"))
            )

        if "MarketState" in schema.names():
            expressions.append(
                (pl.col("MarketState") == "CONTINUOUS_TRADING")
                .any()
                .alias(self.apply_prefix("L3HasContinuousTrading"))
            )
            expressions.append(
                (pl.col("MarketState") == "OPENING_AUCTION")
                .any()
                .alias(self.apply_prefix("L3HasOpeningAuction"))
            )
            expressions.append(
                (pl.col("MarketState") == "CLOSING_AUCTION")
                .any()
                .alias(self.apply_prefix("L3HasClosingAuction"))
            )
            expressions.append(
                (pl.col("MarketState") == "INTRADAY_AUCTION")
                .any()
                .alias(self.apply_prefix("L3HasIntradayAuction"))
            )
            if exec_col:
                expressions.append(
                    ((pl.col("MarketState") == "PRE_OPEN") & (pl.col(exec_col) > 0))
                    .any()
                    .alias(self.apply_prefix("L3HasPreOpenExecution"))
                )
                expressions.append(
                    ((pl.col("MarketState") == "POST_TRADE") & (pl.col(exec_col) > 0))
                    .any()
                    .alias(self.apply_prefix("L3HasPostTradeExecution"))
                )

        if "EndOfEvent" in schema.names():
            expressions.append(
                (~pl.col("EndOfEvent").cast(pl.Boolean))
                .any()
                .alias(self.apply_prefix("L3HasMultiRowEvent"))
            )

        if "TradeId" in schema.names():
            expressions.append(
                (pl.col("TradeId").is_not_null() & (pl.col("TradeId") != ""))
                .any()
                .alias(self.apply_prefix("L3HasTradeId"))
            )

        if "LobAction" in schema.names():
            if schema["LobAction"] == pl.String:
                updates = (pl.col("LobAction") == "UPDATE").any()
            else:
                updates = (pl.col("LobAction") == 2).any()
            expressions.append(updates.alias(self.apply_prefix("L3HasUpdates")))
        elif "OldPrice" in schema.names() or "OldSize" in schema.names():
            updates = pl.lit(False)
            if "OldPrice" in schema.names():
                updates = updates | pl.col("OldPrice").is_not_null()
            if "OldSize" in schema.names():
                updates = updates | pl.col("OldSize").is_not_null()
            expressions.append(updates.any().alias(self.apply_prefix("L3HasUpdates")))

        if "OrdersAhead" in schema.names() and "OrderExecuted" in schema.names():
            expressions.append(
                ((pl.col("OrdersAhead") > 0) & pl.col("OrderExecuted"))
                .any()
                .alias(self.apply_prefix("L3ExecutedWithOrdersAhead"))
            )

        if "BestBidPrice" in schema.names() and "BestAskPrice" in schema.names():
            expressions.append(
                (pl.col("BestBidPrice") == pl.col("BestAskPrice"))
                .any()
                .alias(self.apply_prefix("L3HasLockedBook"))
            )
            expressions.append(
                (pl.col("BestBidPrice") > pl.col("BestAskPrice"))
                .any()
                .alias(self.apply_prefix("L3HasCrossedBook"))
            )
            if "MarketState" in schema.names():
                expressions.append(
                    (
                        (pl.col("MarketState") == "CONTINUOUS_TRADING")
                        & (pl.col("BestBidPrice") == pl.col("BestAskPrice"))
                    )
                    .any()
                    .alias(self.apply_prefix("L3HasLockedBookContinuous"))
                )
                expressions.append(
                    (
                        (pl.col("MarketState") == "CONTINUOUS_TRADING")
                        & (pl.col("BestBidPrice") > pl.col("BestAskPrice"))
                    )
                    .any()
                    .alias(self.apply_prefix("L3HasCrossedBookContinuous"))
                )
                expressions.append(
                    (
                        (pl.col("MarketState") != "CONTINUOUS_TRADING")
                        & (pl.col("BestBidPrice") > pl.col("BestAskPrice"))
                    )
                    .any()
                    .alias(self.apply_prefix("L3HasCrossedBookNonContinuous"))
                )

        if "ReceiveTimestamp" in schema.names():
            expressions.append(
                (pl.col("ReceiveTimestamp") > 0)
                .any()
                .alias(self.apply_prefix("L3HasReceiveTimestamp"))
            )

        if "SendTimestamp" in schema.names():
            expressions.append(
                (pl.col("SendTimestamp") > 0)
                .any()
                .alias(self.apply_prefix("L3HasSendTimestamp"))
            )

        if (
            "LobAction" in schema.names()
            and "OldPrice" in schema.names()
            and "OldSize" in schema.names()
        ):
            if schema["LobAction"] == pl.String:
                is_update = pl.col("LobAction") == "UPDATE"
            else:
                is_update = pl.col("LobAction") == 2
            expressions.append(
                (
                    is_update
                    & (pl.col("OldPrice") == pl.col("Price"))
                    & (pl.col("OldSize") == pl.col("Size"))
                )
                .any()
                .alias(self.apply_prefix("L3HasNoOpUpdate"))
            )

        if "OrdersAhead" in schema.names() and "SizeAhead" in schema.names():
            expressions.append(
                ((pl.col("SizeAhead") != 0) == (pl.col("OrdersAhead") != 0))
                .all()
                .alias(self.apply_prefix("L3SizeAheadOrdersAheadConsistent"))
            )

        if "OrderExecuted" in schema.names() and "OldPriceLevel" in schema.names():
            expressions.append(
                (
                    pl.col("OrderExecuted")
                    & ((pl.col("OldPriceLevel") != 1) | (pl.col("OrdersAhead") > 0))
                )
                .any()
                .alias(self.apply_prefix("L3ExecutedNotAtTopOfBook"))
            )

        if "EventTimestamp" in schema.names():
            ts_diff = pl.col("EventTimestamp").diff().dt.total_nanoseconds()
            expressions.append(
                (ts_diff.min().fill_null(0) >= 0).alias(
                    self.apply_prefix("L3EventTimestampNonDecreasing")
                )
            )
            expressions.append(
                (ts_diff.min().fill_null(0) > 0).alias(
                    self.apply_prefix("L3EventTimestampStrictlyIncreasing")
                )
            )

        for col, name in [
            ("EventNo", "L3EventNo"),
            ("ExchangeSequenceNo", "L3ExchangeSequenceNo"),
            ("BMLLSequenceNo", "L3BMLLSequenceNo"),
        ]:
            if col in schema.names() and "EventTimestamp" in schema.names():
                diff = pl.col(col).diff()
                expressions.append(
                    (diff.min().fill_null(0) >= 0).alias(
                        self.apply_prefix(f"{name}NonDecreasing")
                    )
                )
                expressions.append(
                    (diff.min().fill_null(0) > 0).alias(
                        self.apply_prefix(f"{name}StrictlyIncreasing")
                    )
                )

        df = l3.group_by(gcols).agg(expressions)
        self.df = df
        return df
