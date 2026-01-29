import polars as pl
from intraday_analytics import BaseAnalytics, BaseTWAnalytics
from dataclasses import dataclass

@dataclass
class L2AnalyticsConfig:
    levels: int = 10
    time_bucket_seconds: float = 60.0

class L2AnalyticsLast(BaseAnalytics):
    """
    Computes L2 order book metrics based on the last snapshot in each time bucket.

    Metrics computed:
    - Bid and Ask Price, Quantity, and Number of Orders for N levels.
    - Cumulative Bid and Ask Quantity and Number of Orders for N levels.
    - Order and Volume Imbalances for N levels.
    - Spread in basis points.
    """

    REQUIRES = ["l2"]

    def __init__(self, config: L2AnalyticsConfig):
        self.N = config.levels
        super().__init__(
            "l2last",
            {
                **{
                    f"{side}{field}{i}": "last"
                    for side in ("Bid", "Ask")
                    for field in (
                        "Price",
                        "Quantity",
                        "NumOrders",
                        "CumQuantity",
                        "CumOrders",
                    )
                    for i in range(1, self.N + 1)
                },
                **{
                    f"{field}{i}": "last"
                    for field in ("OrdersImbalance", "VolumeImbalance")
                    for i in range(1, self.N + 1)
                },
                "SpreadBps": "last",
            },
        )

    def compute(self) -> pl.LazyFrame:
        gcols = ["MIC", "ListingId", "Ticker", "TimeBucket", "CurrencyCode"]

        # --- Compute last snapshot ---
        l2_last = self.l2.group_by(gcols).agg(
            # This is actually a slow agg
            [
                pl.col(f"{side}{field}{i}").last()
                for side in ("Bid", "Ask")
                for field in ("Price", "Quantity", "NumOrders")
                for i in range(1, self.N + 1)
            ]
            + [pl.col("MarketState").last()]
        )

        # --- Compute cumulative levels ---
        for i in range(1, self.N + 1):
            for side in ["Bid", "Ask"]:
                l2_last = l2_last.with_columns(
                    (
                        pl.col(f"{side}Quantity{i}")
                        + (0 if i == 1 else pl.col(f"{side}CumQuantity{i-1}"))
                    ).alias(f"{side}CumQuantity{i}")
                )
                l2_last = l2_last.with_columns(
                    (
                        pl.col(f"{side}NumOrders{i}")
                        + (0 if i == 1 else pl.col(f"{side}CumOrders{i-1}"))
                    ).alias(f"{side}CumOrders{i}")
                )

        # --- Compute imbalances ---
        for i in range(1, self.N + 1):
            l2_last = l2_last.with_columns(
                (
                    (pl.col(f"BidCumQuantity{i}") - pl.col(f"AskCumQuantity{i}"))
                    / (pl.col(f"BidCumQuantity{i}") + pl.col(f"AskCumQuantity{i}"))
                ).alias(f"VolumeImbalance{i}"),
                (
                    (pl.col(f"BidCumOrders{i}") - pl.col(f"AskCumOrders{i}"))
                    / (pl.col(f"BidCumOrders{i}") + pl.col(f"AskCumOrders{i}"))
                ).alias(f"OrdersImbalance{i}"),
            )

        # --- Compute spreads ---
        l2_last = l2_last.with_columns(
            (
                20000
                * (pl.col("AskPrice1") - pl.col("BidPrice1"))
                / (pl.col("AskPrice1") + pl.col("BidPrice1"))
            ).alias("SpreadBps")
        )

        self.df = l2_last
        return l2_last


class L2AnalyticsTW(BaseTWAnalytics):
    """
    Computes time-weighted average (TWA) metrics for the L2 order book.

    Metrics computed:
    - Time-Weighted Average Spread (relative)
    - Time-Weighted Average Mid Price
    - Time-Weighted Average Ask Price
    - Time-Weighted Average Bid Price
    """

    REQUIRES = ["l2"]

    def __init__(self, config: L2AnalyticsConfig):
        super().__init__(
            "l2tw",
            {
                "Spread_RelTWA": "last",
                "Mid_TWA": "last",
                "Ask_TWA": "last",
                "Bid_TWA": "last",
            },
            nanoseconds=int(config.time_bucket_seconds * 1e9),
        )
        self.N = config.levels

    # TWAP Metrics
    def tw_analytics(self, l2: pl.LazyFrame, **kwargs) -> pl.LazyFrame:
        N = self.N
        l2 = l2.group_by(["ListingId", "TimeBucket"])

        l2 = l2.agg(
            (
                (
                    pl.col("DT")
                    * (
                        20000
                        * (pl.col(f"AskPrice1") - pl.col(f"BidPrice1"))
                        / (pl.col(f"AskPrice1") + pl.col(f"BidPrice1"))
                    )
                ).sum()
                / pl.col("DT").sum()
            ).alias("Spread_RelTWA"),
            (
                (
                    pl.col("DT") * (0.5 * (pl.col(f"AskPrice1") + pl.col(f"BidPrice1")))
                ).sum()
                / pl.col("DT").sum()
            ).alias("Mid_TWA"),
            (((pl.col("DT") * pl.col(f"AskPrice1")).sum()) / pl.col("DT").sum()).alias(
                "Ask_TWA"
            ),
            (((pl.col("DT") * pl.col(f"BidPrice1")).sum()) / pl.col("DT").sum()).alias(
                "Bid_TWA"
            ),
            ((pl.col("EventTimestamp"))).len().alias("EventCount"),
        )
        return l2
