import polars as pl
from intraday_analytics import BaseAnalytics
from dataclasses import dataclass

@dataclass
class TradeAnalyticsConfig:
    pass

class TradeAnalytics(BaseAnalytics):
    """
    Computes trade-based analytics for continuous trading segments.

    Metrics computed:
    - OHLC (Open, High, Low, Close)
    - VWAP (Volume-Weighted Average Price)
    - Volume
    - VWPP (Volume-Weighted Price Placement)
    - Retail Trade Imbalance
    """

    REQUIRES = ["trades"]  # (trades_plus)

    def __init__(self, config: TradeAnalyticsConfig):
        self.config = config
        super().__init__(
            "trades",
            {
                "Volume": "zero",
                # ""
            },
        )

    def compute(self) -> pl.LazyFrame:
        df = (
            self.trades.filter(pl.col("Classification") == "LIT_CONTINUOUS")
            .group_by(["MIC", "ListingId", "Ticker", "TimeBucket"])
            .agg(
                (
                    (pl.col("LPrice") * pl.col("Size")).sum() / pl.col("Size").sum()
                ).alias("VWAP"),
                (
                    (((pl.col("PricePoint").clip(0, 1) * 2 - 1) * pl.col("Size")).sum())
                    / pl.col("Size").sum()
                ).alias("VolumeWeightedPricePlacement"),
                pl.col("Size").sum().alias("Volume"),
                pl.col("LPrice").drop_nans().first().alias("Open"),
                pl.col("LPrice").drop_nans().last().alias("Close"),
                pl.col("LPrice").drop_nans().max().alias("High"),
                pl.col("LPrice").drop_nans().min().alias("Low"),
                pl.col("MarketState").last(),
            )
        )
        df2 = (
            self.trades.filter(pl.col("Classification") == "LIT_CONTINUOUS")
            .filter(pl.col("BMLLParticipantType") == "RETAIL")
            .group_by(["MIC", "ListingId", "Ticker", "TimeBucket"])
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
        self.df = df.join(
            df2, on=["MIC", "ListingId", "Ticker", "TimeBucket"], how="left"
        )
        return self.df
