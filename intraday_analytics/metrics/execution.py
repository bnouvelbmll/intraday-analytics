import polars as pl
from intraday_analytics import BaseAnalytics, dc


class ExecutionAnalytics(BaseAnalytics):
    """
    Computes execution-related metrics, such as executed volumes and VWAP,
    categorized by aggressor side and trade type (Lit or Dark).

    Metrics computed:
    - Executed Volume for Bid and Ask sides.
    - VWAP for Bid and Ask sides.
    - Lit and Dark Volume for Sell, Buy, and Unknown aggressors.
    - Lit and Dark VWAP for Sell, Buy, and Unknown aggressors.
    - Lit and Dark Volume-Weighted Price Placement (VWPP) for Sell and Buy aggressors.
    - Trade Imbalance.
    """

    REQUIRES = ["trades", "l3"]

    def __init__(self):
        fill_cols = {
            "ExecutedVolumeBid": "zero",
            "ExecutedVolumeAsk": "zero",
            "LitVolumeSellAggressor": "zero",
            "LitVolumeBuyAggressor": "zero",
            "DarkVolumeSellAggressor": "zero",
            "DarkVolumeBuyAggressor": "zero",
            "DarkVolumeUnknownAggressor": "zero",
            "TradeImbalance": "zero",
        }
        super().__init__("execution", fill_cols)

    def compute(self) -> pl.LazyFrame:

        l3 = self.l3.with_columns(
            pl.col("TimeBucket").cast(pl.Int64).alias("TimeBucketInt")
        )
        trades = self.trades.with_columns(
            pl.col("TimeBucket").cast(pl.Int64).alias("TimeBucketInt")
        )

        def agg_exec_side(
            side_val: int, side_name: str, vwap_name: str
        ) -> pl.LazyFrame:
            return (
                l3.filter(pl.col("Side") == side_val)
                .group_by(["ListingId", "TimeBucketInt"])
                .agg(
                    pl.col("ExecutionSize").sum().alias(f"ExecutedVolume{side_name}"),
                    (pl.col("ExecutionPrice") * pl.col("ExecutionSize"))
                    .sum()
                    .truediv(pl.col("ExecutionSize").sum())
                    .alias(vwap_name),
                )
            )

        vol_bid = agg_exec_side(1, "Bid", "VwapBid")
        vol_ask = agg_exec_side(2, "Ask", "VwapAsk")

        def agg_trades(
            trade_type: str, aggressor_side: int, suffix: str
        ) -> pl.LazyFrame:
            aggressor_map = {1: "Buy", 2: "Sell", 0: "Unknown"}
            side_suffix = aggressor_map.get(aggressor_side, "Error")

            return (
                trades.filter(
                    (pl.col("BMLLTradeType") == trade_type)
                    & (pl.col("AggressorSide") == aggressor_side)
                )
                .group_by(["ListingId", "TimeBucketInt"])
                .agg(
                    pl.col("Size")
                    .sum()
                    .alias(f"{trade_type}Volume{side_suffix}Aggressor"),
                    (pl.col("LPrice") * pl.col("Size"))
                    .sum()
                    .truediv(pl.col("Size").sum())
                    .alias(f"{trade_type}VWAP{side_suffix}Aggressor"),
                    (
                        (
                            (pl.col("PricePoint").clip(0, 1) * 2 - 1) * pl.col("Size")
                        ).sum()
                        / pl.col("Size").sum()
                    ).alias(
                        f"{trade_type}VolumeWeightedPricePlacement{side_suffix}Aggressor"
                    ),
                )
            )

        lit_sell = agg_trades("LIT", 2, "LitAggSell")
        lit_buy = agg_trades("LIT", 1, "LitAggBuy")
        dark_sell = agg_trades("DARK", 2, "DarkAggSell")
        dark_buy = agg_trades("DARK", 1, "DarkAggBuy")
        dark_unknown = agg_trades("DARK", 0, "DarkAggUnknown")

        df = vol_bid

        for other_df, suffix in [
            (vol_ask, "_va"),
            (lit_sell, "_ls"),
            (lit_buy, "_lb"),
            (dark_sell, "_ds"),
            (dark_buy, "_db"),
            (dark_unknown, "_du"),
        ]:

            df = df.join(
                other_df, on=["ListingId", "TimeBucketInt"], how="full", suffix=suffix
            )
            df = dc(df, suffix, timebucket_col="TimeBucketInt")

        df = df.with_columns(
            pl.col("TimeBucketInt").cast(pl.Datetime("ns")).alias("TimeBucket")
        ).drop("TimeBucketInt")

        df = df.with_columns(
            (
                (pl.col("ExecutedVolumeAsk") - pl.col("ExecutedVolumeBid"))
                / (pl.col("ExecutedVolumeAsk") + pl.col("ExecutedVolumeBid"))
            ).alias("TradeImbalance")
        )
        self.df = df
        return df
