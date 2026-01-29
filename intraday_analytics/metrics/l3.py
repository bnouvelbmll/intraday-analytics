import polars as pl
from intraday_analytics import BaseAnalytics, dc
from dataclasses import dataclass

@dataclass
class L3AnalyticsConfig:
    pass

class L3Analytics(BaseAnalytics):
    """
    Computes L3 order book event metrics, such as counts and volumes for
    insert, remove, and update events on both the bid and ask sides.

    Metrics computed:
    - Insert, Remove, and Update Counts for Bid and Ask sides.
    - Insert, Remove, and Update Volumes for Bid and Ask sides.
    """

    REQUIRES = ["l3"]

    def __init__(self, config: L3AnalyticsConfig):
        self.config = config
        super().__init__(
            "l3",
            {
                "InsertCountBid": "zero",
                "RemoveCountBid": "zero",
                "UpdateCountBid": "zero",
                "InsertVolumeBid": "zero",
                "RemoveVolumeBid": "zero",
                # "UpdateVolumeBid":"zero",
                "UpdateInsertedVolumeBid": "zero",
                "UpdateRemovedVolumeBid": "zero",
                "InsertCountAsk": "zero",
                "RemoveCountAsk": "zero",
                "UpdateCountAsk": "zero",
                "InsertVolumeAsk": "zero",
                "RemoveVolumeAsk": "zero",
                # "UpdateVolumeAsk":"zero",
                "UpdateInsertedVolumeAsk": "zero",
                "UpdateRemovedVolumeAsk": "zero",
            },
        )

    def compute(self) -> pl.LazyFrame:
        df_bid = (
            self.l3.filter(pl.col("Side") == 1)
            .group_by(["ListingId", "TimeBucket"])
            .agg(
                (pl.when(pl.col("LobAction") == 2).then(1).otherwise(0))
                .sum()
                .alias("InsertCountBid"),
                (pl.when(pl.col("LobAction") == 3).then(1).otherwise(0))
                .sum()
                .alias("RemoveCountBid"),
                (pl.when(pl.col("LobAction") == 4).then(1).otherwise(0))
                .sum()
                .alias("UpdateCountBid"),
                (pl.when(pl.col("LobAction") == 2).then(pl.col("Size")).otherwise(0))
                .sum()
                .alias("InsertVolumeBid"),
                (pl.when(pl.col("LobAction") == 3).then(pl.col("OldSize")).otherwise(0))
                .sum()
                .alias("RemoveVolumeBid"),
                (pl.when(pl.col("LobAction") == 4).then(pl.col("Size")).otherwise(0))
                .sum()
                .alias("UpdateInsertedVolumeBid"),
                (pl.when(pl.col("LobAction") == 4).then(pl.col("OldSize")).otherwise(0))
                .sum()
                .alias("UpdateRemovedVolumeBid"),
            )
        )
        df_ask = (
            self.l3.filter(pl.col("Side") == 2)
            .group_by(["ListingId", "TimeBucket"])
            .agg(
                (pl.when(pl.col("LobAction") == 2).then(1).otherwise(0))
                .sum()
                .alias("InsertCountAsk"),
                (pl.when(pl.col("LobAction") == 3).then(1).otherwise(0))
                .sum()
                .alias("RemoveCountAsk"),
                (pl.when(pl.col("LobAction") == 4).then(1).otherwise(0))
                .sum()
                .alias("UpdateCountAsk"),
                (pl.when(pl.col("LobAction") == 2).then(pl.col("Size")).otherwise(0))
                .sum()
                .alias("InsertVolumeAsk"),
                (pl.when(pl.col("LobAction") == 3).then(pl.col("OldSize")).otherwise(0))
                .sum()
                .alias("RemoveVolumeAsk"),
                (pl.when(pl.col("LobAction") == 4).then(pl.col("Size")).otherwise(0))
                .sum()
                .alias("UpdateInsertedVolumeAsk"),
                (pl.when(pl.col("LobAction") == 4).then(pl.col("OldSize")).otherwise(0))
                .sum()
                .alias("UpdateRemovedVolumeAsk"),
            )
        )

        self.df = dc(
            df_bid.join(
                df_ask, on=["ListingId", "TimeBucket"], how="full", suffix="_ask"
            ),
            "_ask",
        )
        return self.df
