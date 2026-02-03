import polars as pl
import pandas as pd
import datetime as dt
import logging
from pydantic import BaseModel, Field
from typing import List
import polars as pl

from intraday_analytics.bases import BaseAnalytics
from intraday_analytics.utils import SYMBOL_COL
from pydantic import BaseModel, Field
from typing import List, Optional, Literal

logger = logging.getLogger(__name__)


class DenseAnalyticsConfig(BaseModel):
    ENABLED: bool = True
    mode: Literal["adaptative", "uniform"] = "adaptative"
    time_interval: Optional[List[str]] = None
    time_bucket_seconds: Optional[float] = None
    time_bucket_anchor: Literal["end", "start"] = "end"
    time_bucket_closed: Literal["right", "left"] = "right"


class DenseAnalytics(BaseAnalytics):
    """
    Generates a dense time grid.
    """

    REQUIRES = ["trades", "marketstate"]

    def __init__(self, ref: pl.DataFrame, config: DenseAnalyticsConfig):
        self.ref = ref
        self.config = config
        super().__init__("da")

    def compute(self) -> pl.LazyFrame:
        # (Keep original logic as it's structural, not metric-generating in the same sense)
        sc = SYMBOL_COL
        symbols = pl.DataFrame({sc: list(sorted(self.ref[sc].unique()))})

        if self.config.mode == "uniform":
            dates = (
                self.trades.select("TradeDate").collect()["TradeDate"].value_counts()
            )
            date = dates.sort("count")[-1]["TradeDate"][0]

            start_time = str(self.config.time_interval[0])
            end_time = str(self.config.time_interval[1])
            start_time = pd.Timestamp("1971-01-01 " + start_time).time()
            end_time = pd.Timestamp("1971-01-01 " + end_time).time()

            start_dt = dt.datetime(
                date.year, date.month, date.day, start_time.hour, start_time.minute
            )

            end_dt = dt.datetime(
                date.year, date.month, date.day, end_time.hour, end_time.minute
            )

            frequency = str(int(self.config.time_bucket_seconds * 1e9)) + "ns"
            interval_ns = int(self.config.time_bucket_seconds * 1e9)
            interval = dt.timedelta(seconds=self.config.time_bucket_seconds)

            if self.config.time_bucket_anchor == "end":
                range_start = start_dt + interval
                range_end = end_dt
                closed = "both"
            else:
                range_start = start_dt
                range_end = end_dt
                closed = "left"

            times_df = pl.DataFrame(
                {
                    "TimeBucket": pl.datetime_range(
                        start=range_start,
                        end=range_end,
                        interval=frequency,
                        closed=closed,
                        eager=True,
                    )
                }
            )

            return (
                pl.LazyFrame(symbols.cast(pl.Int64))
                .join(pl.LazyFrame(times_df), how="cross")
                .join(
                    self.ref.lazy().select(
                        [SYMBOL_COL, "MIC", "Ticker", "CurrencyCode"]
                    ),
                    on=SYMBOL_COL,
                    how="left",
                )
                .sort([sc, "TimeBucket"])
            )
        elif self.config.mode == "adaptative":
            tsc = "EventTimestamp"
            available_listings = self.marketstate.select("ListingId").unique().collect()
            symbols = symbols.join(available_listings, on=sc, how="inner")

            calendar = self.marketstate.filter(
                pl.col("ListingId").is_in(list(symbols[sc]))
            )
            continuous_intervals = (
                calendar.with_columns(
                    [
                        pl.col("MarketState")
                        .shift(1)
                        .over("ListingId")
                        .alias("pstate"),
                        pl.col("MarketState")
                        .shift(-1)
                        .over("ListingId")
                        .alias("nstate"),
                        pl.col(tsc).shift(-1).over("ListingId").alias("nts"),
                    ]
                )
                .filter(pl.col("MarketState") == "CONTINUOUS_TRADING")
                .filter(
                    (pl.col("pstate") != "CONTINUOUS_TRADING")
                    | (pl.col("nstate") != "CONTINUOUS_TRADING")
                    | (pl.col("pstate").is_null())
                    | (pl.col("nstate").is_null())
                )
                .select(["ListingId", "MarketState", tsc, "nts"])
                .collect()
                .to_pandas()
            )
            res = []
            failed_listings = []
            frequency = str(int(self.config.time_bucket_seconds * 1e9)) + "ns"
            TS_PADDING = pd.Timedelta(seconds=600)
            for s in symbols[sc]:
                ciq = continuous_intervals.query("ListingId==@s")[tsc]
                if len(ciq) == 0:
                    failed_listings.append(s)
                    continue
                start_dt = pd.Timestamp(ciq.iloc[0]).floor(str(frequency)) - TS_PADDING
                end_dt = (
                    pd.Timestamp(
                        continuous_intervals.query("ListingId==@s")["nts"].iloc[-1]
                    ).ceil(str(frequency))
                    + TS_PADDING
                )
                res.append(
                    pl.DataFrame(
                        {
                            "TimeBucket": pl.datetime_range(
                                start=start_dt,
                                end=end_dt,
                                interval=str(frequency),
                                closed="left",
                                eager=True,
                            ).cast(pl.Datetime("ns"))
                        }
                    ).with_columns(ListingId=s)
                )

            if failed_listings:
                logger.warning(
                    f"Could not determine continuous intervals for {len(failed_listings)} listings: {failed_listings}"
                )

            if not res:
                return pl.DataFrame(
                    schema={"ListingId": pl.Int64, "TimeBucket": pl.Datetime("ns")}
                ).lazy()

            r = pl.concat(res).sort([sc, "TimeBucket"])
            return r.lazy()
        else:
            raise ValueError()
