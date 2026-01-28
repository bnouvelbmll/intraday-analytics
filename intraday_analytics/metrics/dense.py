import polars as pl
import pandas as pd
import datetime as dt
from intraday_analytics import BaseAnalytics, SYMBOL_COL


class DenseAnalytics(BaseAnalytics):
    """
    Generates a dense time grid for each symbol to ensure that all metrics are
    populated for all time intervals, even if there is no activity.

    This class supports two modes for generating the time grid:
    - `uniform`: Creates a uniform time grid between a specified start and end
      time for a single date. The date is determined by the most frequent
      `TradeDate` in the trades data.
    - `adaptive`: Creates a time grid based on the continuous trading
      intervals of the market state. This mode adapts the time grid to the
      actual trading periods of each symbol.
    """

    REQUIRES = ["trades"]  # Use trades plus to know date

    def __init__(self, ref: pl.DataFrame, config: dict):
        self.ref = ref
        self.config = config
        super().__init__("da")

    def compute(self) -> pl.LazyFrame:
        sc = SYMBOL_COL
        symbols = pl.DataFrame({sc: list(sorted(self.ref[sc].unique()))})

        if self.config["DENSE_OUTPUT_MODE"] == "uniform":
            dates = (
                self.trades.select("TradeDate").collect()["TradeDate"].value_counts()
            )
            date = dates.sort("count")[-1]["TradeDate"][0]

            start_time = str(self.config["DENSE_OUTPUT_TIME_INTERVAL"][0])
            end_time = str(self.config["DENSE_OUTPUT_TIME_INTERVAL"][1])
            start_time = pd.Timestamp("1971-01-01 " + start_time).time()
            end_time = pd.Timestamp("1971-01-01 " + end_time).time()

            start_dt = dt.datetime(
                date.year, date.month, date.day, start_time.hour, start_time.minute
            )

            end_dt = dt.datetime(
                date.year, date.month, date.day, end_time.hour, end_time.minute
            )

            frequency = str(int(self.config["TIME_BUCKET_SECONDS"] * 1e9)) + "ns"

            times_df = pl.DataFrame(
                {
                    "TimeBucket": pl.datetime_range(
                        start=start_dt,
                        end=end_dt,
                        interval=frequency,
                        closed="left",  # common choice; avoids including end_time
                        eager=True,
                    )
                }
            )

            return (
                pl.LazyFrame(symbols.cast(pl.Int64))
                .join(pl.LazyFrame(times_df), how="cross")
                .set_sorted(sc, "TimeBucket")
            )
        elif self.config["DENSE_OUTPUT_MODE"] == "adaptative":
            tsc = "EventTimestamp"
            calendar = self.marketstate.filter(
                pl.col("ListingId").is_in(list(symbols[sc]))
            )
            continuous_intervals = (
                calendar.with_columns(
                    pstate=pl.col("MarketState").shift(1),
                    nstate=pl.col("MarketState").shift(-1),
                )
                .with_columns(
                    plid=pl.col("ListingId").shift(1),
                    nlid=pl.col("ListingId").shift(-1),
                    nts=pl.col(tsc).shift(-1),
                )
                .filter(pl.col("MarketState") == "CONTINUOUS_TRADING")
                .filter(
                    (pl.col("pstate") != "CONTINUOUS_TRADING")
                    | (pl.col("nstate") != "CONTINUOUS_TRADING")
                )
                .filter(pl.col("plid") == pl.col("nlid"))
                .select(["ListingId", "MarketState", tsc, "nts"])
                .collect()
                .to_pandas()
            )
            # continuous_intervals = continuous_intervals.groupby(["ListingId"])
            # continuous_intervals = continuous_intervals .set_index("ListingId")
            res = []
            frequency = str(int(self.config["TIME_BUCKET_SECONDS"] * 1e9)) + "ns"
            TS_PADDING = pd.Timedelta(seconds=600)
            for s in symbols[sc]:
                try:
                    start_dt = (
                        pd.Timestamp(
                            continuous_intervals.query("ListingId==@s")[tsc].iloc[0]
                        ).floor(str(frequency))
                        - TS_PADDING
                    )
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
                                    closed="left",  # common choice; avoids including end_time
                                    eager=True,
                                ).cast(pl.Datetime("ns"))
                            }
                        ).with_columns(ListingId=s)
                    )
                except Exception as e:
                    print(s, e)
            r = pl.concat(res).set_sorted([sc, "TimeBucket"])
            return r.lazy()
        else:
            raise ValueError()
