import polars as pl
import pandas as pd
import datetime as dt
import logging
from pydantic import BaseModel, Field
from typing import List
import polars as pl

from intraday_analytics.analytics_base import BaseAnalytics
from intraday_analytics.analytics_registry import register_analytics
from intraday_analytics.utils import SYMBOL_COL
from pydantic import BaseModel, Field
from typing import List, Optional, Literal

logger = logging.getLogger(__name__)


class DenseAnalyticsConfig(BaseModel):
    """
    Configuration for the dense time-grid pass.

    Controls how a uniform or adaptive time grid is generated for each symbol.
    This pass does not compute analytics itself; it provides time buckets that
    downstream analytics can align to. Use this to control bucket size, anchor,
    and optional symbol column overrides.
    """

    ENABLED: bool = True
    metric_prefix: Optional[str] = None
    mode: Literal["adaptative", "uniform"] = "adaptative"
    time_interval: Optional[List[str]] = None
    time_bucket_seconds: Optional[float] = None
    time_bucket_anchor: Literal["end", "start"] = "end"
    time_bucket_closed: Literal["right", "left"] = "right"
    symbol_cols: Optional[List[str]] = None


@register_analytics("dense", config_attr="dense_analytics", needs_ref=True)
class DenseAnalytics(BaseAnalytics):
    """
    Generates a dense time grid.
    """

    REQUIRES = ["trades", "marketstate"]

    def __init__(self, ref: pl.DataFrame, config: DenseAnalyticsConfig):
        self.ref = ref
        self.config = config
        super().__init__("da", metric_prefix=config.metric_prefix)

    def compute(self) -> pl.LazyFrame:
        # (Keep original logic as it's structural, not metric-generating in the same sense)
        scs = self.config.symbol_cols or [SYMBOL_COL]
        symbols = self.ref.select(scs).unique()

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

            ref_cols = [
                c for c in ["MIC", "Ticker", "CurrencyCode"] if c in self.ref.columns
            ]
            ref_sel = self.ref.select(scs + ref_cols).unique(subset=scs).lazy()

            return (
                pl.LazyFrame(symbols)
                .join(pl.LazyFrame(times_df), how="cross")
                .join(ref_sel, on=scs, how="left")
                .sort(scs + ["TimeBucket"])
            )
        elif self.config.mode == "adaptative":
            tsc = "EventTimestamp"
            if "ListingId" in scs:
                available_listings = (
                    self.marketstate.select("ListingId").unique().collect()
                )
                symbols = symbols.join(available_listings, on="ListingId", how="inner")

            calendar = self.marketstate
            if "ListingId" not in scs:
                if "ListingId" not in self.ref.columns:
                    raise ValueError("DenseAnalytics requires ListingId in ref.")
                calendar = calendar.join(
                    self.ref.select(["ListingId"] + scs), on="ListingId", how="left"
                )

            if "ListingId" in symbols.columns:
                calendar = calendar.filter(
                    pl.col("ListingId").is_in(list(symbols["ListingId"]))
                )
            continuous_intervals = (
                calendar.with_columns(
                    [
                        pl.col("MarketState").shift(1).over(scs).alias("pstate"),
                        pl.col("MarketState").shift(-1).over(scs).alias("nstate"),
                        pl.col(tsc).shift(-1).over(scs).alias("nts"),
                    ]
                )
                .filter(pl.col("MarketState") == "CONTINUOUS_TRADING")
                .filter(
                    (pl.col("pstate") != "CONTINUOUS_TRADING")
                    | (pl.col("nstate") != "CONTINUOUS_TRADING")
                    | (pl.col("pstate").is_null())
                    | (pl.col("nstate").is_null())
                )
                .select(scs + ["MarketState", tsc, "nts"])
                .collect()
                .to_pandas()
            )
            continuous_intervals = continuous_intervals.dropna(subset=[tsc, "nts"])
            res = []
            failed_listings = []
            frequency = str(int(self.config.time_bucket_seconds * 1e9)) + "ns"
            TS_PADDING = pd.Timedelta(seconds=600)
            sym_rows = symbols.iter_rows(named=True)
            for row in sym_rows:
                mask = True
                for c in scs:
                    mask = mask & (continuous_intervals[c] == row[c])
                ciq = continuous_intervals.loc[mask, tsc]
                if len(ciq) == 0:
                    failed_listings.append(row)
                    continue
                try:
                    start_dt = (
                        pd.Timestamp(ciq.iloc[0]).floor(str(frequency)) - TS_PADDING
                    )
                    end_dt = (
                        pd.Timestamp(
                            continuous_intervals.loc[mask, "nts"].iloc[-1]
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
                        ).with_columns(**{c: row[c] for c in scs})
                    )
                except (TypeError, ValueError) as exc:
                    failed_listings.append({**row, "error": str(exc)})
                    continue

            if failed_listings:
                logger.warning(
                    f"Could not determine continuous intervals for {len(failed_listings)} listings: {failed_listings}"
                )

            if not res:
                return pl.DataFrame(
                    schema={
                        **{c: pl.Int64 for c in scs},
                        "TimeBucket": pl.Datetime("ns"),
                    }
                ).lazy()

            r = pl.concat(res).sort(scs + ["TimeBucket"])
            return r.lazy()
        else:
            raise ValueError()
