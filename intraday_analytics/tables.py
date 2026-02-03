import polars as pl
import pandas as pd
from typing import List, Callable, Any
import os
from abc import ABC, abstractmethod


def _timebucket_expr(
    ts_col: pl.Expr,
    nanoseconds: int,
    anchor: str,
    closed: str,
) -> pl.Expr:
    trunc = ts_col.dt.truncate(f"{nanoseconds}ns")
    dur = pl.duration(nanoseconds=nanoseconds)
    if anchor == "end":
        if closed == "right":
            return pl.when(ts_col == trunc).then(ts_col).otherwise(trunc + dur)
        return trunc + dur
    if closed == "left":
        return trunc
    return pl.when(ts_col == trunc).then(trunc - dur).otherwise(trunc)


class DataTable(ABC):
    """Abstract base class for defining an input data table."""

    name: str
    timestamp_col: str
    bmll_table_name: str
    s3_folder_name: str
    s3_file_prefix: str
    source_id_col: str = "BMLLObjectId"

    @abstractmethod
    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        """Loads the table from the data source."""
        import bmll2

        return bmll2.get_market_data_range(
            markets=markets,
            table_name=self.bmll_table_name,
            df_engine="polars",
            start_date=start_date,
            end_date=end_date,
            lazy_load=True,
        )

    def post_load_process(
        self,
        lf: pl.LazyFrame,
        ref: pl.DataFrame,
        nanoseconds: int,
        time_bucket_anchor: str = "end",
        time_bucket_closed: str = "right",
    ) -> pl.LazyFrame:
        """Applies post-loading transformations, like resampling."""
        return (
            lf.filter(pl.col("ListingId").is_in(ref["ListingId"].to_list()))
            .with_columns(pl.col(self.timestamp_col).cast(pl.Datetime("ns")))
            .with_columns(
                TimeBucket=_timebucket_expr(
                    pl.col(self.timestamp_col),
                    nanoseconds,
                    time_bucket_anchor,
                    time_bucket_closed,
                ).cast(pl.Datetime("ns"))
            )
        )

    def get_s3_paths(
        self, mics: List[str], year: int, month: int, day: int
    ) -> List[str]:
        """
        Generates S3 paths for this table.
        """
        import bmll2

        ap = bmll2._configure.L2_ACCESS_POINT_ALIAS
        yyyy = "%04d" % (year,)
        mm = "%02d" % (month,)
        dd = "%02d" % (day,)

        return [
            f"s3://{ap}/{self.s3_folder_name}/{mic}/{yyyy}/{mm}/{dd}/{self.s3_file_prefix}{mic}-{yyyy}{mm}{dd}.parquet"
            for mic in mics
        ]

    @abstractmethod
    def get_transform_fn(
        self,
        ref: pl.DataFrame,
        nanoseconds: int,
        time_bucket_anchor: str = "end",
        time_bucket_closed: str = "right",
    ) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
        """
        Returns a transformation function for this table, including filtering and resampling.
        Subclasses must implement this method to define their specific transformation logic.
        """
        pass


class TradesPlusTable(DataTable):
    """Represents the 'trades-plus' data table."""

    name = "trades"
    bmll_table_name = "trades-plus"
    timestamp_col = "TradeTimestamp"
    s3_folder_name = "Trades-Plus"
    s3_file_prefix = "trades-plus-"

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        """Loads trades data and adds calculated price columns."""
        lf = super().load(markets, start_date, end_date)
        return lf.with_columns(
            LocalPrice=pl.col("TradeNotional") / pl.col("Size"),
            PriceEUR=pl.col("TradeNotionalEUR") / pl.col("Size"),
        )

    def get_transform_fn(
        self,
        ref: pl.DataFrame,
        nanoseconds: int,
        time_bucket_anchor: str = "end",
        time_bucket_closed: str = "right",
    ) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
        def select_and_resample(lf: pl.LazyFrame) -> pl.LazyFrame:
            lf_filtered = lf.filter(
                pl.col("ListingId").is_in(ref["ListingId"].to_list())
            )
            lf_filtered = lf_filtered.with_columns(
                LocalPrice=pl.col("TradeNotional") / pl.col("Size"),
                PriceEUR=pl.col("TradeNotionalEUR") / pl.col("Size"),
                TradeTimestamp=pl.col(self.timestamp_col).cast(pl.Datetime("ns")),
            )
            return lf_filtered.with_columns(
                TimeBucket=_timebucket_expr(
                    pl.col(self.timestamp_col),
                    nanoseconds,
                    time_bucket_anchor,
                    time_bucket_closed,
                ).cast(pl.Datetime("ns"))
            )

        return select_and_resample


class L2Table(DataTable):
    """Represents the 'l2' data table."""

    name = "l2"
    bmll_table_name = "l2"
    timestamp_col = "EventTimestamp"
    s3_folder_name = "Equity"
    s3_file_prefix = ""
    source_id_col: str = "BMLLObjectId"

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        return super().load(markets, start_date, end_date)

    def get_transform_fn(
        self,
        ref: pl.DataFrame,
        nanoseconds: int,
        time_bucket_anchor: str = "end",
        time_bucket_closed: str = "right",
    ) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
        def select_and_resample(lf: pl.LazyFrame) -> pl.LazyFrame:
            lf_renamed = lf.rename({self.source_id_col: "ListingId"})
            lf_filtered = lf_renamed.filter(
                pl.col("ListingId").is_in(ref["ListingId"].to_list())
            )
            return lf_filtered.with_columns(
                TimeBucket=_timebucket_expr(
                    pl.col(self.timestamp_col),
                    nanoseconds,
                    time_bucket_anchor,
                    time_bucket_closed,
                ).cast(pl.Datetime("ns"))
            )

        return select_and_resample


class L3Table(DataTable):
    """Represents the 'l3' data table."""

    name = "l3"
    bmll_table_name = "l3"
    timestamp_col = "EventTimestamp"
    s3_folder_name = "Equity-L3"
    s3_file_prefix = "l3-"

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        """Loads L3 data and creates the 'EventTimestamp' alias."""
        lf = super().load(markets, start_date, end_date)
        return lf.with_columns(pl.col("TimestampNanoseconds").alias("EventTimestamp"))

    def get_transform_fn(
        self,
        ref: pl.DataFrame,
        nanoseconds: int,
        time_bucket_anchor: str = "end",
        time_bucket_closed: str = "right",
    ) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
        def select_and_resample(lf: pl.LazyFrame) -> pl.LazyFrame:
            lf_filtered = lf.filter(
                pl.col("ListingId").is_in(ref["ListingId"].to_list())
            )
            lf_filtered = lf_filtered.select(
                [
                    "TimestampNanoseconds",
                    "ListingId",
                    "LobAction",
                    "Price",
                    "Size",
                    "OldSize",
                    "OldPrice",
                    "Side",
                    "ExecutionSize",
                    "ExecutionPrice",
                ]
            ).with_columns(
                EventTimestamp=pl.col("TimestampNanoseconds").cast(pl.Datetime("ns"))
            )
            return lf_filtered.with_columns(
                TimeBucket=_timebucket_expr(
                    pl.col(self.timestamp_col),
                    nanoseconds,
                    time_bucket_anchor,
                    time_bucket_closed,
                ).cast(pl.Datetime("ns"))
            )

        return select_and_resample


class MarketStateTable(DataTable):
    """Represents the 'MarketState' data table."""

    name = "marketstate"
    bmll_table_name = "MarketState"
    timestamp_col = "EventTimestamp"
    s3_folder_name = "MarketState"
    s3_file_prefix = "market-state-"

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        return super().load(markets, start_date, end_date)

    def get_transform_fn(
        self,
        ref: pl.DataFrame,
        nanoseconds: int,
        time_bucket_anchor: str = "end",
        time_bucket_closed: str = "right",
    ) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
        def select_and_resample(lf: pl.LazyFrame) -> pl.LazyFrame:
            lf_filtered = lf.filter(
                pl.col("ListingId").is_in(ref["ListingId"].to_list())
            )
            lf_filtered = lf_filtered.with_columns(
                EventTimestamp=pl.col("TimestampNanoseconds").cast(pl.Datetime("ns"))
            )
            return lf_filtered.with_columns(
                TimeBucket=_timebucket_expr(
                    pl.col(self.timestamp_col),
                    nanoseconds,
                    time_bucket_anchor,
                    time_bucket_closed,
                ).cast(pl.Datetime("ns"))
            )

        return select_and_resample


ALL_TABLES = {
    "trades": TradesPlusTable(),
    "l2": L2Table(),
    "l3": L3Table(),
    "marketstate": MarketStateTable(),
}
