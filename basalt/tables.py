import polars as pl
import pandas as pd
from typing import List, Callable, Any
import os
from abc import ABC, abstractmethod
from .utils.schema_name_mapping import (
    resolve_schema_dataset,
    db_to_pascal_column,
    pascal_to_db_column,
)


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


def _filter_and_bucket(
    lf: pl.LazyFrame,
    *,
    ref: pl.DataFrame,
    nanoseconds: int,
    time_bucket_anchor: str,
    time_bucket_closed: str,
    timestamp_col: str,
    source_id_col: str | None = None,
    timestamp_aliases: list[str] | None = None,
) -> pl.LazyFrame:
    """Best-effort common transform for table loading."""
    timestamp_aliases = list(timestamp_aliases or [])
    cols = lf.collect_schema().names()
    out = lf

    if source_id_col and source_id_col in cols and "ListingId" not in cols:
        out = out.rename({source_id_col: "ListingId"})
        cols = out.collect_schema().names()

    if "ListingId" in cols and "ListingId" in ref.columns:
        out = out.filter(pl.col("ListingId").is_in(ref["ListingId"].to_list()))
    elif "Ticker" in cols and "Ticker" in ref.columns:
        out = out.filter(pl.col("Ticker").is_in(ref["Ticker"].to_list()))

    cols = out.collect_schema().names()
    if timestamp_col not in cols:
        for cand in timestamp_aliases:
            if cand in cols:
                out = out.with_columns(pl.col(cand).alias(timestamp_col))
                break

    cols = out.collect_schema().names()
    if timestamp_col not in cols:
        return out

    return out.with_columns(pl.col(timestamp_col).cast(pl.Datetime("ns"))).with_columns(
        TimeBucket=_timebucket_expr(
            pl.col(timestamp_col),
            nanoseconds,
            time_bucket_anchor,
            time_bucket_closed,
        ).cast(pl.Datetime("ns"))
    )


class DataTable(ABC):
    """Abstract base class for defining an input data table."""

    name: str
    timestamp_col: str
    bmll_table_name: str
    s3_folder_name: str
    s3_file_prefix: str
    source_id_col: str = "BMLLObjectId"
    snowflake_table: str | None = None
    databricks_table: str | None = None
    snowflake_column_overrides: dict[str, str] = {}
    databricks_column_overrides: dict[str, str] = {}

    @abstractmethod
    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        """Loads the table from the data source."""
        import bmll2

        from .api_stats import api_call

        return api_call(
            "bmll2.get_market_data_range",
            lambda: bmll2.get_market_data_range(
                markets=markets,
                table_name=self.bmll_table_name,
                df_engine="polars",
                start_date=start_date,
                end_date=end_date,
                lazy_load=True,
            ),
            extra={"table": self.bmll_table_name},
        )

    def schema_dataset_name(self) -> str:
        """
        Resolve schema dataset name used in `schemas/*.yml`.

        We try both BMLL table name and S3 folder name aliases.
        """
        return (
            resolve_schema_dataset(self.bmll_table_name)
            or resolve_schema_dataset(self.s3_folder_name)
            or self.bmll_table_name
        )

    def db_to_pascal_column(self, nat_name: str) -> str:
        """Map DB/native (NAT) column name to parquet/PascalCase column name."""
        return db_to_pascal_column(self.schema_dataset_name(), nat_name)

    def pascal_to_db_column(self, name: str) -> str:
        """Map parquet/PascalCase column name to DB/native (NAT) column name."""
        return pascal_to_db_column(self.schema_dataset_name(), name)

    def snowflake_table_name(self) -> str:
        """Snowflake table name for this table."""
        if self.snowflake_table:
            return self.snowflake_table
        return self.bmll_table_name.replace("-", "_").upper()

    def databricks_table_name(self) -> str:
        """Databricks table name for this table."""
        if self.databricks_table:
            return self.databricks_table
        return self.bmll_table_name.replace("-", "_").lower()

    def nat_to_source_column(self, source: str, nat_name: str) -> str:
        """
        Map NAT/DB column name to source-specific physical column name.
        """
        source = str(source).strip().lower()
        nat = str(nat_name).strip().upper()
        if source == "snowflake":
            return self.snowflake_column_overrides.get(nat, nat)
        if source == "databricks":
            return self.databricks_column_overrides.get(nat, nat)
        return nat

    def source_to_nat_column(self, source: str, source_column: str) -> str:
        """
        Map source-specific physical column name back to NAT/DB column name.
        """
        source = str(source).strip().lower()
        col = str(source_column).strip().upper()
        if source == "snowflake":
            inverse = {v.upper(): k for k, v in self.snowflake_column_overrides.items()}
            return inverse.get(col, col)
        if source == "databricks":
            inverse = {v.upper(): k for k, v in self.databricks_column_overrides.items()}
            return inverse.get(col, col)
        return col

    def load_from_source(
        self,
        source: str,
        *,
        markets: list[str],
        start_date,
        end_date,
        ref: pl.DataFrame,
        nanoseconds: int,
        time_bucket_anchor: str = "end",
        time_bucket_closed: str = "right",
    ) -> pl.LazyFrame:
        """
        Load the table from a source mechanism.

        Currently implemented:
        - `bmll`: BMLL API/S3-backed path

        Placeholders (fallback-compatible, pending schema wiring):
        - `snowflake`
        - `databricks`
        """
        source = str(source).lower().strip()
        if source == "bmll":
            lf = self.load(markets, start_date, end_date)
            return self.post_load_process(
                lf,
                ref,
                nanoseconds,
                time_bucket_anchor=time_bucket_anchor,
                time_bucket_closed=time_bucket_closed,
            )
        if source == "snowflake":
            raise NotImplementedError(
                "Snowflake table loading not wired yet. "
                f"Expected table mapping hint: PUBLIC.{self.snowflake_table_name()} "
                f"(from bmll table '{self.bmll_table_name}', s3 folder '{self.s3_folder_name}')."
            )
        if source == "databricks":
            raise NotImplementedError(
                "Databricks table loading not wired yet. "
                "Expected table mapping hint: "
                f"default_catalog.default_schema.{self.databricks_table_name()} "
                f"(from bmll table '{self.bmll_table_name}', s3 folder '{self.s3_folder_name}')."
            )
        raise ValueError(f"Unknown data source mechanism: {source}")

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
    snowflake_table = "TRADES_PLUS"
    databricks_table = "trades_plus"

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
            cols = lf_filtered.collect_schema().names()
            if "PreTradeMid" not in cols and "PreTradeMid1ms" in cols:
                lf_filtered = lf_filtered.with_columns(
                    PreTradeMid=pl.col("PreTradeMid1ms")
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
    snowflake_table = "L2_DATA_EQUITY"
    databricks_table = "l2_data_equity"

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
            cols = lf.collect_schema().names()
            if self.source_id_col in cols:
                lf_renamed = lf.rename({self.source_id_col: "ListingId"})
            else:
                lf_renamed = lf
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
    snowflake_table = "L3"
    databricks_table = "l3"

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


class CBBOTable(DataTable):
    """Represents the 'cbbo' data table (millisecond CBBO)."""

    name = "cbbo"
    bmll_table_name = "cbbo"
    timestamp_col = "EventTimestamp"
    s3_folder_name = "CBBO"
    s3_file_prefix = "cbbo-"
    source_id_col: str = "BMLLObjectId"
    snowflake_table = "CBBO_EQUITY"
    databricks_table = "cbbo_equity"

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
            cols = lf.collect_schema().names()
            if self.source_id_col in cols:
                lf_renamed = lf.rename({self.source_id_col: "ListingId"})
            else:
                lf_renamed = lf
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


class MarketStateTable(DataTable):
    """Represents the 'MarketState' data table."""

    name = "marketstate"
    bmll_table_name = "market-state"
    timestamp_col = "EventTimestamp"
    s3_folder_name = "MarketState"
    s3_file_prefix = "market-state-"
    snowflake_table = "MARKET_STATE"
    databricks_table = "market_state"

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


class ReferenceTable(DataTable):
    """Represents reference table metadata used for source mapping."""

    name = "reference"
    bmll_table_name = "reference"
    timestamp_col = "Date"
    s3_folder_name = "Reference"
    s3_file_prefix = "reference-"
    snowflake_table = "REFERENCE"
    databricks_table = "reference"
    databricks_column_overrides = {"REFERENCE_DATE": "DATE"}

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
            return lf.filter(pl.col("ListingId").is_in(ref["ListingId"].to_list()))

        return select_and_resample


class ImbalanceTable(DataTable):
    """Represents imbalance feed table."""

    name = "imbalance"
    bmll_table_name = "imbalance"
    timestamp_col = "EventTimestamp"
    s3_folder_name = "Imbalance"
    s3_file_prefix = "imbalance-"
    snowflake_table = "IMBALANCE"
    databricks_table = "imbalance"

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
            return _filter_and_bucket(
                lf,
                ref=ref,
                nanoseconds=nanoseconds,
                time_bucket_anchor=time_bucket_anchor,
                time_bucket_closed=time_bucket_closed,
                timestamp_col=self.timestamp_col,
                timestamp_aliases=["TimestampNanoseconds", "Timestamp", "LocalTimestamp"],
            )

        return select_and_resample


class NBBOTable(DataTable):
    """Represents NBBO table."""

    name = "nbbo"
    bmll_table_name = "nbbo"
    timestamp_col = "EventTimestamp"
    s3_folder_name = "NBBO"
    s3_file_prefix = "nbbo-"
    source_id_col = "BMLLObjectId"
    snowflake_table = "NBBO"
    databricks_table = "nbbo"

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
            return _filter_and_bucket(
                lf,
                ref=ref,
                nanoseconds=nanoseconds,
                time_bucket_anchor=time_bucket_anchor,
                time_bucket_closed=time_bucket_closed,
                timestamp_col=self.timestamp_col,
                source_id_col=self.source_id_col,
                timestamp_aliases=[
                    "TimestampNanoseconds",
                    "ExchangeTimestampNanoseconds",
                    "ExchangeLocalTimestampNanoseconds",
                    "ExchangeTimestamp",
                ],
            )

        return select_and_resample


class FutureL1Table(DataTable):
    name = "future_l1"
    bmll_table_name = "future_l1"
    timestamp_col = "EventTimestamp"
    s3_folder_name = "Future-L1"
    s3_file_prefix = "future-l1-"
    snowflake_table = "FUTURE_L1"
    databricks_table = "future_l1"

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        return super().load(markets, start_date, end_date)

    def get_transform_fn(self, ref, nanoseconds, time_bucket_anchor="end", time_bucket_closed="right"):
        def select_and_resample(lf: pl.LazyFrame) -> pl.LazyFrame:
            return _filter_and_bucket(
                lf,
                ref=ref,
                nanoseconds=nanoseconds,
                time_bucket_anchor=time_bucket_anchor,
                time_bucket_closed=time_bucket_closed,
                timestamp_col=self.timestamp_col,
                timestamp_aliases=["TimestampNanoseconds", "Timestamp", "LocalTimestamp"],
            )

        return select_and_resample


class FutureMarketStateTable(DataTable):
    name = "future_market_state"
    bmll_table_name = "future_market_state"
    timestamp_col = "EventTimestamp"
    s3_folder_name = "Future-MarketState"
    s3_file_prefix = "future-market-state-"
    snowflake_table = "FUTURE_MARKET_STATE"
    databricks_table = "future_market_state"

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        return super().load(markets, start_date, end_date)

    def get_transform_fn(self, ref, nanoseconds, time_bucket_anchor="end", time_bucket_closed="right"):
        def select_and_resample(lf: pl.LazyFrame) -> pl.LazyFrame:
            return _filter_and_bucket(
                lf,
                ref=ref,
                nanoseconds=nanoseconds,
                time_bucket_anchor=time_bucket_anchor,
                time_bucket_closed=time_bucket_closed,
                timestamp_col=self.timestamp_col,
                timestamp_aliases=["TimestampNanoseconds", "Timestamp"],
            )

        return select_and_resample


class FutureReferenceTable(DataTable):
    name = "future_reference"
    bmll_table_name = "future_reference"
    timestamp_col = "Date"
    s3_folder_name = "Future-Reference"
    s3_file_prefix = "future-reference-"
    snowflake_table = "FUTURE_REFERENCE"
    databricks_table = "future_reference"
    databricks_column_overrides = {"REFERENCE_DATE": "DATE"}

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        return super().load(markets, start_date, end_date)

    def get_transform_fn(self, ref, nanoseconds, time_bucket_anchor="end", time_bucket_closed="right"):
        def select_and_resample(lf: pl.LazyFrame) -> pl.LazyFrame:
            return _filter_and_bucket(
                lf,
                ref=ref,
                nanoseconds=nanoseconds,
                time_bucket_anchor=time_bucket_anchor,
                time_bucket_closed=time_bucket_closed,
                timestamp_col=self.timestamp_col,
                timestamp_aliases=["ReferenceDate"],
            )

        return select_and_resample


class FutureStatisticsTable(DataTable):
    name = "future_statistics"
    bmll_table_name = "future_statistics"
    timestamp_col = "Timestamp"
    s3_folder_name = "Future-Statistics"
    s3_file_prefix = "future-statistics-"
    snowflake_table = "FUTURE_STATISTICS"
    databricks_table = "future_statistics"

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        return super().load(markets, start_date, end_date)

    def get_transform_fn(self, ref, nanoseconds, time_bucket_anchor="end", time_bucket_closed="right"):
        def select_and_resample(lf: pl.LazyFrame) -> pl.LazyFrame:
            return _filter_and_bucket(
                lf,
                ref=ref,
                nanoseconds=nanoseconds,
                time_bucket_anchor=time_bucket_anchor,
                time_bucket_closed=time_bucket_closed,
                timestamp_col=self.timestamp_col,
                timestamp_aliases=["EventTimestamp", "TimestampNanoseconds"],
            )

        return select_and_resample


class FutureTradesTable(DataTable):
    name = "future_trades"
    bmll_table_name = "future_trades"
    timestamp_col = "TradeTimestamp"
    s3_folder_name = "Future-Trades"
    s3_file_prefix = "future-trades-"
    snowflake_table = "FUTURE_TRADES"
    databricks_table = "future_trades"

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        return super().load(markets, start_date, end_date)

    def get_transform_fn(self, ref, nanoseconds, time_bucket_anchor="end", time_bucket_closed="right"):
        def select_and_resample(lf: pl.LazyFrame) -> pl.LazyFrame:
            return _filter_and_bucket(
                lf,
                ref=ref,
                nanoseconds=nanoseconds,
                time_bucket_anchor=time_bucket_anchor,
                time_bucket_closed=time_bucket_closed,
                timestamp_col=self.timestamp_col,
                timestamp_aliases=["TradeTimestampNanoseconds", "PublicationTimestamp"],
            )

        return select_and_resample


class OpraReferenceTable(DataTable):
    name = "opra_reference"
    bmll_table_name = "opra_reference"
    timestamp_col = "Date"
    s3_folder_name = "OPRA-Reference"
    s3_file_prefix = "opra-reference-"
    snowflake_table = "OPRA_REFERENCE"
    databricks_table = "opra_reference"

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        return super().load(markets, start_date, end_date)

    def get_transform_fn(self, ref, nanoseconds, time_bucket_anchor="end", time_bucket_closed="right"):
        def select_and_resample(lf: pl.LazyFrame) -> pl.LazyFrame:
            return _filter_and_bucket(
                lf,
                ref=ref,
                nanoseconds=nanoseconds,
                time_bucket_anchor=time_bucket_anchor,
                time_bucket_closed=time_bucket_closed,
                timestamp_col=self.timestamp_col,
                timestamp_aliases=["ExpiryDate"],
            )

        return select_and_resample


class OpraStatisticsTable(DataTable):
    name = "opra_statistics"
    bmll_table_name = "opra_statistics"
    timestamp_col = "Date"
    s3_folder_name = "OPRA-Statistics"
    s3_file_prefix = "opra-statistics-"
    snowflake_table = "OPRA_STATISTICS"
    databricks_table = "opra_statistics"

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        return super().load(markets, start_date, end_date)

    def get_transform_fn(self, ref, nanoseconds, time_bucket_anchor="end", time_bucket_closed="right"):
        def select_and_resample(lf: pl.LazyFrame) -> pl.LazyFrame:
            return _filter_and_bucket(
                lf,
                ref=ref,
                nanoseconds=nanoseconds,
                time_bucket_anchor=time_bucket_anchor,
                time_bucket_closed=time_bucket_closed,
                timestamp_col=self.timestamp_col,
                timestamp_aliases=["ExpiryDate"],
            )

        return select_and_resample


class OpraTradesTable(DataTable):
    name = "opra_trades"
    bmll_table_name = "opra_trades"
    timestamp_col = "EventTimestamp"
    s3_folder_name = "OPRA-Trades"
    s3_file_prefix = "opra-trades-"
    snowflake_table = "OPRA_TRADES"
    databricks_table = "opra_trades"

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        return super().load(markets, start_date, end_date)

    def get_transform_fn(self, ref, nanoseconds, time_bucket_anchor="end", time_bucket_closed="right"):
        def select_and_resample(lf: pl.LazyFrame) -> pl.LazyFrame:
            return _filter_and_bucket(
                lf,
                ref=ref,
                nanoseconds=nanoseconds,
                time_bucket_anchor=time_bucket_anchor,
                time_bucket_closed=time_bucket_closed,
                timestamp_col=self.timestamp_col,
                timestamp_aliases=["LocalTimestamp", "Date"],
            )

        return select_and_resample


ALL_TABLES = {
    "trades": TradesPlusTable(),
    "l2": L2Table(),
    "l3": L3Table(),
    "cbbo": CBBOTable(),
    "marketstate": MarketStateTable(),
    "reference": ReferenceTable(),
    "imbalance": ImbalanceTable(),
    "nbbo": NBBOTable(),
    "future_l1": FutureL1Table(),
    "future_market_state": FutureMarketStateTable(),
    "future_reference": FutureReferenceTable(),
    "future_statistics": FutureStatisticsTable(),
    "future_trades": FutureTradesTable(),
    "opra_reference": OpraReferenceTable(),
    "opra_statistics": OpraStatisticsTable(),
    "opra_trades": OpraTradesTable(),
}
