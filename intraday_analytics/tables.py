from abc import ABC, abstractmethod
import polars as pl


class DataTable(ABC):
    """Abstract base class for defining an input data table."""

    name: str
    timestamp_col: str
    bmll_table_name: str
    s3_folder_name: str
    s3_file_prefix: str

    @abstractmethod
    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        """Loads the table from the data source."""
        import bmll2 # Moved import inside function
        return bmll2.get_market_data_range(
            markets=markets,
            table_name=self.bmll_table_name,
            df_engine="polars",
            start_date=start_date,
            end_date=end_date,
            lazy_load=True,
        )

    def post_load_process(
        self, lf: pl.LazyFrame, ref: pl.DataFrame, nanoseconds: int
    ) -> pl.LazyFrame:
        """Applies post-loading transformations, like resampling."""
        return lf.filter(
            pl.col("ListingId").is_in(ref["ListingId"].to_list())
        ).with_columns(
            TimeBucket=pl.when(
                pl.col(self.timestamp_col)
                == pl.col(self.timestamp_col).dt.truncate(f"{nanoseconds}ns")
            )
            .then(pl.col(self.timestamp_col))
            .otherwise(pl.col(self.timestamp_col))
            .dt.truncate(f"{nanoseconds}ns")
            + pl.duration(nanoseconds=nanoseconds)
        )


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
            LPrice=pl.col("TradeNotional") / pl.col("Size"),
            EPrice=pl.col("TradeNotionalEUR") / pl.col("Size"),
        )


class L2Table(DataTable):
    """Represents the 'l2' data table."""

    name = "l2"
    bmll_table_name = "l2"
    timestamp_col = "EventTimestamp"
    s3_folder_name = "Equity"
    s3_file_prefix = ""

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        return super().load(markets, start_date, end_date)


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


class MarketStateTable(DataTable):
    """Represents the 'MarketState' data table."""

    name = "marketstate"
    bmll_table_name = "MarketState"
    timestamp_col = "EventTimestamp"
    s3_folder_name = "MarketState"
    s3_file_prefix = "market-state-"

    def load(self, markets: list[str], start_date, end_date) -> pl.LazyFrame:
        return super().load(markets, start_date, end_date)


ALL_TABLES = {
    "trades": TradesPlusTable(),
    "l2": L2Table(),
    "l3": L3Table(),
    "marketstate": MarketStateTable(),
}
