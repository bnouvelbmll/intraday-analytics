import os
import re
import sys
import glob
import bmll
import bmll2
import polars as pl
import pandas as pd
import pyarrow.parquet as pq
from typing import List, Callable

# small constants
SYMBOL_COL = "ListingId"


def lprint(*args, **kwargs):
    """
    A logging function that prefixes messages with the current timestamp.
    """
    print(pd.Timestamp("today").isoformat(), *args, **kwargs)


def dc(lf, suffix, timebucket_col="TimeBucket"):
    """
    Drops duplicate columns after a join operation.

    When joining DataFrames, columns with the same name in both frames are
    renamed with a suffix. This function identifies such columns, fills null
    values in the original column with values from the suffixed column, and
    then drops the suffixed column.

    Args:
        lf: The DataFrame to process.
        suffix: The suffix that was added to the duplicate columns.
        timebucket_col: The name of the time bucket column.

    Returns:
        A DataFrame with the duplicate columns removed.
    """
    cc = ["ListingId", timebucket_col, "MIC", "Ticker"]
    ccc = lf.collect_schema().names()
    ccxv = [ccx for ccx in cc if (ccx + suffix) in ccc]
    return (
        lf.with_columns(
            *[pl.col(ccx).fill_null(pl.col(ccx + suffix)) for ccx in ccxv],
        )
        .drop([c + suffix for c in ccxv])
        .sort(SYMBOL_COL, timebucket_col)
    )  # ensure we remain sorted


def get_total_system_memory_gb():
    """
    Retrieves the total system physical memory in GiB.

    This function attempts to get the system memory using `os.sysconf` on
    Linux and macOS. As a fallback on Linux, it parses `/proc/meminfo`.

    Returns:
        The total system memory in GiB.

    Raises:
        NotImplementedError: If the memory cannot be determined on the current
                             operating system.
    """
    if sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
        try:
            # 1. Get page size in bytes (SC_PAGE_SIZE)
            page_size_bytes = os.sysconf("SC_PAGE_SIZE")

            # 2. Get total number of physical pages (SC_PHYS_PAGES)
            num_phys_pages = os.sysconf("SC_PHYS_PAGES")

            # 3. Calculate total memory in bytes
            mem_total_bytes = page_size_bytes * num_phys_pages

            # 4. Convert bytes to Gigabytes (GiB)
            mem_total_gb = mem_total_bytes / (1024**3)
            return mem_total_gb

        except ValueError:
            # sysconf key might not be available
            pass

    # Fallback to the /proc/meminfo parsing method (Linux only)
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/meminfo", "r") as f:
                meminfo = f.read()

            match = re.search(r"^MemTotal:\s+(\d+)\s+kB", meminfo)
            if match:
                mem_total_kb = int(match.groups()[0])
                mem_total_gb = mem_total_kb / (1024**2)
                return mem_total_gb
        except (FileNotFoundError, OSError, AttributeError):
            pass

    raise NotImplementedError


def preload(sd, ed, ref, nanoseconds):
    """
    Preloads data from the data lake for a given date range and reference data.

    This function constructs queries to fetch trades, L2, and L3 data from the
    data lake, applies initial filtering and resampling, and returns LazyFrames
    for further processing.

    Args:
        sd: The start date of the data to load.
        ed: The end date of the data to load.
        ref: A DataFrame containing the reference data (e.g., symbols).
        nanoseconds: The time bucket size in nanoseconds.

    Returns:
        A tuple of LazyFrames for trades, L2, and L3 data.
    """
    def resample_and_select(lf, timestamp_col="EventTimestamp"):
        return lf.filter(
            pl.col("ListingId").is_in(ref["ListingId"].to_list())
        ).with_columns(
            TimeBucket=pl.when(
                pl.col(timestamp_col)
                == pl.col(timestamp_col).dt.truncate(f"{nanoseconds}ns")
            )
            .then(pl.col(timestamp_col))
            .otherwise(pl.col(timestamp_col))
            .dt.truncate(f"{nanoseconds}ns")
            + pl.duration(nanoseconds=nanoseconds)
        )

    print(sd, ed, nanoseconds)

    trades = resample_and_select(
        bmll2.get_market_data_range(
            markets=ref["MIC"].unique().tolist(),
            table_name="trades-plus",
            df_engine="polars",
            start_date=sd,
            end_date=ed,
            lazy_load=True,
        ).with_columns(
            LPrice=pl.col("TradeNotional") / pl.col("Size"),
            EPrice=pl.col("TradeNotionalEUR") / pl.col("Size"),
        ),
        timestamp_col="TradeTimestamp",
    )

    # Pull the L2
    l2 = resample_and_select(
        bmll2.get_market_data_range(
            markets=ref["MIC"].unique().tolist(),
            table_name="l2",
            df_engine="polars",
            start_date=sd,
            end_date=ed,
            lazy_load=True,
        )
    )

    l3 = resample_and_select(
        bmll2.get_market_data_range(
            markets=ref["MIC"].unique().tolist(),
            table_name="l3",
            df_engine="polars",
            start_date=sd,
            end_date=ed,
            lazy_load=True,
        ).with_columns(pl.col("TimestampNanoseconds").alias("EventTimestamp"))
    )
    return trades, l2, l3


# helper: forward fill with shifts (exact original algorithm)
def ffill_with_shifts(
    df: pl.LazyFrame,
    group_cols: list[str],
    time_col: str,
    value_cols: list[str],
    shifts: List[int | float],
    round_op: Callable = None,
) -> pl.LazyFrame:
    """
    Forward-fills values in a grouped DataFrame after expanding the timestamps.

    This function is used to create a dense time series by introducing new
    timestamps (based on the provided shifts) and then forward-filling the
    values to populate the new rows.

    Args:
        df: The input LazyFrame.
        group_cols: The columns to group by.
        time_col: The timestamp column.
        value_cols: The columns to forward-fill.
        shifts: A list of time shifts to apply to the timestamps.
        round_op: An optional function to round the timestamps.

    Returns:
        A LazyFrame with the expanded and forward-filled data.
    """
    if round_op is None:
        round_op = lambda x: x

    # 1. Compute shifted timestamps
    df_shifted = pl.concat(
        [
            df.select(
                group_cols + [(round_op(pl.col(time_col) + shift)).alias(time_col)]
            )
            for shift in shifts
        ]
    ).unique()

    # 2. Combine original and shifted timestamps
    df_expanded = (
        pl.concat([df.select(group_cols + [time_col]), df_shifted])
        .unique(subset=group_cols + [time_col])
        .sort(group_cols + [time_col])
    )

    # 3. Join original values
    result = df_expanded.join(df, on=group_cols + [time_col], how="left")

    # 4. Forward-fill missing values within groups
    for col in value_cols:
        result = result.with_columns(pl.col(col).forward_fill().over(group_cols))

    return result


def assert_unique_lazy(lf: pl.LazyFrame, keys: list[str], name=""):
    """
    Asserts that the keys in a LazyFrame are unique.

    This function uses `map_batches` to perform the uniqueness check on chunks
    of the data as it is being processed. This is more memory-efficient than
    collecting the entire DataFrame first.

    Args:
        lf: The LazyFrame to check.
        keys: The list of key columns that should be unique.
        name: An optional name for the check, used in the error message.

    Returns:
        The original LazyFrame, if the check passes.

    Raises:
        AssertionError: If duplicate keys are found.
    """
    def check_uniqueness(df: pl.DataFrame) -> pl.DataFrame:
        # Perform the actual check on the materialized chunk
        is_duplicated = df.select(keys).is_duplicated().any()
        if is_duplicated:
            # You can also print which keys are duplicated here for debugging
            dupes = df.filter(df.select(keys).is_duplicated()).head(5)
            raise AssertionError(f"Duplicate keys found in {name}: {dupes}")
        return df

    # map_batches injects a custom function into the physical plan
    return lf.map_batches(check_uniqueness)


def write_per_listing(df, listing_id):
    """
    Writes a DataFrame to a Parquet file, partitioned by listing ID.
    """
    out_path = f"/tmp/out-listing-{listing_id}.parquet"
    df.write_parquet(out_path)


class BatchWriter:
    """
    A class for writing batches of data to a single Parquet file.

    This class uses `pyarrow.parquet.ParquetWriter` to append data to a
    Parquet file, which is useful when writing out results in batches.
    """
    def __init__(self, outfile):
        self.out_path = outfile
        self.writer = None

    def write(self, df, listing_id):
        """
        Writes a DataFrame to the Parquet file.

        Args:
            df: The DataFrame to write.
            listing_id: The listing ID (not used in this implementation).
        """
        tbl = df.to_arrow()
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.out_path, tbl.schema)
        self.writer.write_table(tbl)


def generate_path(mics, year, month, day, table_name):
    """
    Generates S3 paths for a given set of parameters.

    Args:
        mics: A list of Market Identifier Codes (MICs).
        year: The year.
        month: The month.
        day: The day.
        table_name: The name of the table.

    Returns:
        A list of S3 paths.
    """
    ap = bmll2._configure.L2_ACCESS_POINT_ALIAS
    table_map = {
        "l2": "Equity",
        "l3": "Equity-L3",
        "trades": "Trades-Plus",
        "marketstate": "MarketState",
    }

    file_prefix = {
        "l2": "",
        "l3": "l3-",
        "trades": "trades-plus-",
        "marketstate": "market-state-",
    }

    yyyy = "%04d" % (year,)
    mm = "%02d" % (month,)
    dd = "%02d" % (day,)
    prefix = file_prefix[table_name]

    return [
        f"s3://{ap}/{table_map[table_name]}/{mic}/{yyyy}/{mm}/{dd}/{prefix}{mic}-{yyyy}{mm}{dd}.parquet"
        for mic in mics
    ]


def get_all_files_for_date(date, mics, table_name):
    """
    Generates a list of S3 paths for a given date, list of MICs, and table name.
    """
    year, month, day = date.year, date.month, date.day
    return generate_path(mics, year, month, day, table_name)


def get_files_for_date_range(start_date, end_date, mics, table_name):
    """
    Generates a list of S3 paths for a date range, list of MICs, and table name.
    """
    files = []
    current_date = start_date
    while current_date <= end_date:
        files.extend(get_all_files_for_date(current_date, mics, table_name))
        current_date += pd.Timedelta(days=1)
    return files


def get_s3_files(config, date, ref):
    """
    Retrieves S3 file paths for all required tables based on the config.
    """
    s3_files = {}
    for table_name in config["REQUIRES"]:
        s3_files[table_name] = get_files_for_date_range(
            date, date, ref["MIC"].unique().tolist(), table_name
        )
    return s3_files
