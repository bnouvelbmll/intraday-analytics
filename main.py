"""
This script runs the intraday analytics pipeline.

It processes high-frequency financial data for a specified date range and universe
of symbols. The pipeline is configurable through the `CONFIG` dictionary and can
be customized to compute various metrics.

The main steps of the pipeline are:
1.  **Data Preparation**: Data is loaded from the data lake, batched, and
    prepared for processing.
2.  **Metric Computation**: A series of analytics modules are run on the
    prepared data to compute metrics such as L2 order book features, trade
    analytics, and L3 event counts.
3.  **Output**: The results are written to Parquet files.

The script is designed to be run from the command line. It uses a locking
mechanism to prevent multiple instances from running simultaneously.
"""
import bmll2
import polars as pl
import pandas as pd
import os
import sys
import shutil
import logging

from intraday_analytics import (
    AnalyticsPipeline,
    DEFAULT_CONFIG,
    cache_universe,
    managed_execution,
)
from intraday_analytics.execution import ProcessInterval
from intraday_analytics.metrics.dense import DenseAnalytics
from intraday_analytics.metrics.l2 import L2AnalyticsLast, L2AnalyticsTW
from intraday_analytics.metrics.trade import TradeAnalytics
from intraday_analytics.metrics.l3 import L3Analytics
from intraday_analytics.metrics.execution import ExecutionAnalytics


# At 1min : 3 minutes per day.
# At 10 sec: 15 minutes per day ! 250gb matchine (20 GB memory)

USER_CONFIG = {
    # --- Date & Scope ---
    "START_DATE": "2025-11-01",
    "END_DATE": "2025-12-31",
    "UNIVERSE": {"Index": "bepacp"},
    # --- Analytics Parameters ---
    "TIME_BUCKET_SECONDS": 60,
    "L2_LEVELS": 10,
    "DENSE_OUTPUT": True,
}

CONFIG = {**DEFAULT_CONFIG, **USER_CONFIG}


whitelist = [
    "AQXE",
    "BATE",
    "CHIX",
    "BOTC",
    "XMAD",
    "XETR",
    "XAMS",
    "XBRU",
    "XLIS",
    "XPAR",
    "XWAR",
    "XMIL",
    "XLON",
    "XOSL",
    "XSWX",
    "TRQX",
    "XHEL",
    "XSTO",
    "XCSE",
    "ONSE",
    "XDUB",
    "CEUX",
    "AQEU",
    "TREU",
    "TWEM",
    "SGMX",
    "SGMU",
    "XEQT",
    "XTAL",
    "XLIT",
    "XRIS",
    "TQEX",
    "XBUD",
    "XPRA",
    "XWBO",
    "XSEB",
    "AQSE",
    "BMTF",
    "ARTX",
    "XFRA",
    "XSTU",
    "XGAT",
    "XMUN",
    "XHAN",
    "XHAM",
    "XDUS",
    "XBER",
]


@cache_universe(CONFIG["TEMP_DIR"])
def get_universe(date):
    """
    Retrieves the universe of symbols for a given date.

    This function loads a list of STOXX 600 symbols, filters them based on a
    whitelist of MICs, and enriches the data with reference information from
    the data lake.

    Args:
        date: The date for which to retrieve the universe.

    Returns:
        A pandas DataFrame containing the universe of symbols.
    """
    bmll2.get_file("support/ODDO/SXXP21July2025.csv")
    eumics = whitelist

    stoxx600bbg = (
        pd.read_csv("SXXP21July2025.csv")
        .rename(
            columns={
                "bbg_ticker": "BloombergTicker",
                "exchange": "Exchange",
                "isin": "ISIN",
                "currency": "CurrencyCode",
            }
        )
        .assign(
            BloombergTicker=lambda df: df["BloombergTicker"].str.strip(),
            CurrencyCode=lambda df: df["CurrencyCode"].str.strip(),
        )
    )

    ref = bmll2.get_market_data_range(
        markets=eumics,
        table_name="reference",
        start_date=date,
        end_date=date,
        df_engine="polars",
    )

    ref = (
        ref.filter(pl.col("ISIN").is_in(list(stoxx600bbg["ISIN"])))
        .filter(pl.col("IsPrimary"))
        .filter(pl.col("IsAlive"))
        .to_pandas()
    )

    return ref


def get_pipeline(N, symbols=None, ref=None, date=None):
    """
    Constructs the analytics pipeline.

    This function creates an `AnalyticsPipeline` instance and adds the desired
    analytics modules to it. The modules are added in the order they should be
    executed.

    Args:
        N: The number of L2 order book levels to compute metrics for.
        symbols: An optional list of symbols to filter the universe by.
        ref: An optional reference DataFrame.
        date: The date for which the pipeline is being constructed.

    Returns:
        An `AnalyticsPipeline` instance.
    """
    assert date is not None
    modules = []

    if CONFIG["DENSE_OUTPUT"]:
        cref = ref if ref is not None else get_universe(date)
        if symbols is not None:
            cref = cref.filter(pl.col("ListingId").is_in(list(symbols)))
        modules += [DenseAnalytics(cref, CONFIG)]

    modules += [
        L2AnalyticsLast(N),
        L2AnalyticsTW(N, CONFIG),
        TradeAnalytics(),
        L3Analytics(),
        ExecutionAnalytics(),
    ]

    return AnalyticsPipeline(modules)


def create_date_batches(
    start_date_str: str,
    end_date_str: str,
    period_freq: str | None = None,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Creates date batches from a date range, aligning to calendar weeks,
    months, or years for better consistency.

    Args:
        start_date_str: The start date of the range.
        end_date_str: The end date of the range.
        period_freq: The frequency to use for batching ('W', '2W', 'M', 'A').
                     If None, the frequency is auto-detected based on the
                     duration of the date range.
    """
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)
    total_days = (end_date - start_date).days

    if period_freq is None:
        logging.info("Auto-detecting batching frequency...")
        if total_days <= 7:
            logging.info("Date range is <= 7 days, creating a single batch.")
            return [(start_date, end_date)]
        # Auto-detect frequency
        if 7 < total_days <= 60:
            period_freq = "W"
        elif 60 < total_days <= 180:
            period_freq = "2W"
        elif 180 < total_days <= 365 * 2:
            period_freq = "M"
        else:
            period_freq = "A"
        logging.info(f"Auto-detected frequency: {period_freq}")
    else:
        logging.info(f"Using specified frequency: {period_freq}")


    # Map user-friendly frequency to pandas period and offset
    if period_freq == "W":
        align_freq = "W"
        offset = pd.DateOffset(weeks=1)
    elif period_freq == "2W":
        align_freq = "W"  # Align to week start
        offset = pd.DateOffset(weeks=2)
    elif period_freq == "M":
        align_freq = "M"
        offset = pd.DateOffset(months=1)
    elif period_freq == "A":
        align_freq = "A"
        offset = pd.DateOffset(years=1)
    else:
        raise ValueError(
            f"Unsupported frequency: {period_freq}. Supported values are 'W', '2W', 'M', 'A'."
        )

    # Align start date to the beginning of the period for consistent chunks
    aligned_start = start_date.to_period(align_freq).start_time

    batches = []
    current_start = aligned_start
    while current_start <= end_date:
        current_end = current_start + offset - pd.Timedelta(days=1)
        # Ensure the batch does not go beyond the overall end_date
        batch_end = min(current_end, end_date)
        batches.append((current_start, batch_end))
        current_start += offset

    return batches


if __name__ == "__main__":
    os.environ["POLARS_MAX_THREADS"] = "48"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    
    logging.basicConfig(
        level=CONFIG.get("LOGGING_LEVEL", "INFO").upper(),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    with managed_execution() as (processes, temp_dir):
        CONFIG["TEMP_DIR"] = temp_dir

        # storage path helper
        s3_user = (
            "s3://"
            + bmll2.storage_paths()[CONFIG["AREA"]]["bucket"]
            + "/"
            + bmll2.storage_paths()[CONFIG["AREA"]]["prefix"]
        )

        date_batches = create_date_batches(
            CONFIG["START_DATE"], CONFIG["END_DATE"], CONFIG.get("DEFAULT_FREQ")
        )
        logging.info(f"📅 Created {len(date_batches)} date batches.")

        for sd, ed in date_batches:
            logging.info(f"🚀 Starting batch for dates: {sd.date()} -> {ed.date()}")
            p = ProcessInterval(
                sd=sd,
                ed=ed,
                config=CONFIG,
                get_pipeline=get_pipeline,
                get_universe=get_universe,
            )
            p.start()
            processes.append(p)
