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
from profiling import Profiler

from intraday_analytics import (
    AnalyticsPipeline,
    DEFAULT_CONFIG,
    managed_execution,
    create_date_batches,
    cache_universe,
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


if __name__ == "__main__":
    os.environ["POLARS_MAX_THREADS"] = "48"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    logging.basicConfig(
        level=CONFIG.get("LOGGING_LEVEL", "INFO").upper(),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    date_batches = create_date_batches(
        CONFIG["START_DATE"], CONFIG["END_DATE"], CONFIG.get("DEFAULT_FREQ")
    )
    logging.info(f"📅 Created {len(date_batches)} date batches.")

    with managed_execution(CONFIG) as (processes, temp_dir):
        profiler = None
    if CONFIG.get("ENABLE_PROFILER_TOOL", False):
        try:
            profiler = Profiler()
            profiler.start()
            logging.info("📊 Profiler client started in main process.")
        except Exception as e:
            logging.error(f"Failed to start profiler client in main process: {e}")
            profiler = None

        try:
            CONFIG["TEMP_DIR"] = temp_dir

            # storage path helper
            s3_user = (
                "s3://"
                + bmll2.storage_paths()[CONFIG["AREA"]]["bucket"]
                + "/"
                + bmll2.storage_paths()[CONFIG["AREA"]]["prefix"]
            )

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
        finally:
            if profiler:
                profiler.stop()
                logging.info("📊 Profiler client stopped in main process.")
