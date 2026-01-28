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
try:
    import viztracer
    from viztracer import VizLoggingHandler, get_tracer
except ImportError:
    viztracer = None

from intraday_analytics import (
    AnalyticsPipeline, # Re-add AnalyticsPipeline import
    DEFAULT_CONFIG,
    cache_universe,
)
from intraday_analytics.utils import create_date_batches
from intraday_analytics.execution import ProcessInterval

# Re-add direct imports for analytics modules
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
    "DATASETNAME": "sample2d",
    # --- Analytics Parameters ---
    "TIME_BUCKET_SECONDS": 60,
    "L2_LEVELS": 10,
    "DENSE_OUTPUT": True,
    # --- Batching & Performance ---
    "PREPARE_DATA_MODE": "naive",  # "naive" or "s3_shredding"
    "SEPARATE_METRIC_PROCESS": True,
    "NUM_WORKERS": -1, # use -1 for all available CPUs
    "CLEAN_UP_BATCH_FILES": True,
    "CLEAN_UP_TEMP_DIR": True,
    # --- Output Configuration ---
    # To customize the final output path, add "FINAL_OUTPUT_PATH_TEMPLATE" here.
    # Example: "FINAL_OUTPUT_PATH_TEMPLATE": "s3://custom-bucket/analytics/{datasetname}/{start_date}_{end_date}.parquet"
    "S3_STORAGE_OPTIONS": {"region": "us-east-1"}, # Example S3 storage options
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
    bmll2.get_file("SXXP21July2025.csv")
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

    return AnalyticsPipeline(modules, CONFIG)


if __name__ == "__main__":
    os.environ["POLARS_MAX_THREADS"] = "48"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    logging.basicConfig(
        level=CONFIG.get("LOGGING_LEVEL", "INFO").upper(),
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,
    )
    date_batches = create_date_batches(
        CONFIG["START_DATE"], CONFIG["END_DATE"], CONFIG.get("DEFAULT_FREQ")
    )
    logging.info(f"📅 Created {len(date_batches)} date batches.")

    tracer = None
    if CONFIG.get("ENABLE_PROFILER_TOOL", False):
        try:
            tracer = viztracer.VizTracer()
            tracer.start()
            handler = VizLoggingHandler()
            handler.setTracer(get_tracer())
            logging.getLogger().addHandler(handler)
            logging.info("📊 VizTracer started in main process.")
        except Exception as e:
            logging.error(f"Failed to start VizTracer in main process: {e}")
            tracer = None

    try:
        temp_dir = CONFIG["TEMP_DIR"] # Define temp_dir
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        os.makedirs(temp_dir, exist_ok=True)
        
        # storage path helper
        s3_user = (
            "s3://"
            + bmll2.storage_paths()[CONFIG["AREA"]]["bucket"]
            + "/"
            + bmll2.storage_paths()[CONFIG["AREA"]]["prefix"]
        )

        # Call compute_metrics after all data is prepared
        from intraday_analytics.execution import compute_metrics

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
            p.join() # Wait for each ProcessInterval to complete sequentially
            
            # Compute metrics for this interval immediately
            logging.info(f"📊 Computing metrics for batch: {sd.date()} -> {ed.date()}")
            compute_metrics(CONFIG, get_pipeline, get_universe, start_date=sd, end_date=ed)
            
        logging.info("✅ All data preparation and metric computation processes completed.")

    finally:
        if tracer:
            tracer.stop()
            logging.info("📊 VizTracer stopped in main process.")
        
        # Clean up temporary directory if configured
        if CONFIG.get("CLEAN_UP_TEMP_DIR", True) and os.path.exists(temp_dir):
            logging.info(f"🧹 Cleaning up temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir)
