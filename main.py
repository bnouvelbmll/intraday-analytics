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

from intraday_analytics.execution import run_metrics_pipeline

from intraday_analytics.utils import create_date_batches

# Re-add direct imports for analytics modules
from intraday_analytics.metrics.dense import DenseAnalytics, DenseAnalyticsConfig
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
    # --- Analytics Passes ---
    "PASSES": [
        {
            "name": "pass1",
            "time_bucket_seconds": 60,
            "modules": [
                "dense",
                "l2_last",
                "l2_tw",
                "trade",
                "l3",
                "execution",
            ],
            "trade_analytics": {
                "enable_retail_imbalance": True,
            },
        }
    ],
    # --- Batching & Performance ---
    "PREPARE_DATA_MODE": "s3_shredding",  # "naive" or "s3_shredding"
    "SEPARATE_METRIC_PROCESS": True,
    "NUM_WORKERS": -1,  # use -1 for all available CPUs
    "CLEAN_UP_BATCH_FILES": True,
    "CLEAN_UP_TEMP_DIR": True,
    "EAGER_EXECUTION": True,
    # --- Output Configuration ---
    # To customize the final output path, add "FINAL_OUTPUT_PATH_TEMPLATE" here.
    # Example: "FINAL_OUTPUT_PATH_TEMPLATE": "s3://custom-bucket/analytics/{datasetname}/{start_date}_{end_date}.parquet"
    "S3_STORAGE_OPTIONS": {"region": "us-east-1"},  # Example S3 storage options
    # "LOGGING_LEVEL": "debug",
    "EAGER_EXECUTION": True,
    "ENABLE_PROFILER_TOOL": False,
    "MEMORY_PER_WORKER": 35,
}

config_data = {**DEFAULT_CONFIG, **USER_CONFIG}
CONFIG = AnalyticsConfig(**config_data)


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
    # "BMTF",
    # "ARTX",
    # "XFRA",
    # "XSTU",
    # "XGAT",
    # "XMUN",
    # "XHAN",
    # "XHAM",
    # "XDUS",
    # "XBER",
]


@cache_universe(CONFIG.TEMP_DIR)
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

    date = pd.Timestamp(date).date()
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


def get_pipeline(pass_config, context, symbols=None, ref=None, date=None):
    """
    Constructs the analytics pipeline for a single pass.

    This function creates an `AnalyticsPipeline` instance and adds the desired
    analytics modules to it based on the pass configuration.

    Args:
        pass_config: The configuration for the analytics pass.
        context: A dictionary for sharing data between passes.
        symbols: An optional list of symbols to filter the universe by.
        ref: An optional reference DataFrame.
        date: The date for which the pipeline is being constructed.

    Returns:
        An `AnalyticsPipeline` instance.
    """
    assert date is not None
    modules = []

    for module_name in pass_config.modules:
        if module_name == "dense":
            cref = ref if ref is not None else get_universe(date)
            if symbols is not None:
                cref = cref.filter(pl.col("ListingId").is_in(list(symbols)))
            modules.append(DenseAnalytics(cref, pass_config.dense_analytics))
        elif module_name == "l2_last":
            modules.append(L2AnalyticsLast(pass_config.l2_analytics))
        elif module_name == "l2_tw":
            modules.append(L2AnalyticsTW(pass_config.l2_analytics))
        elif module_name == "trade":
            modules.append(TradeAnalytics(pass_config.trade_analytics))
        elif module_name == "l3":
            modules.append(L3Analytics(pass_config.l3_analytics))
        elif module_name == "execution":
            modules.append(ExecutionAnalytics(pass_config.execution_analytics))

    return AnalyticsPipeline(modules, CONFIG, pass_config, context)


if __name__ == "__main__":
    os.environ["POLARS_MAX_THREADS"] = "48"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    logging.basicConfig(
        level=CONFIG.LOGGING_LEVEL.upper(),
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,
    )

    ret_code = 0
    try:
        from intraday_analytics.execution import run_metrics_pipeline

        context = {}
        for pass_config in CONFIG.PASSES:
            logging.info(f"Starting pass: {pass_config.name}")
            run_metrics_pipeline(
                CONFIG,
                get_pipeline=lambda symbols, ref, date: get_pipeline(
                    pass_config, context, symbols, ref, date
                ),
                get_universe=get_universe,
            )

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        ret_code = 1

    sys.exit(ret_code)
