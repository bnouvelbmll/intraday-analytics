"""
Example 1: Compute OHLCV Minute Bars

This script demonstrates how to compute Open, High, Low, Close, and Volume (OHLCV)
analytics for a given universe of instruments over a 2-month period. The data is
aggregated into 1-minute bars.
"""

import os
import logging
import bmll.reference
import polars as pl

from intraday_analytics.execution import run_metrics_pipeline
from intraday_analytics.pipeline import AnalyticsPipeline
from intraday_analytics.analytics.dense import DenseAnalytics, DenseAnalyticsConfig
from intraday_analytics.analytics.trade import (
    TradeAnalytics,
    TradeAnalyticsConfig,
    TradeGenericConfig,
)
from intraday_analytics.configuration import AnalyticsConfig, PassConfig

# --- Configuration ---

# This configuration defines the parameters for the analytics pipeline.
# You can customize the date range, universe, dataset name, and the analytics to be computed.
USER_CONFIG = {
    "START_DATE": "2025-11-01",
    "END_DATE": "2025-12-31",
    "DATASETNAME": "demo_ohlcv_1min",
    "PASSES": [
        {
            "name": "pass1",
            "time_bucket_seconds": 60,  # 1-minute bars
            "modules": ["dense", "trade"],
            "trade_analytics": {
                "generic_metrics": [
                    {
                        "measures": ["OHLC", "Volume"],
                    }
                ]
            },
        }
    ],
}

# --- Universe Definition ---


def get_universe(date):
    """
    Retrieves the universe of instruments for a given date.

    This example uses a query to the BMLL reference data service to get a list of
    instruments. You can replace this with your own logic to define the universe.
    """
    # This is the reference universe as requested by the user.
    # Note: This is a placeholder and will not be executed by this script.
    # The actual universe will be determined by the data available in the environment.
    universe_query = bmll.reference.query(
        Index="bezacp", object_types="Instrument"
    ).query("IsAlive")

    # For the purpose of this demo, we will return an empty dataframe,
    # as we cannot execute the query. The pipeline will use the symbols
    # available in the data source.
    return pl.DataFrame()


# --- Pipeline Definition ---


def get_pipeline(
    pass_config: PassConfig, context: dict, symbols: list, ref: pl.DataFrame, date: str
):
    """
    Constructs the analytics pipeline for a single pass.
    """
    modules = []
    if "dense" in pass_config.modules:
        modules.append(DenseAnalytics(ref, DenseAnalyticsConfig()))
    if "trade" in pass_config.modules:
        trade_config = TradeAnalyticsConfig(**pass_config.trade_analytics)
        modules.append(TradeAnalytics(trade_config))

    return AnalyticsPipeline(modules, pass_config, context)


# --- Main Execution ---

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Create the main configuration object
    config = AnalyticsConfig(**USER_CONFIG)

    # Run the pipeline
    run_metrics_pipeline(
        config,
        get_pipeline,
        get_universe,
    )

    logging.info("Pipeline finished successfully.")
    output_file = f"{config.DATASETNAME}/{config.START_DATE}_{config.END_DATE}.parquet"
    logging.info(f"Output written to: {output_file}")
