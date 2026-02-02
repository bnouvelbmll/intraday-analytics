"""
Example 2: Two-Pass Analysis with TA-Lib

This script demonstrates a two-pass analysis pipeline.
1.  **Pass 1**: Computes OHLCV minute bars, similar to the first example.
2.  **Pass 2**: Takes the OHLCV data from Pass 1 as input and uses the TA-Lib
    library to compute common technical analysis indicators, such as Simple
    Moving Average (SMA) and Relative Strength Index (RSI).

To run this example, you need to install the TA-Lib library:
pip install talib-binary
"""

import os
import logging
import bmll.reference
import polars as pl
import talib
from intraday_analytics.pipeline import BaseAnalytics, AnalyticsPipeline
from intraday_analytics.execution import run_metrics_pipeline
from intraday_analytics.analytics.dense import DenseAnalytics, DenseAnalyticsConfig
from intraday_analytics.analytics.trade import TradeAnalytics, TradeAnalyticsConfig
from intraday_analytics.config import AnalyticsConfig, PassConfig

# --- Configuration ---

USER_CONFIG = {
    "START_DATE": "2025-11-01",
    "END_DATE": "2025-12-31",
    "DATASETNAME": "demo_talib_metrics",
    "PASSES": [
        {
            "name": "ohlcv_pass",
            "time_bucket_seconds": 60,
            "modules": ["dense", "trade"],
            "trade_analytics": {"generic_metrics": [{"measures": ["OHLC", "Volume"]}]},
        },
        {
            "name": "talib_pass",
            "modules": ["talib_metrics"],
        },
    ],
}

# --- Universe Definition ---


def get_universe(date):
    """
    Retrieves the universe of instruments for a given date.
    """
    universe_query = bmll.reference.query(
        Index="bezacp", object_types="Instrument"
    ).query("IsAlive")

    return pl.DataFrame(universe_query)


# --- Custom TA-Lib Analytics Module ---


class TALibAnalytics(BaseAnalytics):
    """
    A custom analytics module to compute TA-Lib indicators.
    """

    REQUIRES = ["ohlcv_pass"]  # Depends on the output of the first pass

    def __init__(self):
        super().__init__("talib")

    def compute(self, ohlcv_pass: pl.LazyFrame) -> pl.LazyFrame:
        """
        Computes SMA and RSI using TA-Lib.
        """
        return (
            ohlcv_pass.group_by("ListingId", maintain_order=True)
            .agg(
                [
                    pl.col("*"),  # Keep all original columns
                    pl.col("Close")
                    .apply(lambda s: talib.SMA(s, timeperiod=14))
                    .alias("SMA14"),
                    pl.col("Close")
                    .apply(lambda s: talib.RSI(s, timeperiod=14))
                    .alias("RSI14"),
                ]
            )
            .explode(pl.col_matching(r"^(SMA14|RSI14)$"))
        )


# --- Pipeline Definition ---


def get_pipeline(
    pass_config: PassConfig, context: dict, symbols: list, ref: pl.DataFrame, date: str
):
    """
    Constructs the analytics pipeline for each pass.
    """
    modules = []
    pass_name = pass_config.name

    if pass_name == "ohlcv_pass":
        if "dense" in pass_config.modules:
            modules.append(DenseAnalytics(ref, DenseAnalyticsConfig()))
        if "trade" in pass_config.modules:
            trade_config = TradeAnalyticsConfig(**pass_config.trade_analytics)
            modules.append(TradeAnalytics(trade_config))
    elif pass_name == "talib_pass":
        if "talib_metrics" in pass_config.modules:
            modules.append(TALibAnalytics())

    return AnalyticsPipeline(modules, pass_config, context)


# --- Main Execution ---

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    config = AnalyticsConfig(**USER_CONFIG)

    run_metrics_pipeline(
        config,
        get_pipeline,
        get_universe,
    )

    logging.info("Pipeline finished successfully.")
    output_file = f"{config.DATASETNAME}/{config.START_DATE}_{config.END_DATE}.parquet"
    logging.info(f"Output written to: {output_file}")
