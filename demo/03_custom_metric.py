"""
Example 3: Defining a Custom Metric

This script demonstrates how to define and compute a custom metric within the
analytics pipeline. We will create a simple custom metric: the price range
(High - Low) for each 1-minute bar.

The custom metric is implemented in a custom analytics module, defined directly
in this file. This approach allows for rapid prototyping and testing of new
metrics without modifying the core library.
"""

import os
import logging
import bmll.reference
import polars as pl
from intraday_analytics.pipeline import BaseAnalytics, AnalyticsPipeline
from intraday_analytics.execution import run_metrics_pipeline
from intraday_analytics.analytics.dense import DenseAnalytics, DenseAnalyticsConfig
from intraday_analytics.analytics.trade import TradeAnalytics, TradeAnalyticsConfig
from intraday_analytics.configuration import AnalyticsConfig, PassConfig

# --- Configuration ---

USER_CONFIG = {
    "START_DATE": "2025-11-01",
    "END_DATE": "2025-12-31",
    "DATASETNAME": "demo_custom_metric",
    "PASSES": [
        {
            "name": "custom_metric_pass",
            "time_bucket_seconds": 60,
            "modules": ["dense", "trade", "custom_metric"],
            "trade_analytics": {"generic_metrics": [{"measures": ["OHLCV"]}]},
        }
    ],
}

# --- Universe Definition ---


def get_universe(date):
    """
    Retrieves the universe of instruments for a given date.
    """
    universe_query = bmll.reference.query(
        Index="bezacp", object_type="Instrument", start_date=date
    ).query("IsAlive")

    return pl.DataFrame(universe_query)


# --- Custom Analytics Module ---


class CustomMetricAnalytics(BaseAnalytics):
    """
    A custom analytics module to compute the price range (High - Low).
    """

    REQUIRES = ["trade"]  # Depends on the output of the TradeAnalytics module

    def __init__(self):
        super().__init__("custom")

    def compute(self, trade: pl.LazyFrame) -> pl.LazyFrame:
        """
        Computes the price range and adds it as a new column.
        """
        return trade.with_columns((pl.col("High") - pl.col("Low")).alias("PriceRange"))


# --- Pipeline Definition ---


def get_pipeline(
    pass_config: PassConfig, context: dict, symbols: list, ref: pl.DataFrame, date: str
):
    """
    Constructs the analytics pipeline using a factory pattern.
    """
    # A factory pattern to build the analytics modules based on the config
    module_factories = {
        "dense": lambda: DenseAnalytics(ref, pass_config.dense_analytics),
        "trade": lambda: TradeAnalytics(pass_config.trade_analytics),
        "custom_metric": lambda: CustomMetricAnalytics(),
    }

    modules = []
    for module_name in pass_config.modules:
        factory = module_factories.get(module_name)
        if factory:
            modules.append(factory())
        else:
            logging.warning(f"Module '{module_name}' not recognized in get_pipeline.")

    return AnalyticsPipeline(modules, pass_config, context)


# --- Main Execution ---

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    config = AnalyticsConfig(**USER_CONFIG)

    run_metrics_pipeline(
        config,
        get_universe,
        get_pipeline=get_pipeline,
    )

    logging.info("Pipeline finished successfully.")
    output_file = f"{config.DATASETNAME}/{config.START_DATE}_{config.END_DATE}.parquet"
    logging.info(f"Output written to: {output_file}")
