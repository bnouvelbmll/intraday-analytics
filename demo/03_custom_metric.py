"""
Example 3: Defining a Custom Metric

This script demonstrates how to define and compute a custom metric within the
analytics pipeline. We will create a simple custom metric: the price range
(High - Low) for each 1-minute bar.

The custom metric is implemented in a custom analytics module, defined directly
in this file. This approach allows for rapid prototyping and testing of new
metrics without modifying the core library.
"""

import bmll.reference
import polars as pl
from basalt.analytics_base import BaseAnalytics
from basalt.pipeline import AnalyticsPipeline
from basalt.cli import run_cli
from basalt.dense_analytics import DenseAnalytics
from basalt.analytics.trade import TradeAnalytics
from basalt.configuration import AnalyticsConfig, PassConfig
from basalt.dagster.dagster_compat import CustomUniverse

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
            "trade_analytics": {"generic_metrics": [{"measures": ["OHLC", "Volume"]}]},
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


# Explicit universes config for Dagster definitions.
UNIVERSES = [CustomUniverse(get_universe, name="demo_custom_metric")]


# --- Custom Analytics Module ---


class CustomMetricAnalytics(BaseAnalytics):
    """
    A custom analytics module to compute the price range (High - Low).
    """

    REQUIRES = ["trades"]  # Depends on the trades table

    def __init__(self):
        super().__init__("custom")

    def compute(self) -> pl.LazyFrame:
        """
        Computes the price range and adds it as a new column.
        """
        gcols = ["MIC", "ListingId", "Ticker", "TimeBucket"]
        price = pl.col("LocalPrice")
        df = (
            self.trades.filter(pl.col("Classification") == "LIT_CONTINUOUS")
            .group_by(gcols)
            .agg((price.max() - price.min()).alias("PriceRange"))
        )
        self.df = df
        return df


# --- Pipeline Definition ---


def get_pipeline(
    pass_config: PassConfig,
    context: dict,
    symbols: list,
    ref: pl.DataFrame,
    date: str,
    config: AnalyticsConfig,
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

    return AnalyticsPipeline(modules, config, pass_config, context)


# --- Main Execution ---

if __name__ == "__main__":
    run_cli(
        USER_CONFIG,
        get_universe,
        get_pipeline=get_pipeline,
        config_file=__file__,
    )
