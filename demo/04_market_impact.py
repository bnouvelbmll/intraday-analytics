"""
Example 4: Compute Market Impact

This script demonstrates how to compute the average market impact for each
execution venue (MIC) at 1-hour intervals.

We use the built-in `TradeAnalytics` module to compute the `PriceImpact`
metric. The pipeline is configured to aggregate data into 1-hour time buckets.
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
    TradeImpactConfig,
)
from intraday_analytics.configuration import AnalyticsConfig, PassConfig

# --- Configuration ---

USER_CONFIG = {
    "START_DATE": "2025-11-01",
    "END_DATE": "2025-12-31",
    "DATASETNAME": "demo_market_impact_1h",
    "PASSES": [
        {
            "name": "market_impact_pass",
            "time_bucket_seconds": 3600,  # 1-hour bars
            "modules": ["dense", "trade"],
            "trade_analytics": {
                "impact_metrics": [
                    {
                        "variant": "PriceImpact",
                        "horizon": "1s",  # Time horizon for post-trade analysis
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
    """
    universe_query = bmll.reference.query(
        Index="bezacp", object_type="Instrument", start_date=date
    ).query("IsAlive")

    return pl.DataFrame(universe_query)


# --- Main Execution ---

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    config = AnalyticsConfig(**USER_CONFIG)

    run_metrics_pipeline(
        config=config,
        get_universe=get_universe,
    )

    logging.info("Pipeline finished successfully.")
    output_file = f"{config.DATASETNAME}/{config.START_DATE}_{config.END_DATE}.parquet"
    logging.info(f"Output written to: {output_file}")
