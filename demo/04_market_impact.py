"""
Example 4: Compute Market Impact

This script demonstrates how to compute the average market impact for each
execution venue (MIC) at 1-hour intervals.

We use the built-in `TradeAnalytics` module to compute the `PriceImpact`
metric. The pipeline is configured to aggregate data into 1-hour time buckets.
"""

import bmll.reference
import polars as pl
from intraday_analytics.cli import run_cli

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
    run_cli(USER_CONFIG, get_universe)
