"""
Example 1: Compute OHLCV Minute Bars

This script demonstrates how to compute Open, High, Low, Close, and Volume (OHLCV)
analytics for a given universe of instruments over a 2-month period. The data is
aggregated into 1-minute bars.
"""

import bmll.reference
import polars as pl

from intraday_analytics.cli import run_cli

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
            "modules": ["trade"],
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
    blacklist=['@ALP','SGMX','SGMU','BOTC']
    universe_query = bmll.reference.query(
        Index="bezacp", object_type="Instrument", start_date=date
    ).query("IsAlive").query("MIC not in @blacklist")

    return pl.DataFrame(universe_query)

# --- Main Execution ---

if __name__ == "__main__":
    run_cli(USER_CONFIG, get_universe)
