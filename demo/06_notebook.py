"""
Example 6: L3 and trade characteristics (notebook-style demo)

This demo runs the l3_characteristics and trade_characteristics modules on
a single MIC/date pair using 1-hour time buckets.
"""

import logging
import bmll.reference
import polars as pl

from intraday_analytics.execution import run_multiday_pipeline
from intraday_analytics.configuration import AnalyticsConfig


USER_CONFIG = {
    "START_DATE": "2025-01-02",
    "END_DATE": "2025-01-02",
    "DATASETNAME": "demo_characteristics_1h",
    "PASSES": [
        {
            "name": "characteristics_pass",
            "time_bucket_seconds": 3600,
            "modules": ["l3_characteristics", "trade_characteristics"],
            "l3_characteristics_analytics": {},
            "trade_characteristics_analytics": {},
        }
    ],
}


def get_universe(date):
    """
    Retrieves the universe of instruments for a given date.
    """
    universe_query = bmll.reference.query(
        Index="bezacp", object_type="Instrument", start_date=date
    ).query("IsAlive")
    return pl.DataFrame(universe_query)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    config = AnalyticsConfig(**USER_CONFIG)

    run_multiday_pipeline(
        config=config,
        get_universe=get_universe,
    )

    logging.info("Pipeline finished successfully.")
    output_file = f"{config.DATASETNAME}/{config.START_DATE}_{config.END_DATE}.parquet"
    logging.info(f"Output written to: {output_file}")
