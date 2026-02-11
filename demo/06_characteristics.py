"""
Example 6: L3 and trade characteristics (notebook-style demo)

# --- Configuration ---
This demo runs the l3_characteristics and trade_characteristics modules on
a single MIC/date pair using 1-hour time buckets.
"""

import bmll.reference
import polars as pl

from basalt.cli import run_cli
from basalt.dagster.dagster_compat import CustomUniverse


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


# Explicit universes config for Dagster definitions.
UNIVERSES = [CustomUniverse(get_universe, name="demo_characteristics")]


if __name__ == "__main__":
    run_cli(
        USER_CONFIG,
        get_universe,
        config_file=__file__,
    )
