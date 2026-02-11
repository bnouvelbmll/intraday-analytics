"""
Example 7: Index-Level Analytics (Reaggregate)

This demo computes listing-level metrics in pass1, then reaggregates them
to index-level metrics in pass2 using a group dataframe (ListingId -> IndexId).
"""

import bmll.reference
import polars as pl

from basalt.cli import run_cli
from basalt.dagster.dagster_compat import CustomUniverse
from basalt.pipeline import create_pipeline


USER_CONFIG = {
    "START_DATE": "2025-11-01",
    "END_DATE": "2025-12-31",
    "DATASETNAME": "demo_index_analytics",
    "PASSES": [
        {
            "name": "pass1",
            "time_bucket_seconds": 60,
            "modules": ["trade"],
        },
        {
            "name": "pass2",
            "time_bucket_seconds": 60,
            "modules": ["reaggregate"],
            "reaggregate_analytics": {
                "source_pass": "pass1",
                "group_df_context_key": "index_membership",
                "join_column": "ListingId",
                "group_column": "IndexId",
                "group_by": ["IndexId", "TimeBucket"],
            },
        },
    ],
}


def get_universe(date):
    blacklist = ["@ALP", "SGMX", "SGMU", "BOTC"]
    universe_query = (
        bmll.reference.query(Index="bezacp", object_type="Instrument", start_date=date)
        .query("IsAlive")
        .query("MIC not in @blacklist")
    )
    return pl.DataFrame(universe_query)


def _get_index_membership(date):
    # Example mapping: ListingId -> IndexId.
    # Replace with your own index membership source if needed.
    ref = get_universe(date)
    if "IndexId" in ref.columns:
        return ref.select(["ListingId", "IndexId"])
    return ref.select(["ListingId"]).with_columns(pl.lit("Index").alias("IndexId"))


def get_pipeline(pass_config, context, symbols=None, ref=None, date=None, config=None):
    if date and "index_membership" not in context:
        context["index_membership"] = _get_index_membership(date)
    return create_pipeline(
        pass_config=pass_config,
        context=context,
        ref=ref,
        config=config,
    )


UNIVERSES = [CustomUniverse(get_universe, name="index_demo")]


if __name__ == "__main__":
    run_cli(
        USER_CONFIG,
        get_universe,
        get_pipeline=get_pipeline,
        config_file=__file__,
    )
