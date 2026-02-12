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

import bmll.reference
import polars as pl
import talib
from basalt.analytics_base import BaseAnalytics
from basalt.pipeline import AnalyticsPipeline
from basalt.cli import run_cli
from basalt.analytics.trade import TradeAnalytics
from basalt.configuration import AnalyticsConfig, PassConfig
from basalt.time.dense import DenseAnalytics
from basalt.dagster.dagster_compat import CustomUniverse

# --- Configuration ---
USER_CONFIG = {
    "START_DATE": "2025-11-01",
    "END_DATE": "2025-11-07",
    "DATASETNAME": "demo_talib_metrics",
    "PASSES": [
        {
            "name": "ohlcv_pass",
            "time_bucket_seconds": 60,
            "modules": ["trade"],
            "trade_analytics": {"generic_metrics": [{"measures": ["OHLC", "Volume"]}]},
        },
        {
            "time_bucket_seconds": 60,
            "name": "talib_pass",
            "modules": ["talib_metrics"],
            "module_inputs": {"talib_metrics": "ohlcv_pass"},
        },
    ],
    "SKIP_EXISTING_OUTPUT": True,
    "EAGER_EXECUTION": True,
    "LOGGING_LEVEL": "DEBUG",
}

# --- Universe Definition ---


def get_universe(date):
    """
    Retrieves the universe of instruments for a given date.

    This example uses a query to the BMLL reference data service to get a list of
    instruments. You can replace this with your own logic to define the universe.
    """
    blacklist = ["@ALP", "SGMX", "SGMU", "BOTC"]
    universe_query = (
        bmll.reference.query(Index="bezacp", object_type="Instrument", start_date=date)
        .query("IsAlive")
        .query("MIC not in @blacklist")
    )

    return pl.DataFrame(universe_query)


# Explicit universes config for Dagster definitions.
UNIVERSES = [CustomUniverse(get_universe, name="demo_talib")]


# --- Custom TA-Lib Analytics Module ---


class TALibAnalytics(BaseAnalytics):
    """
    A custom analytics module to compute TA-Lib indicators.
    """

    REQUIRES = ["df"]  # Consume an input frame mapped via module_inputs.

    def __init__(self):
        super().__init__("talib_metrics")

    def compute(self) -> pl.LazyFrame:
        """
        Computes SMA and RSI using TA-Lib.
        """
        print("X")
        source_df = self.df
        if source_df is None:
            raise ValueError(
                "Input dataframe not loaded. Configure module_inputs for "
                "'talib_metrics', e.g. {'talib_metrics': 'ohlcv_pass'}. "
                f"Available: {list(self.context.keys())}"
            )

        ohlcv_pass = source_df
        print(source_df.shape)
        res = (
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
            .with_columns(
                [
                    pl.col("Close")
                    .map_elements(
                        lambda s: talib.SMA(s.to_numpy(), timeperiod=14),
                        return_dtype=pl.Float64,
                    )
                    .over("ListingId")
                    .alias("SMA14"),
                    pl.col("Close")
                    .map_elements(
                        lambda s: talib.RSI(s.to_numpy(), timeperiod=14),
                        return_dtype=pl.Float64,
                    )
                    .over("ListingId")
                    .alias("RSI14"),
                ]
            )
        )
        print(res)
        print("/X")
        return res


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
    Constructs the analytics pipeline for each pass using a factory pattern.
    """
    # A factory pattern to build the analytics modules based on the config
    module_factories = {
        "dense": lambda: DenseAnalytics(ref, pass_config.dense_analytics),
        "trade": lambda: TradeAnalytics(pass_config.trade_analytics),
        "talib_metrics": lambda: TALibAnalytics(),
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
