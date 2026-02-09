"""
Example 5: Trade analytics on aggressive trades

This demo shows how to run trade analytics on an aggressive-trades view instead
of the raw trades table. We compute aggressive orders and store them in the
pipeline context, then tell TradeAnalytics to read from that context key.
"""

import bmll.reference
import polars as pl

from intraday_analytics.cli import run_cli
from intraday_analytics.pipeline import AnalyticsPipeline
from intraday_analytics.dense_analytics import DenseAnalytics
from intraday_analytics.analytics.trade import TradeAnalytics
from intraday_analytics.dataset_transforms.aggressive_trades import (
    AggressiveTradesTransform,
)
from intraday_analytics.configuration import AnalyticsConfig, PassConfig
from intraday_analytics.dagster_compat import CustomUniverse


USER_CONFIG = {
    "START_DATE": "2025-11-01",
    "END_DATE": "2025-12-31",
    "DATASETNAME": "demo_aggressive_trades_1min",
    "PASSES": [
        {
            "name": "aggressive_trade_pass",
            "time_bucket_seconds": 60,
            "modules": ["dense", "trade"],
            "trade_analytics": {
                "use_tagged_trades": True,
                "tagged_trades_context_key": "aggressive_trades",
                "generic_metrics": [
                    {"sides": ["Total"], "measures": ["Volume", "Count", "VWAP"]}
                ],
            },
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
UNIVERSES = [CustomUniverse(get_universe, name="demo_aggressive_trades")]


class AggressiveTradesPipeline(AnalyticsPipeline):
    """
    Pipeline that injects aggressive-trades into the shared context.
    """

    def __init__(
        self, *args, transform: AggressiveTradesTransform, context_key: str, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._transform = transform
        self._context_key = context_key

    def run_on_multi_tables(self, **tables_for_sym):
        trades = tables_for_sym.get("trades")
        if trades is not None:
            trades_ldf = trades.lazy() if hasattr(trades, "lazy") else trades
            aggressive_ldf = self._transform.transform(trades_ldf)
            if aggressive_ldf is None:
                aggressive_ldf = trades_ldf
            self.context[self._context_key] = aggressive_ldf
        return super().run_on_multi_tables(**tables_for_sym)


def get_pipeline(
    pass_config: PassConfig,
    context: dict,
    ref: pl.DataFrame,
    **kwargs,
):
    """
    Constructs the analytics pipeline and wires aggressive-trades into context.
    """
    modules = [
        DenseAnalytics(ref, pass_config.dense_analytics),
        TradeAnalytics(pass_config.trade_analytics),
    ]

    transform = AggressiveTradesTransform(
        execution_size="Size",
        deltat=1e-3,
        time_bucket_seconds=int(pass_config.time_bucket_seconds),
    )

    return AggressiveTradesPipeline(
        modules,
        config=kwargs["config"],
        pass_config=pass_config,
        context=context,
        transform=transform,
        context_key=pass_config.trade_analytics.tagged_trades_context_key,
    )


if __name__ == "__main__":
    run_cli(
        USER_CONFIG,
        get_universe,
        get_pipeline=get_pipeline,
        config_file=__file__,
        config_precedence=CONFIG_YAML_PRECEDENCE,
    )
# --- Configuration ---
CONFIG_YAML_PRECEDENCE = "yaml_overrides"
