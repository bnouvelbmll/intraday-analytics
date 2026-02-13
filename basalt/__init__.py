"""
Basalt analytics package
"""

SUITE_NAME = "BASALT"
SUITE_EXPANSION = "BMLL Advanced Statistical Analytics & Layered Transformations"
SUITE_FULL_NAME = f"{SUITE_NAME}: {SUITE_EXPANSION}"

from .configuration import AnalyticsConfig, PassConfig
from .analytics_base import BaseAnalytics, BaseTWAnalytics
from .orchestrator import run_metrics_pipeline, run_multiday_pipeline
from .cli import run_cli
from .executors.bmll import bmll_job
from .pipeline import create_pipeline
from .utils import cache_universe
from .connections import (
    snowflake_connect_kwargs,
    snowflake_connection,
    databricks_connect_kwargs,
    databricks_connection,
    s3_connection_options,
)

__all__ = [
    "SUITE_NAME",
    "SUITE_EXPANSION",
    "SUITE_FULL_NAME",
    "AnalyticsConfig",
    "PassConfig",
    "BaseAnalytics",
    "BaseTWAnalytics",
    "run_metrics_pipeline",
    "run_multiday_pipeline",
    "run_cli",
    "bmll_job",
    "create_pipeline",
    "cache_universe",
    "snowflake_connect_kwargs",
    "snowflake_connection",
    "databricks_connect_kwargs",
    "databricks_connection",
    "s3_connection_options",
]
