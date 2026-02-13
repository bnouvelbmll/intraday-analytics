from __future__ import annotations

from .backend import (
    bmll_job,
    submit_function_job,
    submit_instance_job,
    convert_dagster_cron_to_bmll,
)
from .cli_ext import (
    bmll_run,
    bmll_install,
    get_cli_extension,
)

__all__ = [
    "bmll_job",
    "submit_function_job",
    "submit_instance_job",
    "convert_dagster_cron_to_bmll",
    "bmll_run",
    "bmll_install",
    "get_cli_extension",
]


def get_basalt_plugin():
    return {
        "name": "bmll",
        "provides": ["bmll jobs executor"],
        "cli_extensions": ["bmll"],
    }
