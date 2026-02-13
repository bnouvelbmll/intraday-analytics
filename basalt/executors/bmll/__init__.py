from __future__ import annotations

from .backend import (
    bmll_job,
    submit_function_job,
    submit_instance_job,
    convert_dagster_cron_to_bmll,
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

def bmll_run(*args, **kwargs):
    from .cli_ext import bmll_run as _bmll_run

    return _bmll_run(*args, **kwargs)


def bmll_install(*args, **kwargs):
    from .cli_ext import bmll_install as _bmll_install

    return _bmll_install(*args, **kwargs)


def get_cli_extension():
    from .cli_ext import get_cli_extension as _get_cli_extension

    return _get_cli_extension()


def get_basalt_plugin():
    return {
        "name": "bmll",
        "provides": ["bmll jobs executor"],
        "cli_extensions": ["bmll"],
    }
