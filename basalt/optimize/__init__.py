from __future__ import annotations

from .core import optimize_pipeline
from .cli_ext import get_cli_extension

__all__ = ["optimize_pipeline", "get_cli_extension"]


def get_basalt_plugin():
    return {
        "name": "optimize",
        "provides": ["configuration optimization", "experiment search"],
        "cli_extensions": ["optimize"],
    }
