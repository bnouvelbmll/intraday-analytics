from __future__ import annotations

from .backend import (
    k8s_run,
    k8s_install,
)
from .cli_ext import get_cli_extension

__all__ = [
    "k8s_run",
    "k8s_install",
    "get_cli_extension",
]


def get_basalt_plugin():
    return {
        "name": "kubernetes",
        "provides": ["kubernetes executor"],
        "cli_extensions": ["k8s"],
    }
