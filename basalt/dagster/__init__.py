from __future__ import annotations


def get_basalt_plugin():
    return {
        "name": "dagster",
        "provides": ["dagster integration"],
        "cli_extensions": ["dagster"],
    }
