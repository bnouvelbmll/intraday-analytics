from __future__ import annotations


def get_basalt_plugin():
    return {
        "name": "mcp",
        "provides": ["mcp server", "run monitoring", "job configuration"],
        "cli_extensions": ["mcp"],
    }
