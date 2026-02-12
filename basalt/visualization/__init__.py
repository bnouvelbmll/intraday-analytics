from __future__ import annotations


def get_basalt_plugin():
    return {
        "name": "visualization",
        "provides": ["streamlit dataset explorer", "plot module registry"],
        "cli_extensions": ["viz"],
    }
