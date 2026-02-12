"""Alpha101 extension."""

from __future__ import annotations

from .alpha101 import Alpha101AnalyticsConfig

def get_basalt_plugin():
    return {
        "name": "alpha101",
        "analytics_packages": ["basalt.analytics.alpha101"],
        "provides": ["alpha101 analytics modules"],
        "module_configs": [
            {
                "module": "alpha101",
                "config_key": "alpha101_analytics",
                "model": Alpha101AnalyticsConfig,
            }
        ],
    }
