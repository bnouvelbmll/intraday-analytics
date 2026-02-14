from __future__ import annotations

from .iceberg import IcebergAnalyticsConfig
from .aggressive_trades import AggressiveTradesPreprocessConfig
from .cbbo_preprocess import CBBOPreprocessConfig
from .cbbofroml3_preprocess import CBBOfromL3PreprocessConfig

def get_basalt_plugin():
    return {
        "name": "preprocessors",
        "analytics_packages": ["basalt.preprocessors"],
        "provides": ["preprocessor modules"],
        "module_configs": [
            {
                "module": "iceberg",
                "config_key": "iceberg_analytics",
                "model": IcebergAnalyticsConfig,
            },
            {
                "module": "cbbo_preprocess",
                "config_key": "cbbo_preprocess",
                "model": CBBOPreprocessConfig,
            },
            {
                "module": "cbbofroml3_preprocess",
                "config_key": "cbbofroml3_preprocess",
                "model": CBBOfromL3PreprocessConfig,
            },
            {
                "module": "aggressive_preprocess",
                "config_key": "aggressive_preprocess",
                "model": AggressiveTradesPreprocessConfig,
            },
        ],
    }
