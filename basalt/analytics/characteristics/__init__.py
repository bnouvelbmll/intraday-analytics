from __future__ import annotations

from .l3_characteristics import L3CharacteristicsConfig
from .trade_characteristics import TradeCharacteristicsConfig

def get_basalt_plugin():
    return {
        "name": "characteristics",
        "analytics_packages": ["basalt.analytics.characteristics"],
        "provides": ["characteristics analytics modules"],
        "module_configs": [
            {
                "module": "l3_characteristics",
                "config_key": "l3_characteristics_analytics",
                "model": L3CharacteristicsConfig,
            },
            {
                "module": "trade_characteristics",
                "config_key": "trade_characteristics_analytics",
                "model": TradeCharacteristicsConfig,
            },
        ],
    }
