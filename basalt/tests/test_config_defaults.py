from __future__ import annotations

from basalt.config import DEFAULT_CONFIG
from basalt.configuration import AnalyticsConfig


def test_default_config_matches_default_analytics_config_model():
    assert DEFAULT_CONFIG == AnalyticsConfig().to_dict()
