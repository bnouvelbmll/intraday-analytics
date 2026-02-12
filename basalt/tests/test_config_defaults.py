from __future__ import annotations

from basalt.configuration import AnalyticsConfig


def test_default_config_matches_default_analytics_config_model():
    assert AnalyticsConfig().to_dict() == AnalyticsConfig().model_dump()
