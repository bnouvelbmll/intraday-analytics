from __future__ import annotations

from basalt.analytics_base import default_hint_for_column


def test_default_hint_marks_non_aggregatable_labels():
    for col in [
        "EventType",
        "Indicator",
        "MetricX",
        "MetricY",
        "EventAnchorTime",
        "EventContextIndex",
    ]:
        hint = default_hint_for_column(col, weight_col="TradeNotionalEUR")
        assert hint["default_agg"] == "NotAggregated"
