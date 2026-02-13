from __future__ import annotations

import pytest

from basalt.analytics.l2 import L2VolatilityConfig
from basalt.schema_utils import _apply_docs


def test_l2_volatility_rejects_non_std_aggregation():
    with pytest.raises(Exception, match="Input should be 'Std'"):
        L2VolatilityConfig(source="Mid", aggregations=["Mean"])  # type: ignore[list-item]


def test_l2_volatility_docs_are_precise():
    docs = _apply_docs("l2_tw", "L2VolatilityMidStd")
    definition = str(docs.get("definition") or "").lower()
    assert "annualized" in definition
    assert "std(log-returns)" in definition
