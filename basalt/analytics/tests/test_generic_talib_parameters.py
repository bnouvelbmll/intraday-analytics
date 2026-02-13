from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from basalt.analytics.generic import (
    GenericAnalytics,
    GenericAnalyticsConfig,
    TalibIndicatorConfig,
)
import basalt.analytics.generic as generic_mod


class _FakeTalib:
    def __init__(self):
        self.calls: list[dict] = []

    def MACD(self, arr, **kwargs):
        self.calls.append(dict(kwargs))
        arr = np.asarray(arr, dtype=np.float64)
        return arr + 1.0, arr + 2.0, arr + 3.0

    def SMA(self, arr, **kwargs):
        self.calls.append(dict(kwargs))
        arr = np.asarray(arr, dtype=np.float64)
        return arr


def test_talib_indicator_supports_multi_parameter_and_multi_output(monkeypatch):
    fake = _FakeTalib()
    monkeypatch.setattr(generic_mod, "TALIB_AVAILABLE", True)
    monkeypatch.setattr(generic_mod, "talib", fake)

    cfg = GenericAnalyticsConfig(
        source_pass="pass1",
        group_by=["ListingId", "TimeBucket"],
        talib_indicators=[
            TalibIndicatorConfig(
                name="MACD",
                input_col="Close",
                output_col="MACD_signal",
                output_index=1,
                parameters={"fastperiod": 12, "slowperiod": 26, "signalperiod": 9},
            )
        ],
    )
    mod = GenericAnalytics(cfg)
    mod.context = {
        "pass1": pl.DataFrame(
            {
                "ListingId": [1, 1, 1],
                "TimeBucket": [1, 2, 3],
                "Close": [10.0, 11.0, 12.0],
            }
        )
    }
    out = mod.compute().collect().sort("TimeBucket")

    assert fake.calls
    assert fake.calls[0]["fastperiod"] == 12
    assert fake.calls[0]["slowperiod"] == 26
    assert fake.calls[0]["signalperiod"] == 9
    assert out["MACD_signal"].to_list() == [12.0, 13.0, 14.0]


def test_talib_indicator_timeperiod_is_backward_compatible(monkeypatch):
    fake = _FakeTalib()
    monkeypatch.setattr(generic_mod, "TALIB_AVAILABLE", True)
    monkeypatch.setattr(generic_mod, "talib", fake)

    cfg = GenericAnalyticsConfig(
        source_pass="pass1",
        group_by=["ListingId", "TimeBucket"],
        talib_indicators=[
            TalibIndicatorConfig(
                name="SMA",
                input_col="Close",
                output_col="SMA_5",
                timeperiod=5,
                parameters={},
            )
        ],
    )
    mod = GenericAnalytics(cfg)
    mod.context = {
        "pass1": pl.DataFrame(
            {
                "ListingId": [1, 1],
                "TimeBucket": [1, 2],
                "Close": [10.0, 11.0],
            }
        )
    }
    mod.compute().collect()

    assert fake.calls
    assert fake.calls[0]["timeperiod"] == 5


def test_resample_rule_requires_timebucket_in_group_by():
    with pytest.raises(ValueError, match="requires 'TimeBucket' in group_by"):
        GenericAnalyticsConfig(
            source_pass="pass1",
            group_by=["ListingId"],
            resample_rule="15m",
        )
