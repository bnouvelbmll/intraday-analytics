from __future__ import annotations

import datetime as dt

import polars as pl

from basalt.time.external_events import ExternalEventsAnalytics, ExternalEventsAnalyticsConfig


def test_external_events_anchor_only():
    cfg = ExternalEventsAnalyticsConfig(source_pass="pass1")
    mod = ExternalEventsAnalytics(cfg)
    mod.context = {
        "pass1": pl.DataFrame(
            {
                "ListingId": [1],
                "TimeBucket": [dt.datetime(2025, 1, 1, 10, 0, 0)],
            }
        )
    }
    out = mod.compute().collect()
    assert out.height == 1
    assert out["EventContextIndex"].to_list() == [0]
    assert out["EventDeltaSeconds"].to_list() == [0.0]


def test_external_events_arithmetic_context():
    cfg = ExternalEventsAnalyticsConfig(
        source_pass="pass1",
        radius_seconds=10.0,
        directions="both",
        extra_observations=2,
        scale_factor=1.0,
    )
    mod = ExternalEventsAnalytics(cfg)
    mod.context = {
        "pass1": pl.DataFrame(
            {
                "ListingId": [1],
                "TimeBucket": [dt.datetime(2025, 1, 1, 10, 0, 0)],
            }
        )
    }
    out = mod.compute().collect().sort("EventContextIndex")
    assert out["EventContextIndex"].to_list() == [-2, -1, 0, 1, 2]
    assert out["EventDeltaSeconds"].to_list() == [-10.0, -5.0, 0.0, 5.0, 10.0]


def test_external_events_geometric_context():
    offsets = ExternalEventsAnalytics.build_context_offsets(
        radius_seconds=10.0,
        directions="both",
        extra_observations=2,
        scale_factor=2.0,
    )
    by_idx = {idx: delta for idx, delta in offsets}
    assert by_idx[0] == 0.0
    assert by_idx[-2] == -10.0
    assert by_idx[2] == 10.0
    assert abs(by_idx[-1]) < abs(by_idx[-2])
    assert abs(by_idx[1]) < abs(by_idx[2])


def test_external_events_legacy_context_keys_migrate():
    cfg = ExternalEventsAnalyticsConfig(
        context_before=2,
        context_after=1,
        context_horizon_seconds=10.0,
        context_distribution="geometric",
        geometric_base=10.0,
    )
    assert cfg.radius_seconds == 10.0
    assert cfg.directions == "both"
    assert cfg.extra_observations == 2
    assert cfg.scale_factor == 10.0
