import polars as pl

from basalt.analytics.events import EventAnalytics, EventAnalyticsConfig


def test_event_analytics_local_extrema():
    df = pl.DataFrame(
        {
            "ListingId": [1, 1, 1, 1, 1],
            "TimeBucket": [
                "2025-01-01 10:00:00",
                "2025-01-01 10:01:00",
                "2025-01-01 10:02:00",
                "2025-01-01 10:03:00",
                "2025-01-01 10:04:00",
            ],
            "Close": [1.0, 3.0, 1.0, 3.0, 1.0],
        }
    )
    cfg = EventAnalyticsConfig(source_pass="pass1", window=2, indicator="ewma")
    mod = EventAnalytics(cfg)
    mod.context = {"pass1": df}
    out = mod.compute().collect()
    assert "EventType" in out.columns
    assert out.height > 0
