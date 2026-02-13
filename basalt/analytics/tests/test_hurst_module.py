import polars as pl

from basalt.analytics.hurst import HurstAnalytics, HurstAnalyticsConfig


def test_hurst_rows_output():
    df = pl.DataFrame(
        {
            "ListingId": [1] * 60,
            "TimeBucket": [f"2025-01-01 10:{i:02d}:00" for i in range(60)],
            "MetricA": [float(i) for i in range(60)],
            "MetricB": [float(i % 5) for i in range(60)],
        }
    )
    cfg = HurstAnalyticsConfig(
        source_pass="pass1",
        columns=["MetricA", "MetricB"],
        group_by=["ListingId"],
        max_lag=10,
        min_periods=20,
    )
    mod = HurstAnalytics(cfg)
    mod.context = {"pass1": df}
    out = mod.compute().collect()
    assert "HurstExponent" in out.columns
    assert out.height == 2


def test_hurst_rolling_output():
    df = pl.DataFrame(
        {
            "ListingId": [1] * 40,
            "TimeBucket": [f"2025-01-01 10:{i:02d}:00" for i in range(40)],
            "MetricA": [float(i) for i in range(40)],
        }
    )
    cfg = HurstAnalyticsConfig(
        source_pass="pass1",
        columns=["MetricA"],
        group_by=["ListingId"],
        mode="rolling",
        rolling_window=20,
        rolling_min_periods=10,
        rolling_step=5,
    )
    mod = HurstAnalytics(cfg)
    mod.context = {"pass1": df}
    out = mod.compute().collect()
    assert "HurstExponent" in out.columns
    assert "TimeBucket" in out.columns
    assert out.height > 0
