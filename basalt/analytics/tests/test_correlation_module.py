import polars as pl

from basalt.analytics.correlation import (
    CorrelationAnalytics,
    CorrelationAnalyticsConfig,
)


def test_correlation_rows_output():
    df = pl.DataFrame(
        {
            "ListingId": [1, 1, 1, 1],
            "TimeBucket": [
                "2025-01-01 10:00:00",
                "2025-01-01 10:01:00",
                "2025-01-01 10:02:00",
                "2025-01-01 10:03:00",
            ],
            "MetricA": [1.0, 2.0, 3.0, 4.0],
            "MetricB": [2.0, 4.0, 6.0, 8.0],
        }
    )
    cfg = CorrelationAnalyticsConfig(
        source_pass="pass1",
        columns=["MetricA", "MetricB"],
        group_by=["ListingId"],
        output_format="rows",
    )
    mod = CorrelationAnalytics(cfg)
    mod.context = {"pass1": df}
    out = mod.compute().collect()
    assert "Corr" in out.columns
    assert out.height > 0


def test_correlation_soft_rows_output():
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
            "MetricA": [1.0, 2.0, 3.0, 4.0, 5.0],
            "MetricB": [2.0, 1.5, 3.0, 3.5, 4.0],
        }
    )
    cfg = CorrelationAnalyticsConfig(
        source_pass="pass1",
        columns=["MetricA", "MetricB"],
        group_by=["ListingId"],
        output_format="rows",
        method="soft",
        soft_alpha=1.0,
        soft_sign_only=False,
    )
    mod = CorrelationAnalytics(cfg)
    mod.context = {"pass1": df}
    out = mod.compute().collect()
    assert "Corr" in out.columns
    assert out.height > 0
