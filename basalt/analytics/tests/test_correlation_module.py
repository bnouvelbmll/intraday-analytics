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
