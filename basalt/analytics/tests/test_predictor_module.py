import polars as pl

from basalt.analytics.predictor import PredictorAnalytics, PredictorAnalyticsConfig


def test_predictor_output():
    df = pl.DataFrame(
        {
            "ListingId": [1] * 50,
            "TimeBucket": [f"2025-01-01 10:{i:02d}:00" for i in range(50)],
            "Target": [float(i) for i in range(50)],
            "PredictorA": [float(i - 1) for i in range(50)],
            "PredictorB": [float(i % 3) for i in range(50)],
        }
    )
    cfg = PredictorAnalyticsConfig(
        source_pass="pass1",
        target_column="Target",
        predictor_columns=["PredictorA", "PredictorB"],
        group_by=["ListingId"],
        lags=[1, 2, 3],
        min_periods=10,
        ENABLED=True,
    )
    mod = PredictorAnalytics(cfg)
    mod.context = {"pass1": df}
    out = mod.compute().collect()
    assert "BestCorr" in out.columns
    assert out.height > 0


def test_predictor_top_k_and_tstat_output():
    df = pl.DataFrame(
        {
            "ListingId": [1] * 50,
            "TimeBucket": [f"2025-01-01 10:{i:02d}:00" for i in range(50)],
            "Target": [float(i) for i in range(50)],
            "PredictorA": [float(i - 1) for i in range(50)],
            "PredictorB": [float(i % 3) for i in range(50)],
            "PredictorC": [float(i % 7) for i in range(50)],
        }
    )
    cfg = PredictorAnalyticsConfig(
        source_pass="pass1",
        target_column="Target",
        predictor_columns=["PredictorA", "PredictorB", "PredictorC"],
        group_by=["ListingId"],
        lags=[1, 2, 3],
        min_periods=10,
        top_k=2,
        include_t_value=True,
        ENABLED=True,
    )
    mod = PredictorAnalytics(cfg)
    mod.context = {"pass1": df}
    out = mod.compute().collect()
    assert "TStatistic" in out.columns
    assert out.height == 2
