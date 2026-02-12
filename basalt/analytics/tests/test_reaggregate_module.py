from datetime import datetime

import polars as pl
import pytest

from basalt.analytics.reaggregate import (
    ReaggregateAnalytics,
    ReaggregateAnalyticsConfig,
)


def test_reaggregate_module_aggregates_by_group():
    source = pl.DataFrame(
        {
            "ListingId": ["A", "B", "A", "B"],
            "TimeBucket": [
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2025, 1, 1, 0, 1, 0),
                datetime(2025, 1, 1, 0, 1, 0),
            ],
            "TradeTotalVolume": [10, 20, 5, 15],
            "MidPrice": [100.0, 110.0, 105.0, 95.0],
            "TradeNotionalEUR": [1000.0, 2200.0, 525.0, 1425.0],
        }
    )
    group_df = pl.DataFrame(
        {
            "ListingId": ["A", "B"],
            "IndexId": ["IDX1", "IDX1"],
        }
    )

    config = ReaggregateAnalyticsConfig(
        source_pass="pass1",
        group_df_context_key="group_df",
        join_column="ListingId",
        group_column="IndexId",
        group_by=["IndexId", "TimeBucket"],
        weight_col="TradeNotionalEUR",
    )
    module = ReaggregateAnalytics(config)
    module.context = {"pass1": source, "group_df": group_df}

    result = module.compute().collect().sort(["TimeBucket"])

    assert result.shape[0] == 2
    assert set(result["IndexId"].to_list()) == {"IDX1"}

    volumes = result["TradeTotalVolume"].to_list()
    assert volumes == [30, 20]

    notionals = result["TradeNotionalEUR"].to_list()
    assert notionals == [3200.0, 1950.0]

    prices = result["MidPrice"].to_list()
    # Default mode uses daily weights (per ListingId/day), not per-bucket weights.
    a_daily = 1000.0 + 525.0
    b_daily = 2200.0 + 1425.0
    expected_t0 = (100.0 * a_daily + 110.0 * b_daily) / (a_daily + b_daily)
    expected_t1 = (105.0 * a_daily + 95.0 * b_daily) / (a_daily + b_daily)
    assert prices[0] == pytest.approx(expected_t0, rel=1e-6)
    assert prices[1] == pytest.approx(expected_t1, rel=1e-6)


def test_reaggregate_bucket_weight_mode_keeps_legacy_weighting():
    source = pl.DataFrame(
        {
            "ListingId": ["A", "B", "A", "B"],
            "TimeBucket": [
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2025, 1, 1, 0, 1, 0),
                datetime(2025, 1, 1, 0, 1, 0),
            ],
            "MidPrice": [100.0, 110.0, 105.0, 95.0],
            "TradeNotionalEUR": [1000.0, 2200.0, 525.0, 1425.0],
        }
    )
    group_df = pl.DataFrame({"ListingId": ["A", "B"], "IndexId": ["IDX1", "IDX1"]})
    config = ReaggregateAnalyticsConfig(
        source_pass="pass1",
        group_df_context_key="group_df",
        join_column="ListingId",
        group_column="IndexId",
        group_by=["IndexId", "TimeBucket"],
        weight_col="TradeNotionalEUR",
        weight_mode="bucket",
    )
    module = ReaggregateAnalytics(config)
    module.context = {"pass1": source, "group_df": group_df}
    result = module.compute().collect().sort(["TimeBucket"])
    prices = result["MidPrice"].to_list()
    expected_t0 = (100.0 * 1000.0 + 110.0 * 2200.0) / (1000.0 + 2200.0)
    expected_t1 = (105.0 * 525.0 + 95.0 * 1425.0) / (525.0 + 1425.0)
    assert prices[0] == pytest.approx(expected_t0, rel=1e-6)
    assert prices[1] == pytest.approx(expected_t1, rel=1e-6)


def test_reaggregate_drops_not_aggregated_columns():
    source = pl.DataFrame(
        {
            "ListingId": ["A", "B"],
            "TimeBucket": [
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2025, 1, 1, 0, 0, 0),
            ],
            "EventType": ["local_min", "local_max"],
            "MetricX": ["m1", "m2"],
            "TradeTotalVolume": [10, 20],
        }
    )
    group_df = pl.DataFrame(
        {
            "ListingId": ["A", "B"],
            "IndexId": ["IDX1", "IDX1"],
        }
    )
    config = ReaggregateAnalyticsConfig(
        source_pass="pass1",
        group_df_context_key="group_df",
        join_column="ListingId",
        group_column="IndexId",
        group_by=["IndexId", "TimeBucket"],
    )
    module = ReaggregateAnalytics(config)
    module.context = {"pass1": source, "group_df": group_df}

    result = module.compute().collect()
    assert "EventType" not in result.columns
    assert "MetricX" not in result.columns
    assert "TradeTotalVolume" in result.columns


def test_reaggregate_keeps_not_aggregated_when_used_as_group_key():
    source = pl.DataFrame(
        {
            "ListingId": ["A", "B"],
            "TimeBucket": [
                datetime(2025, 1, 1, 0, 0, 0),
                datetime(2025, 1, 1, 0, 0, 0),
            ],
            "EventType": ["local_min", "local_max"],
            "TradeTotalVolume": [10, 20],
        }
    )
    group_df = pl.DataFrame(
        {
            "ListingId": ["A", "B"],
            "IndexId": ["IDX1", "IDX1"],
        }
    )
    config = ReaggregateAnalyticsConfig(
        source_pass="pass1",
        group_df_context_key="group_df",
        join_column="ListingId",
        group_column="IndexId",
        group_by=["IndexId", "TimeBucket", "EventType"],
    )
    module = ReaggregateAnalytics(config)
    module.context = {"pass1": source, "group_df": group_df}

    result = module.compute().collect().sort(["EventType"])
    assert "EventType" in result.columns
    assert result["EventType"].to_list() == ["local_max", "local_min"]
    assert result["TradeTotalVolume"].to_list() == [20, 10]
