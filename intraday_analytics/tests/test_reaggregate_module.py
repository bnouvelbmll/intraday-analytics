from datetime import datetime

import polars as pl
import pytest

from intraday_analytics.analytics.reaggregate import (
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

    prices = result["MidPrice"].to_list()
    expected_t0 = (100.0 * 1000.0 + 110.0 * 2200.0) / (1000.0 + 2200.0)
    expected_t1 = (105.0 * 525.0 + 95.0 * 1425.0) / (525.0 + 1425.0)
    assert prices[0] == pytest.approx(expected_t0, rel=1e-6)
    assert prices[1] == pytest.approx(expected_t1, rel=1e-6)
