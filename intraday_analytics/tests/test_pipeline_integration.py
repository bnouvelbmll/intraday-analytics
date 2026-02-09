import polars as pl
import pytest
from pathlib import Path

from intraday_analytics.analytics.trade import (
    TradeAnalytics,
    TradeAnalyticsConfig,
    TradeGenericConfig,
)
from intraday_analytics.dataset_transforms.aggressive_trades import (
    AggressiveTradesTransform,
)


@pytest.fixture
def trades_plus_fixture() -> pl.LazyFrame:
    """Loads the trades-plus.parquet fixture file."""
    fixture_path = (
        Path(__file__).parent.parent.parent / "sample_fixture" / "trades-plus.parquet"
    )
    if not fixture_path.exists():
        pytest.skip(f"Fixture file not found at {fixture_path}")
    return pl.scan_parquet(fixture_path)


def test_trade_analytics_on_transformed_trades(trades_plus_fixture: pl.LazyFrame):
    """
    Tests running TradeAnalytics on trades transformed into aggressive orders.
    """
    # 1. Transform the raw trades into aggressive orders
    transform = AggressiveTradesTransform(
        execution_size="Size", deltat=1e-3, time_bucket_seconds=60
    )
    aggressive_orders_ldf = transform.transform(trades_plus_fixture)

    assert aggressive_orders_ldf is not None
    aggressive_orders_df = aggressive_orders_ldf.collect()

    # 2. Configure and run TradeAnalytics on the transformed data
    config = TradeAnalyticsConfig(
        generic_metrics=[
            TradeGenericConfig(sides=["Total"], measures=["Volume", "Count"])
        ]
    )

    trade_analytics = TradeAnalytics(config)

    # Manually set the 'trades' attribute to our transformed data
    trade_analytics.trades = aggressive_orders_df.lazy()

    result_ldf = trade_analytics.compute()
    result_df = result_ldf.collect()

    # 3. Validate the results
    assert not result_df.is_empty(), "TradeAnalytics should produce output."

    if "Classification" in aggressive_orders_df.columns:
        expected_df = aggressive_orders_df.filter(
            pl.col("Classification") == "LIT_CONTINUOUS"
        )
    else:
        expected_df = aggressive_orders_df
    expected_trade_count = len(expected_df)
    expected_volume = expected_df["Size"].sum()

    total_computed_count = result_df["TradeTotalCount"].sum()
    total_computed_volume = result_df["TradeTotalVolume"].sum()

    assert (
        total_computed_count == expected_trade_count
    ), "The total count from TradeAnalytics should match the number of aggressive orders."
    assert (
        total_computed_volume == expected_volume
    ), "The total volume from TradeAnalytics should match the sum of sizes of aggressive orders."

    print("\nTradeAnalytics on transformed data test passed.")
    print(f"Number of aggressive orders: {expected_trade_count}")
    print(f"Total volume of aggressive orders: {expected_volume}")
    print("Analytics output sample:")
    print(result_df.head())
