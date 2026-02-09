import polars as pl
import pytest
from pathlib import Path

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


@pytest.fixture
def l3_fixture() -> pl.LazyFrame:
    """Loads the l3.parquet fixture file."""
    fixture_path = Path(__file__).parent.parent.parent / "sample_fixture" / "l3.parquet"
    if not fixture_path.exists():
        pytest.skip(f"Fixture file not found at {fixture_path}")
    return pl.scan_parquet(fixture_path)


def test_aggressive_trades_transform_with_trades_plus_fixture(
    trades_plus_fixture: pl.LazyFrame,
):
    """
    Tests the AggressiveTradesTransform using the real trades-plus.parquet fixture.
    """
    transform = AggressiveTradesTransform(execution_size="Size", deltat=1e-3)
    result_ldf = transform.transform(trades_plus_fixture)

    assert result_ldf is not None, "Transform should return a LazyFrame, not None."

    result_df = result_ldf.collect()
    input_df = trades_plus_fixture.collect()

    assert not result_df.is_empty(), "The resulting DataFrame should not be empty."
    assert len(result_df) < len(
        input_df
    ), "The number of aggressive orders should be less than the number of raw trades."

    expected_cols = [
        "ConsolidatedTrades",
        "SequenceNumber",
        "TradeTimestamp",
        "Size",
        "Price",
        "AggressorSide",
        "Duration",
        "MinPrice",
        "MaxPrice",
        "NumExecutions",
        "PriceDiff",
    ]
    for col in expected_cols:
        assert (
            col in result_df.columns
        ), f"Expected column '{col}' not found in the output."

    assert (result_df["Size"] > 0).all(), "Aggregated 'Size' should always be positive."
    assert (
        result_df["NumExecutions"] >= 1
    ).all(), "Each aggressive order must consist of at least one execution."
    assert result_df["TradeTimestamp"].is_in(input_df["TradeTimestamp"].implode()).all()

    if "BMLLParticipantType" in result_df.columns:
        assert (
            result_df["BMLLParticipantType"].dtype == pl.String
        ), "Categorical columns like 'BMLLParticipantType' should remain scalar for downstream analytics."


def test_aggressive_trades_transform_with_l3_fixture(l3_fixture: pl.LazyFrame):
    """
    Tests the AggressiveTradesTransform using the l3.parquet fixture to ensure it
    can handle L3 data as input.
    """
    transform = AggressiveTradesTransform(execution_size="ExecutionSize", deltat=1e-3)
    result_ldf = transform.transform(l3_fixture)

    assert result_ldf is not None, "Transform should return a LazyFrame for L3 data."

    result_df = result_ldf.collect()
    input_df = l3_fixture.filter(pl.col("ExecutionSize") > 0).collect()

    assert (
        not result_df.is_empty()
    ), "The resulting DataFrame from L3 data should not be empty."
    assert len(result_df) <= len(
        input_df
    ), "The number of aggressive orders should be less than or equal to the number of raw L3 executions."

    expected_cols = [
        "ConsolidatedTrades",
        "SequenceNumber",
        "EventTimestamp",
        "ExecutionSize",
        "Price",
        "AggressorSide",
        "Duration",
        "MinPrice",
        "MaxPrice",
        "NumExecutions",
        "PriceDiff",
    ]
    for col in expected_cols:
        assert (
            col in result_df.columns
        ), f"Expected column '{col}' not found in the L3 output."

    assert (
        result_df["ExecutionSize"] > 0
    ).all(), "Aggregated 'ExecutionSize' should always be positive."
    assert (
        result_df["NumExecutions"] >= 1
    ).all(), "Each aggressive order must consist of at least one execution."
