import polars as pl

from intraday_analytics.batching import (
    HeuristicBatchingStrategy,
    PolarsScanBatchingStrategy,
    shard_lazyframes_to_batches,
)


class _FakeEstimator:
    def __init__(self, estimates):
        self._estimates = estimates

    def get_estimates_for_symbols(self, symbols):
        return self._estimates


def test_heuristic_grouped_batching():
    symbols = ["L1", "L2", "L3"]
    group_map = {"L1": "I1", "L2": "I1", "L3": "I2"}
    estimates = {"l2": {"L1": 3, "L2": 4, "L3": 2}}
    strategy = HeuristicBatchingStrategy(_FakeEstimator(estimates), {"l2": 5})
    batches = strategy.create_batches(symbols, group_map=group_map)
    assert batches == [["L1", "L2"], ["L3"]]


def test_polars_grouped_batching():
    df = pl.DataFrame({"ListingId": [1, 1, 2, 3]})
    lf_dict = {"l2": df.lazy()}
    strategy = PolarsScanBatchingStrategy(lf_dict, {"l2": 3})
    group_map = {1: "I1", 2: "I1", 3: "I2"}
    batches = strategy.create_batches([1, 2, 3], group_map=group_map)
    assert batches == [[1, 2], [3]]


def test_grouped_sharding_unsorted(tmp_path):
    l2 = pl.DataFrame(
        {
            "ListingId": [2, 1, 3, 2, 1],
            "EventTimestamp": pl.datetime_range(
                start=pl.datetime(2025, 1, 1, 0, 0, 0),
                end=pl.datetime(2025, 1, 1, 0, 0, 4),
                interval="1s",
                eager=True,
            ),
        }
    )
    trades = pl.DataFrame(
        {
            "ListingId": [2, 1, 3],
            "TradeTimestamp": pl.datetime_range(
                start=pl.datetime(2025, 1, 1, 0, 1, 0),
                end=pl.datetime(2025, 1, 1, 0, 1, 2),
                interval="1s",
                eager=True,
            ),
        }
    )
    lf_dict = {"l2": l2.lazy(), "trades": trades.lazy()}
    batches = [[1, 2], [3]]

    shard_lazyframes_to_batches(
        lf_dict=lf_dict,
        batches=batches,
        temp_dir=str(tmp_path),
        buffer_rows=0,
    )

    batch0_l2 = pl.read_parquet(tmp_path / "batch-l2-0.parquet")
    batch1_l2 = pl.read_parquet(tmp_path / "batch-l2-1.parquet")
    assert set(batch0_l2["ListingId"]) == {1, 2}
    assert set(batch1_l2["ListingId"]) == {3}
    assert batch0_l2["ListingId"].is_sorted()
    for listing_id in batch0_l2["ListingId"].unique().to_list():
        subset = batch0_l2.filter(pl.col("ListingId") == listing_id)
        assert subset["EventTimestamp"].is_sorted()
