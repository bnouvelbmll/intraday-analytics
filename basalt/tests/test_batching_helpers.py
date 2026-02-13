from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl

from basalt.batching import (
    _build_batch_map_table,
    _build_group_members,
    _expand_group_batches,
    _finalize_batches_from_shards,
)


def test_group_helpers():
    group_ids, mapping = _build_group_members(["a", "b", "c"], {"a": "g1", "b": "g1"})
    assert group_ids == ["c", "g1"]
    assert mapping["g1"] == ["a", "b"]
    expanded = _expand_group_batches([["g1"], ["c"]], mapping)
    assert expanded == [["a", "b"], ["c"]]


def test_build_batch_map_table():
    tbl = _build_batch_map_table([["a", "b"], ["c"]])
    df = pl.from_arrow(tbl)
    assert set(df.columns) == {"ListingId", "batch_id"}
    assert df.height == 3


def test_finalize_batches_from_shards(tmp_path):
    temp = Path(tmp_path)
    (temp / "trades" / "0").mkdir(parents=True)
    (temp / "l2" / "0").mkdir(parents=True)
    pl.DataFrame(
        {"ListingId": [1], "TradeTimestamp": [dt.datetime(2026, 1, 1)]}
    ).write_parquet(temp / "trades" / "0" / "x.parquet")
    pl.DataFrame(
        {"ListingId": [1], "EventTimestamp": [dt.datetime(2026, 1, 1)]}
    ).write_parquet(temp / "l2" / "0" / "x.parquet")
    rows = _finalize_batches_from_shards(
        temp_dir=temp,
        table_names=["trades", "l2"],
        storage_options=None,
        batch_count=1,
    )
    assert rows >= 2
    assert (temp / "batch-trades-0.parquet").exists()
