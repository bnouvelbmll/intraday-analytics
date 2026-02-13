from __future__ import annotations

import datetime as dt

import polars as pl
import pytest

from basalt.tables import DataTable, _filter_and_bucket, _timebucket_expr


class _DummyTable(DataTable):
    name = "dummy"
    bmll_table_name = "dummy-table"
    timestamp_col = "Timestamp"
    s3_folder_name = "Dummy"
    s3_file_prefix = "dummy-"

    def load(self, markets, start_date, end_date):
        return pl.DataFrame({"ListingId": [1], "Timestamp": [dt.datetime(2026, 1, 1)]}).lazy()

    def get_transform_fn(self, ref, nanoseconds, time_bucket_anchor="end", time_bucket_closed="right"):
        return lambda lf: self.post_load_process(lf, ref, nanoseconds, time_bucket_anchor, time_bucket_closed)


def test_timebucket_expr_anchor_closed_combinations():
    df = pl.DataFrame({"ts": [dt.datetime(2026, 1, 1, 12, 0, 0)]}).with_columns(pl.col("ts").cast(pl.Datetime("ns")))
    for anchor in ("start", "end"):
        for closed in ("left", "right"):
            out = df.select(_timebucket_expr(pl.col("ts"), 1_000_000_000, anchor, closed).alias("tb"))
            assert out.height == 1


def test_filter_and_bucket_with_alias_and_source_id():
    lf = pl.DataFrame(
            {
                "BMLLObjectId": [1, 2],
                "TradeTimestamp": [dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 1)],
            }
        ).with_columns(pl.col("TradeTimestamp").cast(pl.Datetime("ns"))).lazy()
    ref = pl.DataFrame({"ListingId": [1]})
    out = _filter_and_bucket(
        lf,
        ref=ref,
        nanoseconds=1_000_000_000,
        time_bucket_anchor="end",
        time_bucket_closed="right",
        timestamp_col="Timestamp",
        source_id_col="BMLLObjectId",
        timestamp_aliases=["TradeTimestamp"],
    ).collect()
    assert out.height == 1
    assert "TimeBucket" in out.columns


def test_data_table_load_from_source_errors():
    t = _DummyTable()
    ref = pl.DataFrame({"ListingId": [1], "MIC": ["X"]})
    with pytest.raises(NotImplementedError):
        t.load_from_source(
            "snowflake",
            markets=["X"],
            start_date="2026-01-01",
            end_date="2026-01-01",
            ref=ref,
            nanoseconds=1_000_000_000,
        )
    with pytest.raises(NotImplementedError):
        t.load_from_source(
            "databricks",
            markets=["X"],
            start_date="2026-01-01",
            end_date="2026-01-01",
            ref=ref,
            nanoseconds=1_000_000_000,
        )
    with pytest.raises(ValueError, match="Unknown data source mechanism"):
        t.load_from_source(
            "unknown",
            markets=["X"],
            start_date="2026-01-01",
            end_date="2026-01-01",
            ref=ref,
            nanoseconds=1_000_000_000,
        )
