from __future__ import annotations

import datetime as dt
import os
import sys
import types

import pandas as pd
import polars as pl
import pytest
from polars.exceptions import ComputeError

from basalt import utils


def test_dc_merges_suffix_columns():
    lf = pl.DataFrame(
        {
            "ListingId": [None],
            "ListingId_r": [1],
            "TimeBucket": [dt.datetime(2025, 1, 1, 10, 0)],
            "TimeBucket_r": [dt.datetime(2025, 1, 1, 10, 0)],
            "MIC": [None],
            "MIC_r": ["XPAR"],
            "Ticker": [None],
            "Ticker_r": ["ABC"],
        }
    ).lazy()

    out = utils.dc(lf, "_r").collect()
    assert out["ListingId"][0] == 1
    assert out["MIC"][0] == "XPAR"
    assert "ListingId_r" not in out.columns


def test_is_s3_path_and_retryable_error_message():
    assert utils.is_s3_path("s3://bucket/path")
    assert not utils.is_s3_path("/tmp/file")
    assert utils.is_retryable_s3_error(ComputeError("S3 RequestTimeout"))
    assert not utils.is_retryable_s3_error(Exception("other"))


def test_retry_s3_retries_then_succeeds(monkeypatch):
    calls = {"n": 0}
    slept = []

    def _fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ComputeError("S3 RequestTimeout")
        return "ok"

    monkeypatch.setattr(utils.time, "sleep", lambda d: slept.append(d))
    out = utils.retry_s3(_fn, attempts=4, base_delay=1.0, factor=2.0, max_delay=5.0)
    assert out == "ok"
    assert slept == [1.0, 2.0]


def test_filter_existing_s3_files_passthrough_for_non_s3():
    paths = ["/tmp/a.parquet", "/tmp/b.parquet"]
    assert utils.filter_existing_s3_files(paths) == paths


def test_filter_existing_s3_files_with_fsspec(monkeypatch):
    class _FS:
        def exists(self, path):
            return path.endswith("ok.parquet")

    fake_fsspec = types.SimpleNamespace(filesystem=lambda *_a, **_k: _FS())
    monkeypatch.setitem(sys.modules, "fsspec", fake_fsspec)

    paths = ["s3://b/a-ok.parquet", "s3://b/a-missing.parquet"]
    out = utils.filter_existing_s3_files(paths)
    assert out == ["s3://b/a-ok.parquet"]


def test_get_total_system_memory_gb_sysconf_and_not_implemented(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(os, "sysconf", lambda key: 1024 if key == "SC_PAGE_SIZE" else 1024)
    gb = utils.get_total_system_memory_gb()
    assert gb > 0

    monkeypatch.setattr(sys, "platform", "win32")
    with pytest.raises(NotImplementedError):
        utils.get_total_system_memory_gb()


def test_preload_runs_table_load_and_postprocess(monkeypatch):
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace())

    class _Table:
        name = "t1"

        def load(self, markets, sd, ed):
            assert markets == ["XPAR"]
            return pl.DataFrame({"a": [1]}).lazy()

        def post_load_process(self, lf, ref, ns, **kwargs):
            assert ns == 1_000
            return lf.with_columns(pl.lit(2).alias("b"))

    ref = pl.DataFrame({"MIC": ["XPAR"]})
    out = utils.preload(
        pd.Timestamp("2025-01-01"),
        pd.Timestamp("2025-01-01"),
        ref,
        1_000,
        [_Table()],
    )
    assert "t1" in out
    assert out["t1"].collect()["b"][0] == 2


def test_ffill_with_shifts_expands_and_fills():
    df = pl.DataFrame(
        {
            "ListingId": [1, 1],
            "TimeBucket": [
                dt.datetime(2025, 1, 1, 10, 0, 0),
                dt.datetime(2025, 1, 1, 10, 0, 2),
            ],
            "x": [10, 20],
        }
    ).lazy()
    out = utils.ffill_with_shifts(
        df,
        group_cols=["ListingId"],
        time_col="TimeBucket",
        value_cols=["x"],
        shifts=[dt.timedelta(seconds=1)],
    ).collect()

    assert out.height == 4
    assert out.filter(pl.col("TimeBucket") == dt.datetime(2025, 1, 1, 10, 0, 1))["x"][0] == 10


def test_assert_unique_lazy_pass_and_fail():
    good = pl.DataFrame({"A": [1, 2], "B": [3, 4]}).lazy()
    utils.assert_unique_lazy(good, ["A"]).collect()

    bad = pl.DataFrame({"A": [1, 1], "B": [3, 4]}).lazy()
    with pytest.raises(AssertionError, match="Duplicate keys"):
        utils.assert_unique_lazy(bad, ["A"], name="bad").collect()


def test_normalize_float_df_and_lf():
    df = pl.DataFrame({"x": [1.0000000000001, 2.345678901], "y": [1, 2]})
    out_df = utils.normalize_float_df(df, decimals=6, eps=1e-9)
    assert out_df["x"][0] == pytest.approx(1.0)

    out_lf = utils.normalize_float_lf(df.lazy(), decimals=6, eps=1e-9).collect()
    assert out_lf["x"][0] == pytest.approx(1.0)


def test_normalize_float_fast_paths():
    empty = pl.DataFrame({"x": []}, schema={"x": pl.Float64})
    assert utils.normalize_float_df(empty).is_empty()

    no_float = pl.DataFrame({"x": [1, 2]}).lazy()
    assert utils.normalize_float_lf(no_float).collect().shape == (2, 1)


def test_generate_path_and_get_files_for_date_range(monkeypatch):
    fake_bmll2 = types.SimpleNamespace(
        _configure=types.SimpleNamespace(L2_ACCESS_POINT_ALIAS="alias")
    )
    monkeypatch.setitem(sys.modules, "bmll2", fake_bmll2)

    paths = utils.generate_path(["XPAR"], 2025, 1, 2, "l2")
    assert paths[0].startswith("s3://alias/")

    with pytest.raises(ValueError, match="Unknown table name"):
        utils.generate_path(["XPAR"], 2025, 1, 2, "missing")

    files = utils.get_files_for_date_range(
        pd.Timestamp("2025-01-03"),
        pd.Timestamp("2025-01-06"),
        ["XPAR"],
        "l2",
        exclude_weekends=True,
    )
    # Fri + Mon only when weekends are excluded
    assert len(files) == 2


def test_get_s3_files(monkeypatch):
    fake_bmll2 = types.SimpleNamespace(
        _configure=types.SimpleNamespace(L2_ACCESS_POINT_ALIAS="alias")
    )
    monkeypatch.setitem(sys.modules, "bmll2", fake_bmll2)

    ref = pl.DataFrame({"MIC": ["XPAR"]})
    out = utils.get_s3_files({"REQUIRES": ["l2"]}, pd.Timestamp("2025-01-02"), ref)
    assert "l2" in out
    assert out["l2"]


def test_cache_universe_supports_lazy_and_pandas(tmp_path, monkeypatch):
    monkeypatch.setenv("INTRADAY_ANALYTICS_TEMP_DIR", str(tmp_path))
    monkeypatch.setitem(sys.modules, "bmll2", types.ModuleType("bmll2"))

    # LazyFrame branch
    def _lazy(date):
        return pl.DataFrame({"ListingId": [1], "MIC": ["X"]}).lazy()

    out1 = utils.cache_universe(None)(_lazy)("2025-01-02")
    assert out1.height == 1

    # Pandas branch (new date -> uncached path)
    def _pandas(date):
        return pd.DataFrame({"ListingId": [2], "MIC": ["Y"]})

    out2 = utils.cache_universe(None)(_pandas)("2025-01-03")
    assert out2.height == 1


def test_create_date_batches_modes_and_errors():
    single = utils.create_date_batches("2025-01-01", "2025-01-05", None)
    assert len(single) == 1

    weekly = utils.create_date_batches("2025-01-01", "2025-03-15", "W")
    assert len(weekly) >= 2
    assert utils.create_date_batches("2025-01-01", "2025-07-15", "2W")
    assert utils.create_date_batches("2025-01-01", "2025-07-15", "M")
    assert utils.create_date_batches("2025-01-01", "2028-01-15", "A")

    with pytest.raises(ValueError, match="Unsupported frequency"):
        utils.create_date_batches("2025-01-01", "2025-03-15", "D")
