from __future__ import annotations

from types import SimpleNamespace
import datetime as dt

import polars as pl

from basalt import orchestrator


def test_normalized_data_source_path_defaults_to_bmll():
    cfg = SimpleNamespace(DATA_SOURCE_PATH=None)
    assert orchestrator._normalized_data_source_path(cfg) == ["bmll"]


def test_normalized_data_source_path_dedupes_and_normalizes():
    cfg = SimpleNamespace(DATA_SOURCE_PATH=["BMLL", "snowflake", "bmll", "databricks"])
    assert orchestrator._normalized_data_source_path(cfg) == [
        "bmll",
        "snowflake",
        "databricks",
    ]


def test_load_table_lazy_with_fallback_tries_next_source(monkeypatch):
    class _FakeTable:
        def get_transform_fn(self, *_args, **_kwargs):
            return lambda lf: lf

        def load_from_source(self, source, **_kwargs):
            if source == "snowflake":
                raise NotImplementedError("pending wiring")
            if source == "databricks":
                return pl.DataFrame({"ListingId": [1], "x": [2]}).lazy()
            raise RuntimeError("unexpected source")

    monkeypatch.setitem(orchestrator.ALL_TABLES, "trades", _FakeTable())
    monkeypatch.setattr(
        orchestrator,
        "get_files_for_date_range",
        lambda *_args, **_kwargs: [],
    )

    cfg = SimpleNamespace(
        DATA_SOURCE_PATH=["bmll", "snowflake", "databricks"],
        EXCLUDE_WEEKENDS=True,
    )
    pass_cfg = SimpleNamespace(time_bucket_anchor="end", time_bucket_closed="right")
    ref = pl.DataFrame({"MIC": ["XPAR"], "ListingId": [1]})

    lf, source = orchestrator._load_table_lazy_with_fallback(
        table_name="trades",
        config=cfg,
        pass_config=pass_cfg,
        current_date=dt.datetime(2025, 1, 1),
        ref=ref,
        nanoseconds=1_000_000_000,
    )
    assert lf is not None
    assert source == "databricks"
