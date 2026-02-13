from __future__ import annotations

from types import SimpleNamespace

import polars as pl
import pytest

import basalt.orchestrator as orch
from basalt.configuration import PassConfig


class _DummyTable:
    def get_transform_fn(self, *_args, **_kwargs):
        return lambda lf: lf

    def load_from_source(self, source, **_kwargs):
        if source == "bmll":
            return pl.DataFrame({"ListingId": [1], "TimeBucket": [1]}).lazy()
        if source == "snowflake":
            return pl.DataFrame({"ListingId": [1], "TimeBucket": [1]}).lazy()
        raise NotImplementedError("x")


def test_coerce_to_iso_date():
    assert orch._coerce_to_iso_date("2026-01-02 00:00:00") == "2026-01-02"
    assert orch._coerce_to_iso_date("bad") == "bad"
    assert orch._coerce_to_iso_date(1) == 1


def test_normalized_data_source_path():
    cfg = SimpleNamespace(DATA_SOURCE_PATH=["BMll", "snowflake", "bmll", ""])
    assert orch._normalized_data_source_path(cfg) == ["bmll", "snowflake"]
    cfg2 = SimpleNamespace(DATA_SOURCE_PATH=[])
    assert orch._normalized_data_source_path(cfg2) == ["bmll"]


def test_load_table_lazy_with_fallback(monkeypatch):
    monkeypatch.setitem(orch.ALL_TABLES, "x", _DummyTable())
    pass_cfg = PassConfig(name="p1")
    ref = pl.DataFrame({"MIC": ["X"], "ListingId": [1]})

    monkeypatch.setattr(orch, "get_files_for_date_range", lambda *a, **k: [])
    cfg = SimpleNamespace(DATA_SOURCE_PATH=["bmll", "snowflake"], EXCLUDE_WEEKENDS=False)
    lf, source = orch._load_table_lazy_with_fallback(
        table_name="x",
        config=cfg,
        pass_config=pass_cfg,
        current_date="2026-01-01",
        ref=ref,
        nanoseconds=1_000_000_000,
    )
    assert source == "bmll"
    assert lf is not None


def test_resolve_module_names_and_group_map(monkeypatch):
    pass_cfg = PassConfig(name="p1", modules=["trade", "iceberg"])
    pass_cfg.trade_analytics.use_tagged_trades = True
    names = orch._resolve_module_names(pass_cfg)
    assert names.index("iceberg") < names.index("trade")

    ref = pl.DataFrame({"ListingId": [1], "InstrumentId": [10]})
    monkeypatch.setattr(orch, "resolve_batch_group_by", lambda _m: "InstrumentId")
    out = orch._resolve_batch_group_map(pass_cfg, ref)
    assert out == {1: 10}


def test_derive_tables_to_load_and_call_get_pipeline(monkeypatch):
    class _Mod:
        REQUIRES = ["l2", "reference"]

    fake_entries = {"l2": SimpleNamespace(cls=_Mod)}
    monkeypatch.setattr("basalt.analytics_registry.get_registered_entries", lambda: fake_entries)
    pass_cfg = PassConfig(name="p1", modules=["l2"])
    tables = orch._derive_tables_to_load(pass_cfg, user_tables=["trades"])
    assert "trades" in tables and "l2" in tables and "reference" in tables

    def f1(a, b):
        return a + b

    def f2(**kwargs):
        return kwargs["a"] + kwargs["b"]

    assert orch._call_get_pipeline(f1, a=1, b=2, c=3) == 3
    assert orch._call_get_pipeline(f2, a=1, b=2, c=3) == 3


def test_derive_tables_to_load_invalid_override(monkeypatch):
    class _Mod:
        REQUIRES = ["l2", "reference"]

    monkeypatch.setattr(
        "basalt.analytics_registry.get_registered_entries",
        lambda: {"l2": SimpleNamespace(cls=_Mod)},
    )
    pass_cfg = PassConfig(name="p1", modules=["l2"], module_inputs={"l2": "l2"})
    with pytest.raises(ValueError, match="must be a mapping"):
        orch._derive_tables_to_load(pass_cfg, user_tables=[])
