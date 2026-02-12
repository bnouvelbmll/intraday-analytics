from __future__ import annotations

import importlib
import sys
import types

import polars as pl


def _install_fake_bmll(monkeypatch):
    calls = {}
    bmll_module = types.ModuleType("bmll")
    reference_module = types.ModuleType("bmll.reference")

    def fake_query(**kwargs):
        calls["kwargs"] = kwargs
        return [{"InstrumentId": 1, "MIC": "XPAR"}]

    reference_module.query = fake_query
    bmll_module.reference = reference_module
    monkeypatch.setitem(sys.modules, "bmll", bmll_module)
    monkeypatch.setitem(sys.modules, "bmll.reference", reference_module)
    return calls


def _assert_universe_module(monkeypatch, module_name, field, value, error_text):
    calls = _install_fake_bmll(monkeypatch)
    module = importlib.import_module(module_name)
    module = importlib.reload(module)

    df = module.get_universe("2025-01-02", value)

    assert isinstance(df, pl.DataFrame)
    assert df.height == 1
    assert calls["kwargs"] == {
        "object_type": "Instrument",
        "start_date": "2025-01-02",
        field: value,
        "IsAlive": True,
    }

    try:
        module.get_universe("2025-01-02", "")
        assert False, "Expected ValueError when universe selector is empty."
    except ValueError as exc:
        assert error_text in str(exc)


def test_get_universe_index_dispatches_to_reference_query(monkeypatch):
    _assert_universe_module(
        monkeypatch,
        "basalt.universes.index",
        "Index",
        "CAC40",
        "Universe Index",
    )


def test_get_universe_mic_dispatches_to_reference_query(monkeypatch):
    _assert_universe_module(
        monkeypatch,
        "basalt.universes.mic",
        "MIC",
        "XPAR",
        "Universe MIC",
    )


def test_get_universe_opol_dispatches_to_reference_query(monkeypatch):
    _assert_universe_module(
        monkeypatch,
        "basalt.universes.opol",
        "OPOL",
        "PAR",
        "Universe OPOL",
    )
