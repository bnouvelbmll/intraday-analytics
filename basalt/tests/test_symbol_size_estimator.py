import sys
import types

import pandas as pd
import polars as pl

from basalt.batching import MAX_BMLL_OBJECT_IDS_PER_QUERY, SymbolSizeEstimator


def _install_fake_bmll(monkeypatch, query_fn):
    time_series = types.SimpleNamespace(query=query_fn)
    bmll_mod = types.SimpleNamespace(time_series=time_series)
    monkeypatch.setitem(sys.modules, "bmll", bmll_mod)


def test_symbol_size_estimator_paginates_object_ids(monkeypatch):
    calls: list[int] = []
    all_ids: list[int] = []

    def _fake_query(*, object_ids, metric, start_date, end_date):
        calls.append(len(object_ids))
        all_ids.extend(object_ids)
        return pd.DataFrame(
            {
                "ObjectId": object_ids,
                "TradeCount|Lit": [10] * len(object_ids),
            }
        )

    _install_fake_bmll(monkeypatch, _fake_query)
    monkeypatch.setattr(
        "basalt.api_stats.api_call",
        lambda _name, fn, extra=None: fn(),
    )

    listing_ids = list(range(1, 25002))
    universe = pl.DataFrame(
        {
            "ListingId": listing_ids,
            "MIC": ["XLON"] * len(listing_ids),
        }
    )

    estimator = SymbolSizeEstimator("2026-01-02", lambda _date: universe)
    out = estimator.get_estimates_for_symbols(listing_ids)

    assert calls == [
        MAX_BMLL_OBJECT_IDS_PER_QUERY,
        MAX_BMLL_OBJECT_IDS_PER_QUERY,
        5001,
    ]
    assert all(isinstance(v, int) for v in all_ids)
    assert "trades" in out
    assert len(out["trades"]) == 25001


def test_symbol_size_estimator_filters_invalid_listing_ids(monkeypatch):
    captured: list[list[int]] = []

    def _fake_query(*, object_ids, metric, start_date, end_date):
        captured.append(list(object_ids))
        return pd.DataFrame(
            {
                "ObjectId": object_ids,
                "TradeCount|Lit": [5] * len(object_ids),
            }
        )

    _install_fake_bmll(monkeypatch, _fake_query)
    monkeypatch.setattr(
        "basalt.api_stats.api_call",
        lambda _name, fn, extra=None: fn(),
    )

    universe = pl.DataFrame(
        {
            "ListingId": pl.Series("ListingId", [1, 2, None, "x", -10, 2], strict=False),
            "MIC": ["XLON"] * 6,
        }
    )

    estimator = SymbolSizeEstimator("2026-01-02", lambda _date: universe)
    out = estimator.get_estimates_for_symbols([1, 2])

    assert captured == [[1, 2]]
    assert out["trades"][1] == 5
    assert out["trades"][2] == 5
