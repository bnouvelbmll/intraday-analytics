from __future__ import annotations

import polars as pl

import basalt.visualization.scoring as scoring
from basalt.visualization.scoring import (
    score_numeric_columns,
    suggest_feature_target_associations,
    top_interesting_columns,
)


def test_score_numeric_columns_prefers_non_constant():
    df = pl.DataFrame(
        {
            "const": [1.0] * 100,
            "trend": [float(i) for i in range(100)],
            "noise": [float((i * 7) % 19) for i in range(100)],
        }
    )
    scores = score_numeric_columns(df)
    names = [s.column for s in scores]
    assert "trend" in names
    assert "noise" in names
    assert names[0] in {"trend", "noise"}


def test_top_interesting_columns_limit():
    df = pl.DataFrame(
        {
            "a": [0.0, 1.0, 2.0, 3.0, 4.0],
            "b": [1.0, 1.0, 1.0, 1.0, 1.0],
            "c": [2.0, 3.0, 4.0, 5.0, 6.0],
        }
    )
    top = top_interesting_columns(df, limit=1)
    assert len(top) == 1


def test_suggest_feature_target_associations_filters_by_range(monkeypatch):
    def _fake_mi(x_pdf, y, random_state=0):
        _ = (x_pdf, y, random_state)
        return [0.2, 0.7, 1.0]

    monkeypatch.setattr(scoring, "_mutual_info_regression", _fake_mi)
    df = pl.DataFrame(
        {
            "target": [float(i) for i in range(20)],
            "f1": [float(i) for i in range(20)],
            "f2": [float(i % 3) for i in range(20)],
            "f3": [float(i % 7) for i in range(20)],
        }
    )
    out, warn = suggest_feature_target_associations(
        df,
        target="target",
        min_score=0.6,
        max_score=0.8,
        max_suggestions=5,
    )
    assert warn is None
    assert [x.feature for x in out] == ["f2"]
    assert 0.6 <= out[0].score <= 0.8


def test_suggest_feature_target_associations_handles_missing_sklearn(monkeypatch):
    monkeypatch.setattr(scoring, "_mutual_info_regression", None)
    df = pl.DataFrame({"target": [1.0, 2.0, 3.0], "f1": [1.0, 2.0, 3.0]})
    out, warn = suggest_feature_target_associations(df, target="target")
    assert out == []
    assert "scikit-learn" in (warn or "")
