from __future__ import annotations

import polars as pl

from basalt.visualization.scoring import score_numeric_columns, top_interesting_columns


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
