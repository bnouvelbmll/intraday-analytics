from __future__ import annotations

from dataclasses import dataclass
from math import log
from typing import Any

import numpy as np
import polars as pl


@dataclass(frozen=True)
class SeriesScore:
    column: str
    entropy_score: float
    variance_score: float
    non_null_ratio: float
    total_score: float


def _normalized_entropy(values: np.ndarray) -> float:
    if values.size < 2:
        return 0.0
    bins = max(5, min(64, int(np.sqrt(values.size))))
    hist, _ = np.histogram(values, bins=bins)
    probs = hist.astype(float) / float(hist.sum() or 1.0)
    probs = probs[probs > 0]
    if probs.size == 0:
        return 0.0
    entropy = float(-(probs * np.log(probs)).sum())
    max_entropy = float(log(bins)) if bins > 1 else 1.0
    if max_entropy <= 0:
        return 0.0
    return entropy / max_entropy


def score_numeric_columns(df: pl.DataFrame) -> list[SeriesScore]:
    out: list[SeriesScore] = []
    for col, dtype in zip(df.columns, df.dtypes):
        if not dtype.is_numeric():
            continue
        s = df.get_column(col).cast(pl.Float64)
        n_total = len(s)
        if n_total == 0:
            continue
        nn = s.drop_nulls()
        n_non_null = len(nn)
        if n_non_null < 3:
            continue
        values = nn.to_numpy()
        finite = np.isfinite(values)
        values = values[finite]
        if values.size < 3:
            continue
        entropy = _normalized_entropy(values)
        variance = float(np.nanstd(values))
        variance_score = np.log1p(max(variance, 0.0))
        non_null_ratio = float(n_non_null) / float(n_total)
        total = entropy * variance_score * non_null_ratio
        out.append(
            SeriesScore(
                column=col,
                entropy_score=entropy,
                variance_score=variance_score,
                non_null_ratio=non_null_ratio,
                total_score=total,
            )
        )
    out.sort(key=lambda x: x.total_score, reverse=True)
    return out


def top_interesting_columns(df: pl.DataFrame, limit: int = 12) -> list[str]:
    return [row.column for row in score_numeric_columns(df)[: int(limit)]]

