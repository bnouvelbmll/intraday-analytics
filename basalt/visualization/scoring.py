from __future__ import annotations

from dataclasses import dataclass
from math import log

import numpy as np
import polars as pl
import pandas as pd

try:
    from sklearn.feature_selection import mutual_info_regression as _mutual_info_regression
except Exception:
    _mutual_info_regression = None


@dataclass(frozen=True)
class SeriesScore:
    column: str
    entropy_score: float
    variance_score: float
    non_null_ratio: float
    total_score: float


@dataclass(frozen=True)
class FeatureAssociationScore:
    feature: str
    score: float


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


def suggest_feature_target_associations(
    df: pl.DataFrame,
    *,
    target: str,
    min_score: float = 0.6,
    max_score: float = 0.8,
    max_suggestions: int = 5,
    sample_rows: int = 20_000,
    random_seed: int = 0,
) -> tuple[list[FeatureAssociationScore], str | None]:
    """
    Suggest feature/target associations based on normalized mutual information.
    Returns (scores, warning_message).
    """
    if _mutual_info_regression is None:
        return [], "scikit-learn is not installed; mutual information suggestions are unavailable."
    if target not in df.columns:
        return [], f"Target column `{target}` not found."
    if min_score > max_score:
        min_score, max_score = max_score, min_score

    numeric_cols = [
        col
        for col, dtype in zip(df.columns, df.dtypes)
        if dtype.is_numeric() and col != target
    ]
    if not numeric_cols:
        return [], "No numeric feature columns available."

    cols = [target] + numeric_cols
    prepared = (
        df.select([pl.col(c).cast(pl.Float64).alias(c) for c in cols])
        .drop_nulls()
    )
    if prepared.height < 10:
        return [], "Not enough non-null samples to estimate feature-target associations."
    if prepared.height > int(sample_rows):
        prepared = prepared.sample(n=int(sample_rows), seed=int(random_seed))

    pdf = prepared.to_pandas()
    y = pd.to_numeric(pdf[target], errors="coerce").to_numpy()
    x_pdf = pdf[numeric_cols].apply(pd.to_numeric, errors="coerce")
    valid = np.isfinite(y) & np.isfinite(x_pdf.to_numpy()).all(axis=1)
    if valid.sum() < 10:
        return [], "Not enough finite samples to estimate feature-target associations."
    y = y[valid]
    x_pdf = x_pdf.loc[valid]

    scores = np.asarray(
        _mutual_info_regression(x_pdf, y, random_state=int(random_seed)),
        dtype=float,
    )
    if scores.size == 0:
        return [], None
    max_val = float(np.nanmax(scores)) if np.isfinite(scores).any() else 0.0
    if max_val <= 0:
        return [], "Mutual information scores are zero for all candidate features."
    normalized = scores / max_val

    out: list[FeatureAssociationScore] = []
    for feature, score in zip(numeric_cols, normalized):
        val = float(score)
        if not np.isfinite(val):
            continue
        if float(min_score) <= val <= float(max_score):
            out.append(FeatureAssociationScore(feature=feature, score=val))
    out.sort(key=lambda x: x.score, reverse=True)
    return out[: int(max_suggestions)], None
