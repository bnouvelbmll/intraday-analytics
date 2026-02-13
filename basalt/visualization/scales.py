from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd


ScaleName = Literal["linear", "sqrt", "sgnlog", "percentile"]
ScaleOption = Literal["auto", "linear", "sqrt", "sgnlog", "percentile"]


@dataclass(frozen=True)
class ScaleDecision:
    selected: ScaleName
    reason: str


def choose_scale(values: pd.Series) -> ScaleDecision:
    s = pd.to_numeric(values, errors="coerce").dropna()
    if s.empty:
        return ScaleDecision(selected="linear", reason="empty")

    abs_max = float(s.abs().max())
    if abs_max == 0.0:
        return ScaleDecision(selected="linear", reason="constant_zero")

    has_neg = bool((s < 0).any())
    has_pos = bool((s > 0).any())
    q01 = float(s.quantile(0.01))
    q50 = float(s.quantile(0.50))
    q99 = float(s.quantile(0.99))
    span = abs(q99 - q01)

    # Mixed sign with broad dynamic range is best handled by signed log.
    if has_neg and has_pos and abs_max >= 1e2:
        return ScaleDecision(selected="sgnlog", reason="mixed_sign_wide_range")

    # Non-negative with heavy right tail often benefits from sqrt.
    if not has_neg and q50 > 0:
        skew_ratio = q99 / max(q50, 1e-12)
        if skew_ratio >= 15:
            return ScaleDecision(selected="sqrt", reason="positive_heavy_tail")

    # Very broad spans can benefit from percentile for relative ranking.
    if span > 0 and abs_max > 0 and (abs_max / max(abs(q01), 1e-12)) >= 1e4 and len(s) <= 50_000:
        return ScaleDecision(selected="percentile", reason="extreme_dynamic_range")

    return ScaleDecision(selected="linear", reason="default_linear")


def transform_series(values: pd.Series, scale: ScaleName) -> pd.Series:
    s = pd.to_numeric(values, errors="coerce")
    if scale == "linear":
        return s
    if scale == "sqrt":
        return np.sign(s) * np.sqrt(np.abs(s))
    if scale == "sgnlog":
        return np.sign(s) * np.log10(1.0 + np.abs(s))
    # percentile
    return s.rank(pct=True)

