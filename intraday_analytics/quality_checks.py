from __future__ import annotations

import logging
from typing import Iterable

import polars as pl

from intraday_analytics.configuration import QualityCheckConfig


def _null_rate(df: pl.DataFrame, column: str) -> float:
    if column not in df.columns:
        return 0.0
    total = df.height
    if total == 0:
        return 0.0
    return df.select(pl.col(column).is_null().sum()).item() / total


def run_quality_checks(
    df: pl.DataFrame,
    config: QualityCheckConfig,
    columns: Iterable[str] | None = None,
) -> list[str]:
    """
    Run basic quality checks (null rate + range bounds).
    """
    if not config.ENABLED:
        return []
    problems: list[str] = []
    cols = list(columns) if columns is not None else df.columns
    for col in cols:
        rate = _null_rate(df, col)
        if rate > config.null_rate_max:
            problems.append(f"{col}: null_rate={rate:.3f} > {config.null_rate_max:.3f}")
        if col in config.ranges:
            bounds = config.ranges[col]
            if not isinstance(bounds, (list, tuple)) or len(bounds) != 2:
                continue
            low, high = bounds
            series = df.get_column(col)
            if series.is_empty():
                continue
            min_val = series.min()
            max_val = series.max()
            if min_val is not None and min_val < low:
                problems.append(f"{col}: min={min_val} < {low}")
            if max_val is not None and max_val > high:
                problems.append(f"{col}: max={max_val} > {high}")
    return problems


def emit_quality_results(problems: list[str], config: QualityCheckConfig) -> None:
    if not problems:
        return
    message = "Quality checks failed: " + "; ".join(problems)
    if config.action == "raise":
        raise RuntimeError(message)
    logging.warning(message)
