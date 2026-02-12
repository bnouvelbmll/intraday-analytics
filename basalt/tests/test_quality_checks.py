from __future__ import annotations

import logging

import polars as pl
import pytest

from basalt.configuration import QualityCheckConfig
from basalt.quality_checks import emit_quality_results, run_quality_checks


def test_run_quality_checks_returns_empty_when_disabled():
    df = pl.DataFrame({"x": [None, 1]})
    cfg = QualityCheckConfig(ENABLED=False)
    assert run_quality_checks(df, cfg) == []


def test_run_quality_checks_reports_null_rate_and_range_violations():
    df = pl.DataFrame({"x": [None, 1.0, 2.0], "y": [-1, 3, 7]})
    cfg = QualityCheckConfig(
        ENABLED=True,
        null_rate_max=0.2,
        ranges={"y": [0, 5]},
    )

    problems = run_quality_checks(df, cfg)

    assert "x: null_rate=0.333 > 0.200" in problems
    assert "y: min=-1 < 0.0" in problems
    assert "y: max=7 > 5.0" in problems


def test_run_quality_checks_ignores_invalid_range_specs():
    df = pl.DataFrame({"x": [1, 2, 3]})
    cfg = QualityCheckConfig(ENABLED=True, ranges={"x": [0], "z": [1, 2, 3]})
    assert run_quality_checks(df, cfg) == []


def test_emit_quality_results_raises_when_configured():
    cfg = QualityCheckConfig(ENABLED=True, action="raise")
    with pytest.raises(RuntimeError, match="Quality checks failed"):
        emit_quality_results(["x: bad"], cfg)


def test_emit_quality_results_warns_when_configured(caplog):
    cfg = QualityCheckConfig(ENABLED=True, action="warn")
    with caplog.at_level(logging.WARNING):
        emit_quality_results(["x: bad"], cfg)
    assert "Quality checks failed: x: bad" in caplog.text
