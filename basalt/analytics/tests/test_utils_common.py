from __future__ import annotations

import polars as pl

from basalt.analytics.utils.common import (
    build_aggregated_outputs,
    combine_conditions,
    resolve_output_name,
)


def test_resolve_output_name_pattern_and_prefix():
    out = resolve_output_name(
        output_name_pattern="{side}{measure}",
        variant={"side": "Bid", "measure": "Volume"},
        default_name="ignored",
        prefix="P_",
    )
    assert out == "P_BidVolume"


def test_combine_conditions_filters_with_all_clauses():
    df = pl.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
    cond = combine_conditions(pl.col("a") >= 2, pl.col("b") <= 20)
    out = df.filter(cond)
    assert out["a"].to_list() == [2]


def test_build_aggregated_outputs_generates_named_exprs():
    exprs = build_aggregated_outputs(
        expr=pl.col("x"),
        aggregations=["Sum", "Mean"],
        output_name_pattern=None,
        variant={},
        default_name_for_agg=lambda agg: f"Metric{agg}",
        prefix="M_",
    )
    out = pl.DataFrame({"g": [1, 1, 2], "x": [1.0, 3.0, 2.0]}).group_by("g").agg(exprs)
    cols = out.columns
    assert "M_MetricSum" in cols
    assert "M_MetricMean" in cols
