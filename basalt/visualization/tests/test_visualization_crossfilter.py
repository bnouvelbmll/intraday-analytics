from __future__ import annotations

import polars as pl

from basalt.visualization.crossfilter import (
    RangeConstraint,
    SetConstraint,
    apply_constraints,
    constraints_to_mongo,
    constraints_to_predicates,
)


def test_constraints_to_mongo_json_syntax():
    constraints = {
        "x": RangeConstraint(column="x", min_value=1.5, max_value=3.5),
        "MarketState": SetConstraint(column="MarketState", values=["CONTINUOUS_TRADING"]),
    }
    doc = constraints_to_mongo(constraints)
    assert doc["x"]["$gte"] == 1.5
    assert doc["x"]["$lte"] == 3.5
    assert doc["MarketState"]["$in"] == ["CONTINUOUS_TRADING"]


def test_apply_constraints_with_excluded_columns():
    df = pl.DataFrame(
        {
            "x": [1.0, 2.0, 3.0, 4.0],
            "MarketState": ["A", "B", "A", "B"],
        }
    )
    constraints = {
        "x": RangeConstraint(column="x", min_value=2.0, max_value=3.0),
        "MarketState": SetConstraint(column="MarketState", values=["A"]),
    }
    out = apply_constraints(df, constraints, exclude_columns={"x"})
    assert out["x"].to_list() == [1.0, 3.0]
    assert out["MarketState"].to_list() == ["A", "A"]


def test_constraints_to_predicates():
    constraints = {
        "x": RangeConstraint(column="x", min_value=2.0, max_value=3.0),
        "MarketState": SetConstraint(column="MarketState", values=["A"]),
    }
    predicates = constraints_to_predicates(constraints)
    assert ["x", ">=", 2.0] in predicates
    assert ["x", "<=", 3.0] in predicates
    assert ["MarketState", "in", ["A"]] in predicates
