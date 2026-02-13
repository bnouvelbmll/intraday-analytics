from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass
class RangeConstraint:
    column: str
    min_value: float
    max_value: float


@dataclass
class SetConstraint:
    column: str
    values: list[str]


Constraint = RangeConstraint | SetConstraint


def is_discrete_column(df: pl.DataFrame, column: str, *, max_unique: int = 30) -> bool:
    if column not in df.columns or df.is_empty():
        return False
    dtype = df.schema[column]
    if dtype in (pl.Utf8, pl.Categorical, pl.Boolean):
        return True
    n_unique = int(df.select(pl.col(column).drop_nulls().n_unique()).item())
    return n_unique <= max_unique


def apply_constraints(
    df: pl.DataFrame,
    constraints: dict[str, Constraint],
    *,
    exclude_columns: set[str] | None = None,
) -> pl.DataFrame:
    out = df
    excluded = exclude_columns or set()
    for column, constraint in constraints.items():
        if column in excluded or column not in out.columns:
            continue
        if isinstance(constraint, RangeConstraint):
            out = out.filter(
                pl.col(column).cast(pl.Float64).is_between(
                    float(constraint.min_value),
                    float(constraint.max_value),
                    closed="both",
                )
            )
        elif isinstance(constraint, SetConstraint):
            allowed = [str(v) for v in constraint.values]
            if not allowed:
                continue
            out = out.filter(pl.col(column).cast(pl.String).is_in(allowed))
    return out


def numeric_columns(df: pl.DataFrame) -> list[str]:
    out: list[str] = []
    for name, dtype in df.schema.items():
        if dtype.is_numeric():
            out.append(name)
    return out


def candidate_dimensions(df: pl.DataFrame) -> list[str]:
    priority = [
        "TimeBucket",
        "Timestamp",
        "Date",
        "ListingId",
        "InstrumentId",
        "PrimaryListingId",
        "ISIN",
        "Ticker",
        "MarketState",
        "BMLLTradeType",
    ]
    cols = list(df.columns)
    ranked = [c for c in priority if c in cols]
    rest = [c for c in cols if c not in set(ranked)]
    return ranked + rest


def constraints_to_mongo(constraints: dict[str, Constraint]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for column, constraint in constraints.items():
        if isinstance(constraint, RangeConstraint):
            out[column] = {
                "$gte": float(constraint.min_value),
                "$lte": float(constraint.max_value),
            }
        elif isinstance(constraint, SetConstraint):
            out[column] = {"$in": [str(v) for v in constraint.values]}
    return out


def constraints_to_predicates(constraints: dict[str, Constraint]) -> list[list[Any]]:
    predicates: list[list[Any]] = []
    for column, constraint in constraints.items():
        if isinstance(constraint, RangeConstraint):
            predicates.append([column, ">=", float(constraint.min_value)])
            predicates.append([column, "<=", float(constraint.max_value)])
        elif isinstance(constraint, SetConstraint):
            predicates.append([column, "in", [str(v) for v in constraint.values]])
    return predicates
