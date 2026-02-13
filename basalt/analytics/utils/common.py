from typing import Any, Callable, Dict, List, Optional

import polars as pl
from basalt.analytics_base import apply_aggregation


def apply_market_state_filter(
    expr: pl.Expr, market_states: Optional[List[str]]
) -> pl.Expr:
    """
    Apply a MarketState filter to a Polars expression.
    """
    if market_states:
        return expr.filter(pl.col("MarketState").is_in(market_states))
    return expr


def combine_conditions(*conditions: Optional[pl.Expr]) -> pl.Expr:
    """
    Combine optional boolean expressions with logical AND.
    """
    cond = pl.lit(True)
    for item in conditions:
        if item is not None:
            cond = cond & item
    return cond


def apply_alias(
    expr: pl.Expr,
    pattern: Optional[str],
    variant: Dict[str, Any],
    default_name: str,
    prefix: Optional[str] = None,
) -> pl.Expr:
    """
    Apply an alias to a Polars expression based on a pattern or default name.
    """
    alias = pattern.format(**variant) if pattern else default_name
    if prefix:
        alias = f"{prefix}{alias}"
    return expr.alias(alias)


def resolve_output_name(
    *,
    output_name_pattern: Optional[str],
    variant: Dict[str, Any],
    default_name: str,
    prefix: Optional[str] = None,
) -> str:
    """
    Resolve output name from optional pattern, then apply optional prefix.
    """
    alias = (
        output_name_pattern.format(**variant)
        if output_name_pattern
        else default_name
    )
    if prefix:
        alias = f"{prefix}{alias}"
    return alias


def build_aggregated_outputs(
    *,
    expr: pl.Expr,
    aggregations: List[str],
    output_name_pattern: Optional[str],
    variant: Dict[str, Any],
    default_name_for_agg: Callable[[str], str],
    prefix: Optional[str] = None,
) -> List[pl.Expr]:
    """
    Build aliased aggregate expressions from a base expression.
    """
    outputs: List[pl.Expr] = []
    for agg in aggregations:
        agg_expr = apply_aggregation(expr, agg)
        if agg_expr is None:
            continue
        outputs.append(
            apply_alias(
                agg_expr,
                output_name_pattern,
                {**variant, "agg": agg},
                default_name_for_agg(agg),
                prefix=prefix,
            )
        )
    return outputs


class MetricGenerator:
    """
    Helper class to generate metrics from configuration.
    """

    def __init__(self, base_df: pl.LazyFrame):
        self.base_df = base_df

    def generate(self, config_list: List[Any], expression_func) -> List[pl.Expr]:
        """
        Iterate over config requests, expand variants, and build expressions.
        """
        expressions = []
        for req in config_list:
            for variant in req.expand():
                expr = expression_func(req, variant)
                if expr is not None:
                    if isinstance(expr, list):
                        expressions.extend(expr)
                    else:
                        expressions.append(expr)
        return expressions
