from typing import Any, Dict, List, Optional

import polars as pl


def apply_market_state_filter(
    expr: pl.Expr, market_states: Optional[List[str]]
) -> pl.Expr:
    """
    Apply a MarketState filter to a Polars expression.
    """
    if market_states:
        return expr.filter(pl.col("MarketState").is_in(market_states))
    return expr


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
