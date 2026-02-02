import polars as pl
from typing import Optional, List, Dict, Any


def apply_market_state_filter(
    expr: pl.Expr, market_states: Optional[List[str]]
) -> pl.Expr:
    """
    Applies a MarketState filter to a Polars expression.
    """
    if market_states:
        return expr.filter(pl.col("MarketState").is_in(market_states))
    return expr


def apply_alias(
    expr: pl.Expr, pattern: Optional[str], variant: Dict[str, Any], default_name: str
) -> pl.Expr:
    """
    Applies an alias to a Polars expression based on a pattern or default name.
    """
    alias = pattern.format(**variant) if pattern else default_name
    return expr.alias(alias)


class MetricGenerator:
    """
    Helper class to generate metrics from configuration.
    """

    def __init__(self, base_df: pl.LazyFrame):
        self.base_df = base_df

    def generate(self, config_list: List[Any], handler_func) -> List[pl.Expr]:
        """
        Iterates over a list of metric configurations, expands variants,
        and calls the handler function to generate expressions.
        """
        expressions = []
        for req in config_list:
            # Note: Filtering logic should be handled within the handler or applied to the expression
            # because we are building a list of expressions to be aggregated in one go.

            for variant in req.expand():
                expr = handler_func(req, variant)
                if expr is not None:
                    if isinstance(expr, list):
                        expressions.extend(expr)
                    else:
                        # Apply MarketState filter if present in config
                        # Assuming 'market_states' is a field in the config object 'req'
                        # Note: Handlers in TradeAnalytics now handle filtering internally
                        # because aggregations like sum() need filtering *before* aggregation.
                        # So we might not need to do anything here for TradeAnalytics.
                        # But for other analytics where we return pre-aggregated expressions, we might.
                        # For now, we assume handlers return fully formed expressions.
                        expressions.append(expr)
        return expressions
