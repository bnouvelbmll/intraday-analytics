from __future__ import annotations

import polars as pl


SECONDS_IN_TRADING_YEAR = 252 * 24 * 60 * 60


def annualized_std_from_log_returns(
    log_ret_expr: pl.Expr,
    *,
    bucket_seconds: float | None = None,
    sample_seconds: float | None = None,
) -> pl.Expr:
    """
    Annualized volatility from log returns.

    If `sample_seconds` is set, annualization uses sqrt(SECONDS_IN_TRADING_YEAR/sample_seconds).
    Else if `bucket_seconds` is set, annualization uses sqrt(SECONDS_IN_TRADING_YEAR * n / bucket_seconds),
    where n is the number of non-null returns in the bucket/group.
    """
    std_ret = log_ret_expr.std()
    if sample_seconds:
        return std_ret * (float(SECONDS_IN_TRADING_YEAR) / float(sample_seconds)) ** 0.5
    if bucket_seconds:
        n_obs = log_ret_expr.drop_nulls().drop_nans().count()
        factor = (float(SECONDS_IN_TRADING_YEAR) * n_obs / float(bucket_seconds)).sqrt()
        return std_ret * factor
    return std_ret
