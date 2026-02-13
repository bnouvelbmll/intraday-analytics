from __future__ import annotations

import polars as pl

from basalt.analytics.cbbo import CBBOAnalytic


def test_cbbo_quantity_expr_variants():
    qty = pl.col("q")
    dt = pl.col("dt")
    df = pl.DataFrame({"q": [1.0, 2.0, 3.0], "dt": [1, 2, 1]})

    expr_name_pairs = [
        CBBOAnalytic._quantity_expr(qty, dt, "TWMean", base_name="QuantityAtCBB"),
        CBBOAnalytic._quantity_expr(qty, dt, "Min", base_name="QuantityAtCBB"),
        CBBOAnalytic._quantity_expr(qty, dt, "Max", base_name="QuantityAtCBB"),
        CBBOAnalytic._quantity_expr(qty, dt, "Median", base_name="QuantityAtCBB"),
    ]
    exprs = [pair[0].alias(pair[1]) for pair in expr_name_pairs if pair is not None]
    out = df.select(exprs)

    assert out["QuantityAtCBB"][0] == (1.0 * 1 + 2.0 * 2 + 3.0 * 1) / 4
    assert out["QuantityAtCBBMin"][0] == 1.0
    assert out["QuantityAtCBBMax"][0] == 3.0
    assert out["QuantityAtCBBMedian"][0] == 2.0
