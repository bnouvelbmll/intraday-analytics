from __future__ import annotations

from basalt.analytics.common import METRIC_HINTS


def apply_overrides(module: str, col: str, weight_col: str | None):
    import re

    for hint in METRIC_HINTS:
        if hint.get("module") != module:
            continue
        pattern = hint.get("pattern")
        if pattern and re.search(pattern, col):
            return {
                "default_agg": hint.get("default_agg"),
                "weight_col": hint.get("weight_col") or weight_col,
            }
    return None


def default_hint_for_column(col: str, weight_col: str | None):
    col_lower = col.lower()
    if col in {"ListingId", "TimeBucket", "Ticker", "CurrencyCode"}:
        return {"default_agg": "Last", "weight_col": None}
    if col == "MIC":
        return {"default_agg": "Last", "weight_col": None}
    if col == "MarketState":
        return {"default_agg": "Last", "weight_col": None}
    if (
        col.endswith("_right")
        or col.endswith("TimeBucketInt")
        or col.endswith("TimeBucketInt_right")
    ):
        return {"default_agg": "Last", "weight_col": None}

    stat_markers = ["avg", "mean", "median", "std", "vwap"]
    notional_markers = [
        "avgprice",
        "medianprice",
        "priceimpact",
        "realizedspread",
        "effectivespread",
        "price",
        "mid",
        "spread",
        "imbalance",
        "ratio",
    ]

    if any(m in col_lower for m in stat_markers + notional_markers):
        if weight_col:
            return {"default_agg": "NotionalWeighted", "weight_col": weight_col}
        return {"default_agg": "Mean", "weight_col": None}

    sum_markers = ["count", "volume", "notional", "size", "executedvolume"]
    if any(m in col_lower for m in sum_markers):
        return {"default_agg": "Sum", "weight_col": None}

    if weight_col:
        return {"default_agg": "NotionalWeighted", "weight_col": weight_col}

    return {"default_agg": "Mean", "weight_col": None}
