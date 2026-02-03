import polars as pl
from typing import List, Dict
from .configuration import AnalyticsConfig, PassConfig
from .analytics.l2 import L2AnalyticsLast, L2AnalyticsTW
from .analytics.l3 import L3Analytics
from .analytics.trade import (
    TradeAnalytics,
    TradeAnalyticsConfig,
    TradeGenericConfig,
    TradeDiscrepancyConfig,
    TradeFlagConfig,
    TradeChangeConfig,
    TradeImpactConfig,
)
from .analytics.execution import ExecutionAnalytics
from .analytics.common import METRIC_HINTS, METRIC_DOCS
from .analytics.dense import DenseAnalytics
from .analytics.l2 import L2AnalyticsConfig, L2LiquidityConfig, L2SpreadConfig, L2ImbalanceConfig, L2VolatilityConfig
from .analytics.l3 import L3AnalyticsConfig, L3MetricConfig, L3AdvancedConfig
from .analytics.execution import (
    ExecutionAnalyticsConfig,
    L3ExecutionConfig,
    TradeBreakdownConfig,
    ExecutionDerivedConfig,
)


def _build_full_config(levels: int = 10, impact_horizons=None) -> AnalyticsConfig:
    if impact_horizons is None:
        impact_horizons = ["1s", "10s"]

    l2_config = L2AnalyticsConfig(
        liquidity=[
            L2LiquidityConfig(
                sides=["Bid", "Ask"],
                levels=list(range(1, levels + 1)),
                measures=[
                    "Price",
                    "Quantity",
                    "NumOrders",
                    "CumQuantity",
                    "CumOrders",
                    "CumNotional",
                ],
                aggregations=["Last", "TWA"],
            )
        ],
        spreads=[L2SpreadConfig(variant=["Abs", "BPS"], aggregations=["Last", "TWA"])],
        imbalances=[
            L2ImbalanceConfig(
                levels=list(range(1, levels + 1)),
                measure=["CumQuantity", "Orders", "CumNotional"],
                aggregations=["Last", "TWA"],
            )
        ],
        volatility=[
            L2VolatilityConfig(
                source=["Mid", "Bid", "Ask", "WeightedMid"],
                aggregations=["First", "Last", "Min", "Max", "Mean", "Sum", "Median", "Std"],
            )
        ],
    )

    l3_config = L3AnalyticsConfig(
        generic_metrics=[
            L3MetricConfig(
                sides=["Bid", "Ask"],
                actions=["Insert", "Remove", "Update", "UpdateInserted", "UpdateRemoved"],
                measures=["Count", "Volume"],
                aggregations=["First", "Last", "Min", "Max", "Mean", "Sum", "Median", "Std"],
            )
        ],
        advanced_metrics=[
            L3AdvancedConfig(variant="ArrivalFlowImbalance"),
            L3AdvancedConfig(variant="CancelToTradeRatio"),
            L3AdvancedConfig(variant="AvgQueuePosition"),
            L3AdvancedConfig(variant="AvgRestingTime"),
            L3AdvancedConfig(variant="FleetingLiquidityRatio"),
            L3AdvancedConfig(variant="AvgReplacementLatency"),
        ],
    )

    trade_config = TradeAnalyticsConfig(
        generic_metrics=[
            TradeGenericConfig(
                sides=["Total", "Bid", "Ask"],
                measures=[
                    "Volume",
                    "Count",
                    "NotionalEUR",
                    "NotionalUSD",
                    "VWAP",
                    "OHLC",
                    "AvgPrice",
                    "MedianPrice",
                ],
            )
        ],
        discrepancy_metrics=[
            TradeDiscrepancyConfig(
                references=[
                    "MidAtPrimary",
                    "EBBO",
                    "PreTradeMid",
                    "MidPrice",
                    "BestBid",
                    "BestAsk",
                    "BestBidAtVenue",
                    "BestAskAtVenue",
                    "BestBidAtPrimary",
                    "BestAskAtPrimary",
                ],
                sides=["Total", "Bid", "Ask"],
                aggregations=["First", "Last", "Min", "Max", "Mean", "Sum", "Median", "Std"],
            )
        ],
        flag_metrics=[
            TradeFlagConfig(
                flags=[
                    "NegotiatedTrade",
                    "OddLotTrade",
                    "BlockTrade",
                    "CrossTrade",
                    "AlgorithmicTrade",
                ],
                measures=["Volume", "Count", "AvgNotional"],
                sides=["Total", "Bid", "Ask"],
            )
        ],
        change_metrics=[
            TradeChangeConfig(
                measures=["PreTradeElapsedTimeChg", "PostTradeElapsedTimeChg", "PricePoint"],
                scopes=["Local", "Primary", "Venue"],
            )
        ],
        impact_metrics=[
            TradeImpactConfig(
                variant=["EffectiveSpread", "RealizedSpread", "PriceImpact"],
                horizon=impact_horizons,
            )
        ],
    )

    exec_config = ExecutionAnalyticsConfig(
        l3_execution=[
            L3ExecutionConfig(sides=["Bid", "Ask"], measures=["ExecutedVolume", "VWAP"])
        ],
        trade_breakdown=[
            TradeBreakdownConfig(
                trade_types=["LIT", "DARK"],
                aggressor_sides=["Buy", "Sell", "Unknown"],
                measures=["Volume", "VWAP", "VWPP"],
            )
        ],
        derived_metrics=[ExecutionDerivedConfig(variant="TradeImbalance")],
    )

    pass1 = PassConfig(
        name="pass1",
        l2_analytics=l2_config,
        l3_analytics=l3_config,
        trade_analytics=trade_config,
        execution_analytics=exec_config,
    )

    return AnalyticsConfig(PASSES=[pass1])


def get_output_schema(config_or_pass) -> Dict[str, List[str]]:
    """
    Runs a dummy pipeline to determine the output schema (column names)
    generated by the provided configuration.

    Args:
        config_or_pass: AnalyticsConfig or PassConfig.

    Returns:
        A dictionary mapping module names ('l2', 'l3', 'trade', 'execution')
        to a list of output column names.
    """

    # 1. Define Dummy Schemas (Minimal required columns)
    # These schemas must match what the analytics classes expect as input.

    # Common columns
    common_cols = {
        "ListingId": pl.Int64,
        "TimeBucket": pl.Datetime("ns"),
        "MIC": pl.Utf8,
        "Ticker": pl.Utf8,
        "CurrencyCode": pl.Utf8,
        "MarketState": pl.Utf8,
        "EventTimestamp": pl.Datetime("ns"),
        "DT": pl.Int64,  # Duration for TWA
    }

    # L2 Schema
    l2_schema = {**common_cols}
    # Add Bid/Ask Price/Qty/Orders for N levels (assuming max 100 for safety in dummy)
    for i in range(1, 101):
        l2_schema[f"BidPrice{i}"] = pl.Float64
        l2_schema[f"AskPrice{i}"] = pl.Float64
        l2_schema[f"BidQuantity{i}"] = pl.Float64
        l2_schema[f"AskQuantity{i}"] = pl.Float64
        l2_schema[f"BidNumOrders{i}"] = pl.Int64
        l2_schema[f"AskNumOrders{i}"] = pl.Int64

    # Trades Schema
    trades_schema = {
        **common_cols,
        "TradeDate": pl.Date,
        "Price": pl.Float64,
        "LocalPrice": pl.Float64,
        "Size": pl.Float64,
        "AggressorSide": pl.Int64,  # 1=Buy, 2=Sell
        "Classification": pl.Utf8,  # LIT_CONTINUOUS, etc.
        "BMLLTradeType": pl.Utf8,  # LIT, DARK
        "BMLLParticipantType": pl.Utf8,  # RETAIL, etc.
        "TradeNotionalEUR": pl.Float64,
        "TradeNotionalUSD": pl.Float64,
        "PricePoint": pl.Float64,
        # New columns for advanced metrics
        "PreTradeMid": pl.Float64,
        "MidPrice": pl.Float64,
        "PostTradeMidAtPrimary": pl.Float64,
        "PostTradeMid": pl.Float64,
        "BestBidPrice": pl.Float64,
        "BestAskPrice": pl.Float64,
        "BestBidPriceAtVenue": pl.Float64,
        "BestAskPriceAtVenue": pl.Float64,
        "BestBidPriceAtPrimary": pl.Float64,
        "BestAskPriceAtPrimary": pl.Float64,
        "PricePointAtVenue": pl.Float64,
        "PricePointAtPrimary": pl.Float64,
        "PreTradeElapsedTimeChg": pl.Float64,
        "PostTradeElapsedTimeChg": pl.Float64,
        "PreTradeElapsedTimeChgAtVenue": pl.Float64,
        "PostTradeElapsedTimeChgAtVenue": pl.Float64,
        "PreTradeElapsedTimeChgAtPrimary": pl.Float64,
        "PostTradeElapsedTimeChgAtPrimary": pl.Float64,
        "NegotiatedTrade": pl.Utf8,
        "LotType": pl.Utf8,
        "IsBlock": pl.Utf8,
        "CrossingTrade": pl.Utf8,
        "AlgorithmicTrade": pl.Utf8,
    }

    # L3 Schema
    l3_schema = {
        **common_cols,
        "LobAction": pl.Int64,  # 2=Insert, 3=Remove, 4=Update, 1=Exec
        "Side": pl.Int64,  # 1=Bid, 2=Ask
        "Size": pl.Float64,
        "OldSize": pl.Float64,
        "Price": pl.Float64,
        "OldPrice": pl.Float64,
        "ExecutionSize": pl.Float64,
        "ExecutionPrice": pl.Float64,
        "OrderID": pl.Int64,
        "SizeAhead": pl.Float64,
    }

    # 2. Create Dummy LazyFrames
    l2_dummy = pl.LazyFrame(schema=l2_schema)
    trades_dummy = pl.LazyFrame(schema=trades_schema)
    l3_dummy = pl.LazyFrame(schema=l3_schema)

    # MarketState dummy (needed for Dense)
    marketstate_dummy = pl.LazyFrame(
        schema={
            "ListingId": pl.Int64,
            "MarketState": pl.Utf8,
            "EventTimestamp": pl.Datetime("ns"),
        }
    )

    if isinstance(config_or_pass, AnalyticsConfig):
        if not config_or_pass.PASSES:
            return {"error": ["No passes configured."]}
        pass_config = config_or_pass.PASSES[0]
    else:
        pass_config = config_or_pass

    # 3. Instantiate and Compute
    output_columns = {}

    # --- L2 ---
    try:
        # L2 Last
        l2_last = L2AnalyticsLast(pass_config.l2_analytics)
        l2_last.l2 = l2_dummy
        l2_last_df = l2_last.compute()
        output_columns["l2_last"] = l2_last_df.collect_schema().names()

        # L2 TW
        l2_tw = L2AnalyticsTW(pass_config.l2_analytics)
        # BaseTWAnalytics expects .tw_analytics to be called by compute() or similar wrapper
        # But here we can call tw_analytics directly if we mock the input
        # The BaseTWAnalytics structure is a bit complex (resampling),
        # but L2AnalyticsTW.tw_analytics takes a frame and returns metrics.
        # We need to simulate the 'resampled' frame which has 'DT'
        l2_tw_df = l2_tw.tw_analytics(l2_dummy)
        output_columns["l2_tw"] = l2_tw_df.collect_schema().names()
    except Exception as e:
        output_columns["l2_error"] = [str(e)]

    # --- Trade ---
    try:
        trade_analytics = TradeAnalytics(pass_config.trade_analytics)
        trade_analytics.trades = trades_dummy
        trade_df = trade_analytics.compute()
        output_columns["trade"] = trade_df.collect_schema().names()
    except Exception as e:
        output_columns["trade_error"] = [str(e)]

    # --- L3 ---
    try:
        l3_analytics = L3Analytics(pass_config.l3_analytics)
        l3_analytics.l3 = l3_dummy
        l3_df = l3_analytics.compute()
        output_columns["l3"] = l3_df.collect_schema().names()
    except Exception as e:
        output_columns["l3_error"] = [str(e)]

    # --- Execution ---
    try:
        exec_analytics = ExecutionAnalytics(pass_config.execution_analytics)
        exec_analytics.l3 = l3_dummy
        exec_analytics.trades = trades_dummy
        exec_df = exec_analytics.compute()
        output_columns["execution"] = exec_df.collect_schema().names()
    except Exception as e:
        output_columns["execution_error"] = [str(e)]

    return output_columns


def _filter_schema_for_pass1(config: AnalyticsConfig, schema: Dict[str, List[str]]):
    if not config.PASSES:
        return schema

    pass1 = config.PASSES[0]
    if not pass1.modules:
        return schema

    module_map = {
        "l2": ["l2_last"],
        "l2tw": ["l2_tw"],
        "trade": ["trade"],
        "l3": ["l3"],
        "execution": ["execution"],
    }

    keys = []
    for module in pass1.modules:
        keys.extend(module_map.get(module, []))

    if not keys:
        return schema

    return {k: schema[k] for k in keys if k in schema}


def _apply_overrides(module: str, col: str, weight_col: str | None):
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


def _apply_docs(module: str, col: str):
    import re

    for doc in METRIC_DOCS:
        if doc.get("module") != module:
            continue
        pattern = doc.get("pattern")
        if not pattern:
            continue
        m = re.search(pattern, col)
        if not m:
            continue
        data = m.groupdict()
        data.setdefault("side_or_total", data.get("side") or "Total")
        data.setdefault("agg_or_sum", data.get("agg") or "Sum")
        data.setdefault("agg_or_mean", data.get("agg") or "Mean")
        data.setdefault("scope_or_empty", data.get("scope") or "")
        template = doc.get("template", "")
        unit_template = doc.get("unit") or ""
        group_template = doc.get("group") or ""
        role_template = doc.get("group_role") or ""
        semantics_template = doc.get("group_semantics") or ""
        try:
            definition = template.format(**data)
        except Exception:
            definition = template
        try:
            unit = unit_template.format(**data) if unit_template else ""
        except Exception:
            unit = unit_template
        try:
            group = group_template.format(**data) if group_template else ""
        except Exception:
            group = group_template
        try:
            group_role = role_template.format(**data) if role_template else ""
        except Exception:
            group_role = role_template
        try:
            group_semantics = (
                semantics_template.format(**data) if semantics_template else ""
            )
        except Exception:
            group_semantics = semantics_template
        return {
            "definition": definition,
            "unit": unit,
            "group": group,
            "group_role": group_role,
            "group_semantics": group_semantics,
        }
    return {"definition": "", "unit": "", "group": "", "group_role": "", "group_semantics": ""}


def _default_hint_for_column(col: str, weight_col: str | None):
    col_lower = col.lower()
    if col in {"ListingId", "TimeBucket", "Ticker", "CurrencyCode"}:
        return {"default_agg": "Last", "weight_col": None}
    if col == "MIC":
        return {"default_agg": "Last", "weight_col": None}
    if col == "MarketState":
        return {"default_agg": "Last", "weight_col": None}
    if col.endswith("_right") or col.endswith("TimeBucketInt") or col.endswith("TimeBucketInt_right"):
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


def _apply_hints(
    schema: Dict[str, List[str]],
    weight_col: str | None,
) -> Dict[str, List[dict]]:
    hinted = {}
    for module, cols in schema.items():
        rows = []
        for c in cols:
            override = _apply_overrides(module, c, weight_col)
            hint = override if override else _default_hint_for_column(c, weight_col)
            rows.append(
                {
                    "column": c,
                    "default_agg": hint["default_agg"],
                    "weight_col": hint["weight_col"],
                    **_apply_docs(module, c),
                }
            )
        hinted[module] = rows
    return hinted


def _print_schema(
    schema: Dict[str, List[str]] | Dict[str, List[dict]],
    as_json: bool = False,
    omit_keys: bool = True,
):
    import json
    import pandas as pd

    if as_json:
        print(json.dumps(schema, indent=2, sort_keys=True))
        return

    rows = []
    for key in sorted(schema.keys()):
        for c in schema[key]:
            if isinstance(c, dict):
                row = {"module": key, **c}
            else:
                row = {"module": key, "column": c}
            rows.append(row)
    df = pd.DataFrame(rows)
    key_cols = {
        "ListingId",
        "TimeBucket",
        "InstrumentId",
        "ListingId_right",
        "TimeBucketInt_right",
        "TimeBucketInt",
    }
    state_cols = {"MIC", "MarketState", "Ticker", "CurrencyCode"}

    def _print_section(title: str, frame: pd.DataFrame):
        if frame.empty:
            return
        print(title)
        print(frame.to_markdown(index=True))

    if omit_keys:
        df = df[~df["column"].isin(key_cols | state_cols)]
        _print_section("| module | column |", df)
        return

    keys_df = df[df["column"].isin(key_cols)]
    state_df = df[df["column"].isin(state_cols)]
    metrics_df = df[~df["column"].isin(key_cols | state_cols)]

    _print_section("| join keys |", keys_df)
    _print_section("| state |", state_df)
    _print_section("| metrics |", metrics_df)


def main():
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description="Enumerate metric column names for pass1 modules."
    )
    parser.add_argument(
        "--config",
        help="Path to JSON config file. If omitted, uses a full combinatorics config.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Use a full combinatorics config (ignores --config).",
    )
    parser.add_argument(
        "--levels",
        type=int,
        default=10,
        help="Max book levels for full combinatorics mode.",
    )
    parser.add_argument(
        "--impact-horizons",
        default="1s,10s",
        help="Comma-separated horizons for TradeImpact in full mode.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output schema as JSON.",
    )
    parser.add_argument(
        "--no-hints",
        action="store_true",
        help="Disable default aggregation hints.",
    )
    parser.add_argument(
        "--weight-col",
        default="TradeNotionalEUR",
        help="Column used for NotionalWeighted hints (default: TradeNotionalEUR).",
    )
    parser.add_argument(
        "--no-notional-weight",
        action="store_true",
        help="Disable NotionalWeighted hints (falls back to Mean).",
    )
    parser.add_argument(
        "--include-keys",
        action="store_true",
        help="Include join keys/state columns in output.",
    )
    args = parser.parse_args()

    if args.full:
        horizons = [h.strip() for h in args.impact_horizons.split(",") if h.strip()]
        config = _build_full_config(levels=args.levels, impact_horizons=horizons)
    elif args.config:
        with open(args.config, "r") as f:
            data = json.load(f)
        config = AnalyticsConfig(**data)
    else:
        horizons = [h.strip() for h in args.impact_horizons.split(",") if h.strip()]
        config = _build_full_config(levels=args.levels, impact_horizons=horizons)

    schema = get_output_schema(config)
    schema = _filter_schema_for_pass1(config, schema)
    if not args.no_hints:
        weight_col = None if args.no_notional_weight else args.weight_col
        schema = _apply_hints(schema, weight_col=weight_col)
    _print_schema(schema, as_json=args.json, omit_keys=not args.include_keys)


if __name__ == "__main__":
    main()
