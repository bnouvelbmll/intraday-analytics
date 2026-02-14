import polars as pl
from typing import Optional, Dict, Callable, List, Literal

from pydantic import BaseModel, Field, ConfigDict

from basalt.analytics_base import BaseAnalytics
from basalt.analytics_registry import register_analytics


def snake_to_pascal(snake_case: str) -> str:
    """Convert snake_case to PascalCase."""
    return "".join(word.capitalize() for word in snake_case.split("_"))


def _first_available(schema: pl.Schema, candidates: List[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate in schema.names():
            return candidate
    return None


def _alias_prev(name: str) -> str:
    base = snake_to_pascal(name) if "_" in name else name
    return f"Prev{base}"


def enrich_orderbook(df: pl.LazyFrame, group_cols: List[str]) -> pl.LazyFrame:
    """Add lagged order book features needed for aggressor detection."""
    schema = df.collect_schema()
    if "BestAskPrice" not in schema.names() or "BestBidPrice" not in schema.names():
        return df

    df = df.with_columns(
        pl.col("BestAskPrice").shift(1).over(group_cols).alias("OldBestAskPrice"),
        pl.col("BestAskSize").shift(1).over(group_cols).alias("OldBestAskSize"),
        pl.col("BestBidPrice").shift(1).over(group_cols).alias("OldBestBidPrice"),
        pl.col("BestBidSize").shift(1).over(group_cols).alias("OldBestBidSize"),
    )

    lag_cols = {
        "lob_action": ["lob_action", "LobAction"],
        "size": ["size", "Size"],
        "old_size": ["old_size", "OldSize"],
        "price": ["price", "Price"],
        "old_price": ["old_price", "OldPrice"],
        "side": ["side", "Side"],
        "event_timestamp": ["event_timestamp", "EventTimestamp"],
        "execution_size": ["execution_size", "ExecutionSize"],
    }
    for key, candidates in lag_cols.items():
        col_name = _first_available(schema, candidates)
        if col_name:
            df = df.with_columns(
                pl.col(col_name).shift(1).over(group_cols).alias(_alias_prev(col_name))
            )

    df = df.with_columns(
        Spread=(pl.col("BestAskPrice") - pl.col("BestBidPrice")),
        OldSpread=(pl.col("OldBestAskPrice") - pl.col("OldBestBidPrice")),
    )
    return df


def filter_executions(df: pl.LazyFrame, execution_size_col: str) -> pl.LazyFrame:
    """Return cleaned, on-book executions only."""
    trades = df.filter(pl.col(execution_size_col) > 0)
    schema = df.collect_schema()
    printable_col = _first_available(schema, ["Printable", "printable"])
    if printable_col:
        trades = trades.filter(pl.col(printable_col))

    order_type_col = _first_available(schema, ["OrderType", "order_type"])
    if order_type_col:
        trades = trades.filter(pl.col(order_type_col) == 1)

    if "RevealedQty" not in schema.names():
        trades = trades.with_columns(pl.lit(0).alias("RevealedQty"))

    ts_col = _first_available(
        schema, ["EventTimestamp", "TradeTimestamp", "TimestampNanoseconds"]
    )
    if ts_col is None:
        return trades

    return trades.sort(ts_col)


def normalise_aggressor_side(trades: pl.LazyFrame) -> pl.LazyFrame:
    schema = trades.collect_schema()
    if "AggressorSide" not in schema.names():
        if "Side" in schema.names():
            if schema["Side"] in (pl.Int8, pl.Int16, pl.Int32, pl.Int64):
                trades = trades.with_columns(
                    AggressorSide=pl.when(pl.col("Side") == 1).then(-1).otherwise(1)
                )
            else:
                trades = trades.with_columns(
                    AggressorSide=pl.when(pl.col("Side") == "BID").then(-1).otherwise(1)
                )
    elif schema["AggressorSide"] == pl.String:
        trades = trades.with_columns(
            AggressorSide=pl.when(pl.col("AggressorSide") == "BID")
            .then(1)
            .when(pl.col("AggressorSide") == "ASK")
            .then(-1)
            .otherwise(pl.col("AggressorSide").cast(pl.Int8, strict=False))
        )
    return trades


def group_executions(
    trades: pl.LazyFrame,
    deltat: float = 1e-5,
    ts_col: str = "EventTimestamp",
    group_cols: List[str] = None,
) -> pl.LazyFrame:
    if group_cols is None:
        group_cols = ["MIC", "ListingId"]

    direction_shift = (
        pl.col("AggressorSide") == pl.col("AggressorSide").shift(1).over(group_cols)
    ).cast(pl.Int8)

    aggressor_groups = direction_shift.cum_sum().over(group_cols)
    same_side_seq = aggressor_groups - (
        aggressor_groups * (1 - direction_shift)
    ).replace({0: None}).forward_fill().over(group_cols).fill_null(0)

    trades = trades.with_columns(SameSideSequence=same_side_seq)

    is_new_group = pl.col("SameSideSequence") == 0
    trades = trades.with_columns(
        TimeSincePrev=(pl.col(ts_col) - pl.col(ts_col).shift(1).over(group_cols))
        .dt.total_microseconds()
        .fill_null(deltat * 1e6 + 1)
        / 1e6,
    )

    schema = trades.collect_schema()
    end_of_event_col = _first_available(schema, ["EndOfEvent", "end_of_event"])
    if end_of_event_col:
        is_same_group_expr = (
            (
                pl.col(end_of_event_col)
                .shift(1)
                .over(group_cols)
                .cast(bool)
                .fill_null(True)
            ).not_()
        ) | (pl.col("TimeSincePrev") <= deltat)
    else:
        is_same_group_expr = pl.col("TimeSincePrev") <= deltat

    trades = trades.with_columns(
        IsSameGroup=is_same_group_expr,
    ).with_columns(
        SequenceNumber=is_new_group.cum_sum().over(group_cols),
        ConsolidatedTrades=(pl.col("IsSameGroup").not_()).cum_sum().over(group_cols),
    )
    return trades


def get_aggregation_expressions(
    trades_cols: List[str], execution_size: str, price_col: str
) -> List[pl.Expr]:
    """Generate aggregation expressions based on column naming conventions."""

    aggs: List[pl.Expr] = []

    # Define aggregation strategies
    SUM_COLS = ["Size", "TradeNotional", "TradeNotionalUSD", "TradeNotionalEUR"]
    FIRST_COLS = [
        c
        for c in trades_cols
        if "PreTrade" in c or c.startswith("Old") or c in ["Ticker", "MarketState"]
    ]
    LAST_COLS = [
        c
        for c in trades_cols
        if "PostTrade" in c
        or c in ["BestBidPrice", "BestAskPrice", "BestBidSize", "BestAskSize"]
    ]
    UNIQUE_COLS = [
        "BMLLParticipantType",
        "ParticipantType",
        "MarketMechanism",
        "TradingMode",
        "TransactionCategory",
        "NegotiatedTrade",
        "CrossingTrade",
        "AlgorithmicTrade",
        "PublicationMode",
        "DeferralType",
        "BMLLTradeType",
        "Classification",
        "IsBlock",
        "LotType",
    ]

    # Basic aggregations
    aggs.extend(
        [
            (pl.col(c).sum().alias(c))
            for c in SUM_COLS
            if c in trades_cols and c != execution_size
        ]
    )
    aggs.extend([(pl.col(c).first().alias(c)) for c in FIRST_COLS if c in trades_cols])
    aggs.extend([(pl.col(c).last().alias(c)) for c in LAST_COLS if c in trades_cols])
    aggs.extend([(pl.col(c).first().alias(c)) for c in UNIQUE_COLS if c in trades_cols])

    # Recompute PricePoint
    if "BestBidPrice" in LAST_COLS and "BestAskPrice" in LAST_COLS:
        vwap = pl.col("Value").sum() / pl.col(execution_size).sum()
        last_mid = (pl.col("BestAskPrice").last() + pl.col("BestBidPrice").last()) / 2
        last_spread = pl.col("BestAskPrice").last() - pl.col("BestBidPrice").last()
        aggs.append(((vwap - last_mid) / (last_spread / 2)).alias("PricePoint"))

    return aggs


def aggregate_trades(
    trades: pl.LazyFrame,
    execution_size: str,
    ts_col: str,
    group_cols: List[str],
) -> pl.LazyFrame:
    """Aggregate executions into aggressive orders."""
    schema = trades.collect_schema()
    price_col = "ExecutionPrice" if "ExecutionPrice" in schema.names() else "Price"

    trades = trades.with_columns(Value=(pl.col(execution_size) * pl.col(price_col)))

    grouping_cols = group_cols + ["ConsolidatedTrades", "SequenceNumber"]

    vwap_expr = pl.col("Value").sum() / pl.col(execution_size).sum()
    base_stats = [
        pl.col(ts_col).first().alias(ts_col),
        pl.col(execution_size).sum().alias(execution_size),
        vwap_expr.alias(price_col),
        pl.col("AggressorSide").mean().alias("AggressorSide"),
        (pl.col(ts_col).max() - pl.col(ts_col).min())
        .dt.total_microseconds()
        .alias("Duration"),
        pl.col(price_col).min().alias("MinPrice"),
        pl.col(price_col).max().alias("MaxPrice"),
        pl.len().alias("NumExecutions"),
        (pl.col(price_col).max() - pl.col(price_col).min()).alias("PriceDiff"),
    ]
    if "LocalPrice" in schema.names() and price_col != "LocalPrice":
        base_stats.append(vwap_expr.alias("LocalPrice"))
    if price_col != "Price":
        base_stats.append(pl.col(price_col).alias("Price"))

    dynamic_stats = get_aggregation_expressions(
        schema.names(), execution_size, price_col
    )
    base_stats.extend(dynamic_stats)

    return trades.group_by(grouping_cols).agg(base_stats)


class AggressiveTradesTransform:
    def __init__(
        self,
        execution_size: Optional[str] = None,
        deltat: float = 1e-5,
        time_bucket_seconds: Optional[int] = None,
    ):
        self.execution_size = execution_size
        self.deltat = deltat
        self.time_bucket_seconds = time_bucket_seconds

    def transform(self, data: pl.LazyFrame) -> Optional[pl.LazyFrame]:
        schema = data.collect_schema()

        if (
            "EventTimestamp" not in schema.names()
            and "TimestampNanoseconds" in schema.names()
        ):
            data = data.with_columns(
                EventTimestamp=pl.col("TimestampNanoseconds").cast(pl.Datetime("ns"))
            )
            schema = data.collect_schema()

        group_cols = [col for col in ["MIC", "ListingId"] if col in schema.names()]

        is_l3 = (
            _first_available(schema, ["LobAction", "lob_action"]) is not None
            and _first_available(schema, ["ExecutionSize", "execution_size"])
            is not None
        )

        if is_l3:
            ts_col = (
                "EventTimestamp"
                if "EventTimestamp" in schema.names()
                else "TimestampNanoseconds"
            )
            execution_size_col = self.execution_size or "ExecutionSize"
            enriched_data = data
            if "BestAskPrice" in schema.names():
                enriched_data = enrich_orderbook(data, group_cols)
            trades = filter_executions(enriched_data, execution_size_col)
        else:
            ts_col = "TradeTimestamp"
            execution_size_col = self.execution_size or "Size"
            required_enrich_cols = ["OldBestAskPrice", "OldBestBidPrice"]
            if not all(c in schema.names() for c in required_enrich_cols):
                data = enrich_orderbook(data, group_cols)
            trades = filter_executions(data, execution_size_col)

        if trades.collect().is_empty():
            return None

        trades = normalise_aggressor_side(trades)
        trades = group_executions(
            trades, self.deltat, ts_col=ts_col, group_cols=group_cols
        )

        if (
            is_l3
            and "OldPrice" in trades.collect_schema().names()
            and "ExecutionPrice" in trades.collect_schema().names()
        ):
            trades = trades.filter(pl.col("OldPrice") == pl.col("ExecutionPrice"))

        aggressive_orders = aggregate_trades(
            trades, execution_size_col, ts_col=ts_col, group_cols=group_cols
        )

        if self.time_bucket_seconds:
            aggressive_orders = aggressive_orders.with_columns(
                TimeBucket=pl.col(ts_col).dt.truncate(f"{self.time_bucket_seconds}s")
            )

        return aggressive_orders


def compute_aggressive_orders_and_executions(
    data: pl.LazyFrame,
    *,
    execution_size: Optional[str],
    deltat: float,
    time_bucket_seconds: Optional[int],
) -> tuple[Optional[pl.LazyFrame], Optional[pl.LazyFrame]]:
    schema = data.collect_schema()

    if (
        "EventTimestamp" not in schema.names()
        and "TimestampNanoseconds" in schema.names()
    ):
        data = data.with_columns(
            EventTimestamp=pl.col("TimestampNanoseconds").cast(pl.Datetime("ns"))
        )
        schema = data.collect_schema()

    group_cols = [col for col in ["MIC", "ListingId"] if col in schema.names()]

    is_l3 = (
        _first_available(schema, ["LobAction", "lob_action"]) is not None
        and _first_available(schema, ["ExecutionSize", "execution_size"])
        is not None
    )

    if is_l3:
        ts_col = (
            "EventTimestamp"
            if "EventTimestamp" in schema.names()
            else "TimestampNanoseconds"
        )
        execution_size_col = execution_size or "ExecutionSize"
        enriched_data = data
        if "BestAskPrice" in schema.names():
            enriched_data = enrich_orderbook(data, group_cols)
        trades = filter_executions(enriched_data, execution_size_col)
    else:
        ts_col = "TradeTimestamp"
        execution_size_col = execution_size or "Size"
        required_enrich_cols = ["OldBestAskPrice", "OldBestBidPrice"]
        if not all(c in schema.names() for c in required_enrich_cols):
            data = enrich_orderbook(data, group_cols)
        trades = filter_executions(data, execution_size_col)

    if trades.collect().is_empty():
        return None, None

    trades = normalise_aggressor_side(trades)
    trades = group_executions(trades, deltat, ts_col=ts_col, group_cols=group_cols)

    if (
        is_l3
        and "OldPrice" in trades.collect_schema().names()
        and "ExecutionPrice" in trades.collect_schema().names()
    ):
        trades = trades.filter(pl.col("OldPrice") == pl.col("ExecutionPrice"))

    aggressive_orders = aggregate_trades(
        trades, execution_size_col, ts_col=ts_col, group_cols=group_cols
    )

    if time_bucket_seconds:
        aggressive_orders = aggressive_orders.with_columns(
            TimeBucket=pl.col(ts_col).dt.truncate(f"{time_bucket_seconds}s")
        )
        trades = trades.with_columns(
            TimeBucket=pl.col(ts_col).dt.truncate(f"{time_bucket_seconds}s")
        )

    return aggressive_orders, trades


class AggressiveTradesPreprocessConfig(BaseModel):
    """
    Build aggressive order groupings from trades or L3 executions.

    Side outputs:
    - `aggressive_orders`: aggregated aggressive orders (one row per order)
    - `aggressive_executions`: executions grouped into aggressive orders
    """

    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "aggressive_preprocess",
                "tier": "preprocess",
                "desc": "Preprocess: aggregate executions into aggressive orders.",
                "schema_keys": ["aggressive_orders", "aggressive_executions"],
            }
        }
    )

    ENABLED: bool = Field(
        False,
        description="Enable or disable aggressive order preprocessing.",
    )
    source_table: Literal["trades", "l3"] = Field(
        "trades",
        description="Input source table for aggressive grouping.",
    )
    execution_size: Optional[str] = Field(
        None,
        description="Override execution size column (defaults to Size or ExecutionSize).",
    )
    deltat: float = Field(
        1e-5,
        description="Max inter-execution gap (seconds) for grouping into aggressive orders.",
    )
    time_bucket_seconds: Optional[int] = Field(
        None,
        description="Optional time bucket override for aggressive outputs.",
    )


@register_analytics("aggressive_preprocess", config_attr="aggressive_preprocess")
class AggressiveTradesPreprocess(BaseAnalytics):
    """
    Preprocess aggressive orders into side output tables.
    """

    REQUIRES = ["trades"]

    def __init__(self, config: AggressiveTradesPreprocessConfig):
        super().__init__("aggressive_preprocess", {})
        self.config = config
        if config.source_table == "l3":
            self.REQUIRES = ["l3"]

    def compute(self) -> pl.LazyFrame:
        if not self.config.ENABLED:
            return pl.DataFrame().lazy()
        source = self.trades if self.config.source_table == "trades" else self.l3
        if source is None:
            raise ValueError("Aggressive preprocess source table not provided.")
        if isinstance(source, pl.DataFrame):
            source = source.lazy()

        orders, executions = compute_aggressive_orders_and_executions(
            source,
            execution_size=self.config.execution_size,
            deltat=self.config.deltat,
            time_bucket_seconds=self.config.time_bucket_seconds,
        )
        if orders is not None:
            size_col = "ExecutionSize" if "ExecutionSize" in orders.collect_schema().names() else "Size"
            if size_col in orders.collect_schema().names():
                orders = orders.with_columns(AggressiveSize=pl.col(size_col))
            self.set_side_output("aggressive_orders", orders)
        if executions is not None:
            exec_size_col = "ExecutionSize" if "ExecutionSize" in executions.collect_schema().names() else "Size"
            if exec_size_col in executions.collect_schema().names():
                executions = executions.with_columns(AggressiveExecutionSize=pl.col(exec_size_col))
            self.set_side_output("aggressive_executions", executions)
        return pl.DataFrame().lazy()
