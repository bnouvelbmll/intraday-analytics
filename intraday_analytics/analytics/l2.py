import polars as pl
from intraday_analytics.bases import BaseAnalytics, BaseTWAnalytics
from pydantic import BaseModel, Field
from typing import Optional, List, Union, Literal
from .common import (
    CombinatorialMetricConfig,
    Side,
    AggregationMethod,
    metric_doc,
)

# --- Configuration Models ---

L2LiquidityMeasure = Literal[
    "Quantity",
    "CumQuantity",
    "CumNotional",
    "Price",
    "InsertAge",
    "LastMod",
    "SizeAhead",
    "NumOrders",
    "CumOrders",
]


class L2LiquidityConfig(CombinatorialMetricConfig):
    """
    Configuration for L2 Liquidity metrics (at specific levels).
    """

    metric_type: Literal["L2_Liquidity"] = "L2_Liquidity"

    sides: Union[Side, List[Side]] = Field(
        ..., description="Side of the book to analyze (Bid, Ask)."
    )

    levels: Union[int, List[int]] = Field(
        ...,
        description="Book levels to query (1-based index). Can be a single int or a list of ints.",
    )

    measures: Union[L2LiquidityMeasure, List[L2LiquidityMeasure]] = Field(
        ..., description="The specific measure to extract (e.g., 'Quantity', 'Price')."
    )


class L2SpreadConfig(CombinatorialMetricConfig):
    """
    Configuration for Bid-Ask Spread metrics.
    """

    metric_type: Literal["L2_Spread"] = "L2_Spread"

    variant: Union[Literal["Abs", "BPS"], List[Literal["Abs", "BPS"]]] = Field(
        default=["BPS"],
        description="Spread variant: 'Abs' (Ask-Bid) or 'BPS' (Basis Points relative to Mid).",
    )


L2ImbalanceMeasure = Literal["Quantity", "CumQuantity", "CumNotional", "Orders"]


class L2ImbalanceConfig(CombinatorialMetricConfig):
    """
    Configuration for Order Book Imbalance.
    Formula: (Bid - Ask) / (Bid + Ask)
    """

    metric_type: Literal["L2_Imbalance"] = "L2_Imbalance"

    levels: Union[int, List[int]] = Field(
        ...,
        description="Book levels to consider. If >1, usually implies cumulative imbalance up to that level.",
    )

    measure: Union[L2ImbalanceMeasure, List[L2ImbalanceMeasure]] = Field(
        default="CumQuantity",
        description="The underlying measure to use for the imbalance calculation.",
    )


class L2VolatilityConfig(CombinatorialMetricConfig):
    """
    Configuration for Price Volatility (Standard Deviation of price series).
    """

    metric_type: Literal["L2_Volatility"] = "L2_Volatility"

    source: Union[Literal["Mid", "Bid", "Ask", "Last", "WeightedMid"], List[str]] = (
        Field(default="Mid", description="The price series to measure volatility on.")
    )

    # Default to Std, but allow any aggregation from AggregationMethod
    aggregations: List[AggregationMethod] = ["Std"]


class L2OHLCConfig(CombinatorialMetricConfig):
    """
    Configuration for OHLC bars computed from L2 price series.
    """

    metric_type: Literal["L2_OHLC"] = "L2_OHLC"

    source: Union[Literal["Mid", "Bid", "Ask", "WeightedMid"], List[str]] = Field(
        default="Mid", description="The price series to compute OHLC on."
    )

    open_mode: Literal["event", "prev_close"] = Field(
        default="event",
        description="How to compute Open for empty buckets: 'event' uses first event; "
        "'prev_close' uses previous Close and fills empty buckets.",
    )


class L2AnalyticsConfig(BaseModel):
    ENABLED: bool = True
    time_bucket_seconds: Optional[float] = None
    time_bucket_anchor: Literal["end", "start"] = "end"
    time_bucket_closed: Literal["right", "left"] = "right"

    # Combinatorial Configs
    liquidity: List[L2LiquidityConfig] = Field(default_factory=list)
    spreads: List[L2SpreadConfig] = Field(default_factory=list)
    imbalances: List[L2ImbalanceConfig] = Field(default_factory=list)
    volatility: List[L2VolatilityConfig] = Field(default_factory=list)
    ohlc: List[L2OHLCConfig] = Field(default_factory=list)

    # Legacy support (optional, can be deprecated)
    levels: int = 10


# --- Analytics Implementation ---


class L2AnalyticsLast(BaseAnalytics):
    """
    Computes L2 order book metrics based on the last snapshot in each time bucket.
    Now supports combinatorial configuration.
    """

    REQUIRES = ["l2"]

    def __init__(self, config: L2AnalyticsConfig):
        self.config = config
        super().__init__("l2last", {})

    def compute(self) -> pl.LazyFrame:
        gcols = ["MIC", "ListingId", "Ticker", "TimeBucket", "CurrencyCode"]

        expressions = []
        ohlc_specs = []

        # --- Liquidity ---
        for req in self.config.liquidity:
            for variant in req.expand():
                side = variant["sides"]
                level = variant["levels"]
                measure = variant["measures"]

                col_name = f"{side}{measure}{level}"

                if measure in ["Quantity", "Price", "NumOrders"]:
                    base_expr = pl.col(f"{side}{measure}{level}")
                elif measure == "CumQuantity":
                    cols = [f"{side}Quantity{i}" for i in range(1, level + 1)]
                    base_expr = pl.sum_horizontal(cols)
                elif measure == "CumOrders":
                    cols = [f"{side}NumOrders{i}" for i in range(1, level + 1)]
                    base_expr = pl.sum_horizontal(cols)
                elif measure == "CumNotional":
                    # Sum(Price * Qty) for levels 1..N
                    notional_exprs = [
                        pl.col(f"{side}Price{i}") * pl.col(f"{side}Quantity{i}")
                        for i in range(1, level + 1)
                    ]
                    base_expr = pl.sum_horizontal(notional_exprs)
                else:
                    # Fallback for InsertAge, LastMod, SizeAhead
                    base_expr = pl.col(f"{side}{measure}{level}")

                if req.market_states:
                    expr = base_expr.filter(
                        pl.col("MarketState").is_in(req.market_states)
                    ).last()
                else:
                    expr = base_expr.last()

                alias = (
                    req.output_name_pattern.format(**variant)
                    if req.output_name_pattern
                    else f"{side}{measure}{level}"
                )
                expressions.append(expr.alias(alias))

        # --- Spreads ---
        for req in self.config.spreads:
            for variant in req.expand():
                v_type = variant["variant"]
                if v_type == "Abs":
                    base_expr = pl.col("AskPrice1") - pl.col("BidPrice1")
                elif v_type == "BPS":
                    base_expr = (
                        20000
                        * (pl.col("AskPrice1") - pl.col("BidPrice1"))
                        / (pl.col("AskPrice1") + pl.col("BidPrice1"))
                    )

                if req.market_states:
                    expr = base_expr.filter(
                        pl.col("MarketState").is_in(req.market_states)
                    ).last()
                else:
                    expr = base_expr.last()

                alias = (
                    req.output_name_pattern.format(**variant)
                    if req.output_name_pattern
                    else f"Spread{v_type}"
                )
                expressions.append(expr.alias(alias))

        # --- Imbalances ---
        for req in self.config.imbalances:
            for variant in req.expand():
                level = variant["levels"]
                measure = variant["measure"]

                # Logic for imbalance: (Bid - Ask) / (Bid + Ask)

                if measure == "CumQuantity":
                    bid_val = pl.sum_horizontal(
                        [f"BidQuantity{i}" for i in range(1, level + 1)]
                    )
                    ask_val = pl.sum_horizontal(
                        [f"AskQuantity{i}" for i in range(1, level + 1)]
                    )
                elif measure == "Orders":
                    bid_val = pl.sum_horizontal(
                        [f"BidNumOrders{i}" for i in range(1, level + 1)]
                    )
                    ask_val = pl.sum_horizontal(
                        [f"AskNumOrders{i}" for i in range(1, level + 1)]
                    )
                elif measure == "CumNotional":
                    bid_val = pl.sum_horizontal(
                        [
                            pl.col(f"BidPrice{i}") * pl.col(f"BidQuantity{i}")
                            for i in range(1, level + 1)
                        ]
                    )
                    ask_val = pl.sum_horizontal(
                        [
                            pl.col(f"AskPrice{i}") * pl.col(f"AskQuantity{i}")
                            for i in range(1, level + 1)
                        ]
                    )
                else:
                    # Fallback to simple Quantity at level
                    bid_val = pl.col(f"BidQuantity{level}")
                    ask_val = pl.col(f"AskQuantity{level}")

                base_expr = (bid_val - ask_val) / (bid_val + ask_val)
                if req.market_states:
                    expr = base_expr.filter(
                        pl.col("MarketState").is_in(req.market_states)
                    ).last()
                else:
                    expr = base_expr.last()
                alias = (
                    req.output_name_pattern.format(**variant)
                    if req.output_name_pattern
                    else f"Imbalance{measure}{level}"
                )
                expressions.append(expr.alias(alias))

        # --- OHLC ---
        for req in self.config.ohlc:
            for variant in req.expand():
                source = variant["source"]
                if source == "Mid":
                    p = (pl.col("AskPrice1") + pl.col("BidPrice1")) / 2
                elif source == "WeightedMid":
                    p = (
                        pl.col("BidPrice1") * pl.col("AskQuantity1")
                        + pl.col("AskPrice1") * pl.col("BidQuantity1")
                    ) / (pl.col("AskQuantity1") + pl.col("BidQuantity1"))
                elif source == "Bid":
                    p = pl.col("BidPrice1")
                elif source == "Ask":
                    p = pl.col("AskPrice1")
                else:
                    continue

                names = {}
                for ohlc in ["Open", "High", "Low", "Close"]:
                    variant_with_ohlc = {**variant, "ohlc": ohlc}
                    default_name = f"{source}{ohlc}"
                    if ohlc == "Open":
                        expr = p.first()
                    elif ohlc == "High":
                        expr = p.max()
                    elif ohlc == "Low":
                        expr = p.min()
                    else:
                        expr = p.last()

                    alias = (
                        req.output_name_pattern.format(**variant_with_ohlc)
                        if req.output_name_pattern
                        else default_name
                    )
                    expressions.append(expr.alias(alias))
                    names[ohlc] = alias

                ohlc_specs.append(
                    {
                        "open_mode": variant.get("open_mode", "event"),
                        "names": names,
                    }
                )

        # --- Legacy Fallback (if config is empty) ---
        if not expressions:
            N = self.config.levels
            for i in range(1, N + 1):
                for side in ["Bid", "Ask"]:
                    expressions.append(pl.col(f"{side}Price{i}").last())
                    expressions.append(pl.col(f"{side}Quantity{i}").last())
                    expressions.append(pl.col(f"{side}NumOrders{i}").last())
                    expressions.append(
                        pl.sum_horizontal(
                            [f"{side}Quantity{j}" for j in range(1, i + 1)]
                        )
                        .last()
                        .alias(f"{side}CumQuantity{i}")
                    )
                    expressions.append(
                        pl.sum_horizontal(
                            [f"{side}NumOrders{j}" for j in range(1, i + 1)]
                        )
                        .last()
                        .alias(f"{side}CumOrders{i}")
                    )

            for i in range(1, N + 1):
                bid_cum = pl.sum_horizontal(
                    [f"BidQuantity{j}" for j in range(1, i + 1)]
                )
                ask_cum = pl.sum_horizontal(
                    [f"AskQuantity{j}" for j in range(1, i + 1)]
                )
                expressions.append(
                    ((bid_cum - ask_cum) / (bid_cum + ask_cum))
                    .last()
                    .alias(f"VolumeImbalance{i}")
                )

                bid_ord = pl.sum_horizontal(
                    [f"BidNumOrders{j}" for j in range(1, i + 1)]
                )
                ask_ord = pl.sum_horizontal(
                    [f"AskNumOrders{j}" for j in range(1, i + 1)]
                )
                expressions.append(
                    ((bid_ord - ask_ord) / (bid_ord + ask_ord))
                    .last()
                    .alias(f"OrdersImbalance{i}")
                )

            expressions.append(
                (
                    20000
                    * (pl.col("AskPrice1") - pl.col("BidPrice1"))
                    / (pl.col("AskPrice1") + pl.col("BidPrice1"))
                )
                .last()
                .alias("SpreadBps")
            )

        # Add MarketState
        expressions.append(pl.col("MarketState").last())

        # Execute Aggregation
        l2_last = self.l2.group_by(gcols).agg(expressions)

        if any(spec["open_mode"] == "prev_close" for spec in ohlc_specs):
            l2_last = self._ensure_dense_time_buckets(l2_last, gcols)
            for spec in ohlc_specs:
                if spec["open_mode"] != "prev_close":
                    continue
                l2_last = self._apply_prev_close_ohlc(
                    l2_last, gcols, spec["names"]
                )

        self.df = l2_last
        return l2_last

    def _ensure_dense_time_buckets(
        self, df: pl.LazyFrame, gcols: list[str]
    ) -> pl.LazyFrame:
        if not self.config.time_bucket_seconds:
            raise ValueError("time_bucket_seconds must be set for OHLC prev_close mode.")
        group_cols = [c for c in gcols if c != "TimeBucket"]
        df = df.with_columns(pl.col("TimeBucket").cast(pl.Datetime("ns")))
        interval = int(self.config.time_bucket_seconds * 1e9)
        frequency = f"{interval}ns"

        ranges = (
            df.group_by(group_cols)
            .agg(
                pl.datetime_range(
                    start=pl.col("TimeBucket").min(),
                    end=pl.col("TimeBucket").max(),
                    interval=frequency,
                    closed="both",
                )
                .cast(pl.Datetime("ns"))
                .alias("TimeBucket")
            )
            .explode("TimeBucket")
        )

        return ranges.join(df, on=group_cols + ["TimeBucket"], how="left")

    def _apply_prev_close_ohlc(
        self, df: pl.LazyFrame, gcols: list[str], names: dict[str, str]
    ) -> pl.LazyFrame:
        group_cols = [c for c in gcols if c != "TimeBucket"]
        open_col = names["Open"]
        high_col = names["High"]
        low_col = names["Low"]
        close_col = names["Close"]
        temp_col = f"__{close_col}_filled"

        no_event = (
            pl.col(open_col).is_null()
            & pl.col(high_col).is_null()
            & pl.col(low_col).is_null()
            & pl.col(close_col).is_null()
        )

        df = df.with_columns(
            pl.when(no_event)
            .then(pl.col(close_col).shift(1).over(group_cols))
            .otherwise(pl.col(close_col))
            .alias(temp_col)
        ).with_columns(pl.col(temp_col).forward_fill().over(group_cols).alias(temp_col))

        return df.with_columns(
            [
                pl.when(no_event)
                .then(pl.col(temp_col))
                .otherwise(pl.col(open_col))
                .alias(open_col),
                pl.when(no_event)
                .then(pl.col(temp_col))
                .otherwise(pl.col(high_col))
                .alias(high_col),
                pl.when(no_event)
                .then(pl.col(temp_col))
                .otherwise(pl.col(low_col))
                .alias(low_col),
                pl.col(temp_col).alias(close_col),
            ]
        ).drop(temp_col)


class L2AnalyticsTW(BaseTWAnalytics):
    """
    Computes time-weighted average (TWA) metrics.
    """

    REQUIRES = ["l2"]

    def __init__(self, config: L2AnalyticsConfig):
        super().__init__(
            "l2tw",
            {},  # Dynamic schema
            nanoseconds=int(config.time_bucket_seconds * 1e9),
        )
        self.config = config

    def tw_analytics(self, l2: pl.LazyFrame, **kwargs) -> pl.LazyFrame:
        l2 = l2.group_by(["ListingId", "TimeBucket"])

        expressions = []

        # Helper for TWA: (col * DT).sum() / DT.sum()
        def twa(expr, market_states: Optional[List[str]] = None):
            if market_states:
                expr = expr.filter(pl.col("MarketState").is_in(market_states))
            return (expr * pl.col("DT")).sum() / pl.col("DT").sum()

        # --- Spreads ---
        for req in self.config.spreads:
            if "TWA" in req.aggregations:
                for variant in req.expand():
                    v_type = variant["variant"]
                    if v_type == "Abs":
                        raw = pl.col("AskPrice1") - pl.col("BidPrice1")
                    elif v_type == "BPS":
                        raw = (
                            20000
                            * (pl.col("AskPrice1") - pl.col("BidPrice1"))
                            / (pl.col("AskPrice1") + pl.col("BidPrice1"))
                        )

                    # PascalCase: SpreadAbsTWA, SpreadBPSTWA
                    alias = (
                        req.output_name_pattern.format(**variant)
                        if req.output_name_pattern
                        else f"Spread{v_type}TWA"
                    )
                    expressions.append(twa(raw, req.market_states).alias(alias))

        # --- Liquidity ---
        for req in self.config.liquidity:
            if "TWA" in req.aggregations:
                for variant in req.expand():
                    side = variant["sides"]
                    level = variant["levels"]
                    measure = variant["measures"]

                    raw = None
                    if measure == "Price":
                        raw = pl.col(f"{side}Price{level}")
                    elif measure == "Quantity":
                        raw = pl.col(f"{side}Quantity{level}")
                    elif measure == "NumOrders":
                        raw = pl.col(f"{side}NumOrders{level}")
                    elif measure == "CumQuantity":
                        cols = [f"{side}Quantity{i}" for i in range(1, level + 1)]
                        raw = pl.sum_horizontal(cols)
                    elif measure == "CumOrders":
                        cols = [f"{side}NumOrders{i}" for i in range(1, level + 1)]
                        raw = pl.sum_horizontal(cols)
                    elif measure == "CumNotional":
                        notional_exprs = [
                            pl.col(f"{side}Price{i}") * pl.col(f"{side}Quantity{i}")
                            for i in range(1, level + 1)
                        ]
                        raw = pl.sum_horizontal(notional_exprs)

                    if raw is not None:
                        # PascalCase: BidQuantity1TWA
                        alias = (
                            req.output_name_pattern.format(**variant)
                            if req.output_name_pattern
                            else f"{side}{measure}{level}TWA"
                        )
                        expressions.append(twa(raw, req.market_states).alias(alias))

        # --- Imbalances ---
        for req in self.config.imbalances:
            if "TWA" in req.aggregations:
                for variant in req.expand():
                    level = variant["levels"]
                    measure = variant["measure"]

                    # Logic for imbalance: (Bid - Ask) / (Bid + Ask)
                    # We calculate the ratio at each step and then TWA it.

                    if measure == "CumQuantity":
                        bid_val = pl.sum_horizontal(
                            [f"BidQuantity{i}" for i in range(1, level + 1)]
                        )
                        ask_val = pl.sum_horizontal(
                            [f"AskQuantity{i}" for i in range(1, level + 1)]
                        )
                    elif measure == "Orders":
                        bid_val = pl.sum_horizontal(
                            [f"BidNumOrders{i}" for i in range(1, level + 1)]
                        )
                        ask_val = pl.sum_horizontal(
                            [f"AskNumOrders{i}" for i in range(1, level + 1)]
                        )
                    elif measure == "CumNotional":
                        bid_val = pl.sum_horizontal(
                            [
                                pl.col(f"BidPrice{i}") * pl.col(f"BidQuantity{i}")
                                for i in range(1, level + 1)
                            ]
                        )
                        ask_val = pl.sum_horizontal(
                            [
                                pl.col(f"AskPrice{i}") * pl.col(f"AskQuantity{i}")
                                for i in range(1, level + 1)
                            ]
                        )
                    else:
                        # Fallback to simple Quantity at level
                        bid_val = pl.col(f"BidQuantity{level}")
                        ask_val = pl.col(f"AskQuantity{level}")

                    raw = (bid_val - ask_val) / (bid_val + ask_val)

                    # PascalCase: ImbalanceCumQuantity1TWA
                    alias = (
                        req.output_name_pattern.format(**variant)
                        if req.output_name_pattern
                        else f"Imbalance{measure}{level}TWA"
                    )
                    expressions.append(twa(raw, req.market_states).alias(alias))

        # --- Volatility ---
        for req in self.config.volatility:
            for variant in req.expand():
                source = variant["source"]

                # Define price expression
                if source == "Mid":
                    p = (pl.col("AskPrice1") + pl.col("BidPrice1")) / 2
                elif source == "WeightedMid":
                    p = (
                        pl.col("BidPrice1") * pl.col("AskQuantity1")
                        + pl.col("AskPrice1") * pl.col("BidQuantity1")
                    ) / (pl.col("AskQuantity1") + pl.col("BidQuantity1"))
                elif source == "Bid":
                    p = pl.col("BidPrice1")
                elif source == "Ask":
                    p = pl.col("AskPrice1")
                else:
                    continue

                # Log Returns: ln(p_t / p_{t-1})
                log_ret = (p / p.shift(1)).log()

                agg_map = {
                    "First": log_ret.first(),
                    "Last": log_ret.last(),
                    "Min": log_ret.min(),
                    "Max": log_ret.max(),
                    "Mean": log_ret.mean(),
                    "Sum": log_ret.sum(),
                    "Median": log_ret.median(),
                    "Std": log_ret.std(),
                }

                for agg in req.aggregations:
                    if agg not in agg_map:
                        logging.warning(
                            f"Unsupported aggregation '{agg}' for L2 volatility."
                        )
                        continue

                    expr = agg_map[agg]

                    if agg == "Std":
                        # Annualization Factor
                        # We estimate frequency as Count / TimeBucketSeconds
                        # Annualized Vol = Std * Sqrt(SecondsInYear * Frequency)
                        #                = Std * Sqrt(SecondsInYear * Count / TimeBucketSeconds)
                        # SecondsInYear = 252 * 24 * 60 * 60 (Trading Year assumption)
                        seconds_in_year = 252 * 24 * 60 * 60
                        bucket_seconds = self.config.time_bucket_seconds

                        # Avoid division by zero if bucket_seconds is somehow 0 (unlikely)
                        factor = (seconds_in_year * pl.len() / bucket_seconds).sqrt()
                        expr = expr * factor

                    alias = (
                        req.output_name_pattern.format(**variant, agg=agg)
                        if req.output_name_pattern
                        else f"L2Volatility{source}{agg}"
                    )
                    expressions.append(expr.alias(alias))

        # Legacy Fallback
        if not expressions:
            expressions.append(
                twa(
                    20000
                    * (pl.col("AskPrice1") - pl.col("BidPrice1"))
                    / (pl.col("AskPrice1") + pl.col("BidPrice1"))
                ).alias("SpreadRelTWA")
            )
            expressions.append(
                twa(0.5 * (pl.col("AskPrice1") + pl.col("BidPrice1"))).alias("MidTWA")
            )
            expressions.append(twa(pl.col("AskPrice1")).alias("AskTWA"))
            expressions.append(twa(pl.col("BidPrice1")).alias("BidTWA"))

        # Always add EventCount
        expressions.append(pl.col("EventTimestamp").len().alias("EventCount"))

        return l2.agg(expressions)


# Metric documentation (used by schema enumeration)
@metric_doc(
    module="l2_last",
    pattern=r"^(?P<side>Bid|Ask)(?P<measure>Price|CumNotional)(?P<level>\d+)$",
    template="{side} {measure} at book level {level} (last snapshot in TimeBucket).",
    unit="XLOC",
)
def _doc_l2_last_liquidity_price():
    pass


@metric_doc(
    module="l2_last",
    pattern=r"^(?P<source>Bid|Ask|Mid|WeightedMid)(?P<ohlc>Open|High|Low|Close)$",
    template="L2 {ohlc} for {source} price within the TimeBucket.",
    unit="XLOC",
    group="L2OHLC{source}",
    group_role="{ohlc}",
    group_semantics="ohlc_bar,ffill_non_naive",
)
def _doc_l2_last_ohlc():
    pass


@metric_doc(
    module="l2_last",
    pattern=r"^(?P<side>Bid|Ask)(?P<measure>Quantity|CumQuantity|SizeAhead)(?P<level>\d+)$",
    template="{side} {measure} at book level {level} (last snapshot in TimeBucket).",
    unit="Shares",
)
def _doc_l2_last_liquidity_qty():
    pass


@metric_doc(
    module="l2_last",
    pattern=r"^(?P<side>Bid|Ask)(?P<measure>NumOrders|CumOrders)(?P<level>\d+)$",
    template="{side} {measure} at book level {level} (last snapshot in TimeBucket).",
    unit="Orders",
)
def _doc_l2_last_liquidity():
    pass


@metric_doc(
    module="l2_last",
    pattern=r"^(?P<side>Bid|Ask)(?P<measure>InsertAge|LastMod)(?P<level>\d+)$",
    template="{side} {measure} at book level {level} (last snapshot in TimeBucket).",
    unit="Nanoseconds",
)
def _doc_l2_last_liquidity_time():
    pass


@metric_doc(
    module="l2_last",
    pattern=r"^SpreadAbs$",
    template="Best ask minus best bid in absolute price terms using the last snapshot in the TimeBucket.",
    unit="XLOC",
)
def _doc_l2_last_spread_abs():
    pass


@metric_doc(
    module="l2_last",
    pattern=r"^SpreadBPS$",
    template="Best ask minus best bid in basis points using the last snapshot in the TimeBucket.",
    unit="BPS",
)
def _doc_l2_last_spread():
    pass


@metric_doc(
    module="l2_last",
    pattern=r"^SpreadBps$",
    template="Best ask minus best bid in basis points using the last snapshot in the TimeBucket.",
    unit="BPS",
)
def _doc_l2_last_spread_alt():
    pass


@metric_doc(
    module="l2_last",
    pattern=r"^Imbalance(?P<measure>CumQuantity|Orders|CumNotional)(?P<level>\d+)$",
    template="Order book imbalance for {measure} up to level {level} using the last snapshot, expressed as a normalized difference between bid and ask.",
    unit="Imbalance",
)
def _doc_l2_last_imbalance():
    pass


@metric_doc(
    module="l2_last",
    pattern=r"^(VolumeImbalance|OrdersImbalance)(?P<level>\d+)$",
    template="Order book imbalance for volume or orders up to level {level} using the last snapshot, expressed as a normalized difference between bid and ask.",
    unit="Imbalance",
)
def _doc_l2_last_imbalance_alt():
    pass


@metric_doc(
    module="l2_last",
    pattern=r"^L2Volatility(?P<source>Mid|Bid|Ask|WeightedMid)(?P<agg>First|Last|Min|Max|Mean|Sum|Median|Std)$",
    template="Aggregation ({agg}) of log-returns of {source} price within TimeBucket; Std is annualized.",
    unit="Percentage",
)
def _doc_l2_last_volatility():
    pass


@metric_doc(
    module="l2_tw",
    pattern=r"^(?P<side>Bid|Ask)(?P<measure>Price|CumNotional)(?P<level>\d+)TWA$",
    template="{side} {measure} at level {level}, time-weighted average over TimeBucket.",
    unit="XLOC",
)
def _doc_l2_tw_liquidity_price():
    pass


@metric_doc(
    module="l2_tw",
    pattern=r"^(?P<side>Bid|Ask)(?P<measure>Quantity|CumQuantity|SizeAhead)(?P<level>\d+)TWA$",
    template="{side} {measure} at level {level}, time-weighted average over TimeBucket.",
    unit="Shares",
)
def _doc_l2_tw_liquidity_qty():
    pass


@metric_doc(
    module="l2_tw",
    pattern=r"^(?P<side>Bid|Ask)(?P<measure>NumOrders|CumOrders)(?P<level>\d+)TWA$",
    template="{side} {measure} at level {level}, time-weighted average over TimeBucket.",
    unit="Orders",
)
def _doc_l2_tw_liquidity():
    pass


@metric_doc(
    module="l2_tw",
    pattern=r"^(?P<side>Bid|Ask)(?P<measure>InsertAge|LastMod)(?P<level>\d+)TWA$",
    template="{side} {measure} at level {level}, time-weighted average over TimeBucket.",
    unit="Nanoseconds",
)
def _doc_l2_tw_liquidity_time():
    pass


@metric_doc(
    module="l2_tw",
    pattern=r"^SpreadAbsTWA$",
    template="Time-weighted average of spread in absolute price terms within the TimeBucket.",
    unit="XLOC",
)
def _doc_l2_tw_spread_abs():
    pass


@metric_doc(
    module="l2_tw",
    pattern=r"^SpreadBPSTWA$",
    template="Time-weighted average of spread in basis points within the TimeBucket.",
    unit="BPS",
)
def _doc_l2_tw_spread():
    pass


@metric_doc(
    module="l2_tw",
    pattern=r"^SpreadRelTWA$",
    template="Time-weighted average of spread in relative terms within the TimeBucket.",
    unit="BPS",
)
def _doc_l2_tw_spread_rel():
    pass


@metric_doc(
    module="l2_tw",
    pattern=r"^Imbalance(?P<measure>CumQuantity|Orders|CumNotional)(?P<level>\d+)TWA$",
    template="Time-weighted average of order book imbalance for {measure} up to level {level}, expressed as a normalized difference between bid and ask.",
    unit="Imbalance",
)
def _doc_l2_tw_imbalance():
    pass


@metric_doc(
    module="l2_tw",
    pattern=r"^L2Volatility(?P<source>Mid|Bid|Ask|WeightedMid)(?P<agg>First|Last|Min|Max|Mean|Sum|Median|Std)$",
    template="Aggregation ({agg}) of log-returns of {source} price within TimeBucket; Std is annualized.",
    unit="Percentage",
)
def _doc_l2_tw_volatility():
    pass


@metric_doc(
    module="l2_tw",
    pattern=r"^EventCount$",
    template="Number of L2 events in the TimeBucket.",
    unit="Orders",
)
def _doc_l2_tw_event_count():
    pass


@metric_doc(
    module="l2_tw",
    pattern=r"^(?P<source>Mid|Ask|Bid)TWA$",
    template="Time-weighted average of {source} price within the TimeBucket.",
    unit="XLOC",
)
def _doc_l2_tw_mid_prices():
    pass
