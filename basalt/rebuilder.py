from __future__ import annotations

from typing import Iterable, Literal, Sequence

import polars as pl


def ceil_time_bucket_expr(col: str, bucket_seconds: int) -> pl.Expr:
    if bucket_seconds <= 0:
        raise ValueError("bucket_seconds must be > 0")
    truncated = pl.col(col).dt.truncate(f"{bucket_seconds}s")
    return pl.when(pl.col(col) == truncated).then(pl.col(col)).otherwise(
        truncated + pl.duration(seconds=bucket_seconds)
    )


def compile_l3_deltas(
    l3: pl.LazyFrame | pl.DataFrame,
    *,
    group_cols: Sequence[str],
    timestamp_col: str = "EventTimestamp",
    time_index: Literal["event", "timebucket"] = "timebucket",
    bucket_seconds: int = 1,
    lob_action_col: str = "LobAction",
    side_col: str = "Side",
    price_col: str = "Price",
    size_col: str = "Size",
    old_price_col: str = "OldPrice",
    old_size_col: str = "OldSize",
    market_states: Iterable[str] | None = None,
) -> pl.LazyFrame:
    lf = l3.lazy() if isinstance(l3, pl.DataFrame) else l3
    if lf is None:
        raise ValueError("l3 input cannot be None")

    schema = set(lf.collect_schema().names())
    required = set(group_cols) | {
        timestamp_col,
        lob_action_col,
        side_col,
        price_col,
        size_col,
        old_price_col,
        old_size_col,
    }
    missing = sorted(c for c in required if c not in schema)
    if missing:
        raise ValueError(f"Missing required L3 columns: {missing}")

    if market_states is not None and "MarketState" in schema:
        states = list(market_states)
        lf = lf.filter(pl.col("MarketState").is_in(states))

    inserts = lf.filter(pl.col(lob_action_col).is_in([2, 4])).select(
        [pl.col(c) for c in group_cols]
        + [
            pl.col(timestamp_col).alias("__ts"),
            pl.col(side_col).alias("__side"),
            pl.col(price_col).cast(pl.Float64).alias("__price"),
            pl.col(size_col).cast(pl.Float64).alias("__delta"),
        ]
    )
    removes = lf.filter(pl.col(lob_action_col).is_in([3, 4])).select(
        [pl.col(c) for c in group_cols]
        + [
            pl.col(timestamp_col).alias("__ts"),
            pl.col(side_col).alias("__side"),
            pl.col(old_price_col).cast(pl.Float64).alias("__price"),
            (-pl.col(old_size_col).cast(pl.Float64).fill_null(0.0)).alias("__delta"),
        ]
    )

    deltas = (
        pl.concat([inserts, removes], how="vertical")
        .filter(pl.col("__price").is_not_null() & pl.col("__delta").is_not_null())
        .filter(pl.col("__delta") != 0)
    )

    time_col = "__event_time"
    if time_index == "timebucket":
        time_col = "TimeBucket"
        deltas = deltas.with_columns(TimeBucket=ceil_time_bucket_expr("__ts", bucket_seconds))
    elif time_index == "event":
        deltas = deltas.with_columns(__event_time=pl.col("__ts"))
    else:
        raise ValueError(f"Unsupported time_index: {time_index}")

    key_cols = list(group_cols) + [time_col, "__side", "__price"]
    return (
        deltas.group_by(key_cols)
        .agg(pl.col("__delta").sum().alias("DeltaSize"))
        .filter(pl.col("DeltaSize") != 0)
        .sort(key_cols)
    )


def rebuild_level_book_from_deltas(
    deltas: pl.LazyFrame | pl.DataFrame,
    *,
    group_cols: Sequence[str],
    time_col: str,
    top_n: int = 10,
) -> pl.LazyFrame:
    if top_n <= 0:
        raise ValueError("top_n must be > 0")

    lf = deltas.lazy() if isinstance(deltas, pl.DataFrame) else deltas
    if lf is None:
        raise ValueError("deltas input cannot be None")

    keys = list(group_cols) + [time_col]
    by_price = list(group_cols) + ["__side", "__price"]

    states = (
        lf.sort(by_price + [time_col])
        .with_columns(
            pl.col("DeltaSize").cum_sum().over(by_price).alias("Size"),
        )
        .filter(pl.col("Size") > 0)
    )

    bid = states.filter(pl.col("__side") == 1).sort(
        keys + ["__price"],
        descending=[False] * len(keys) + [True],
    )
    ask = states.filter(pl.col("__side") == 2).sort(
        keys + ["__price"],
        descending=[False] * (len(keys) + 1),
    )

    bid_out = bid.group_by(keys, maintain_order=True).agg(
        [
            pl.col("__price").head(top_n).alias("BidPrice"),
            pl.col("Size").head(top_n).alias("BidQuantity"),
        ]
    )
    ask_out = ask.group_by(keys, maintain_order=True).agg(
        [
            pl.col("__price").head(top_n).alias("AskPrice"),
            pl.col("Size").head(top_n).alias("AskQuantity"),
        ]
    )

    out = bid_out.join(ask_out, on=keys, how="full")
    expand = []
    for i in range(top_n):
        expand.extend(
            [
                pl.col("BidPrice").list.get(i, null_on_oob=True).alias(f"BidPrice{i + 1}"),
                pl.col("BidQuantity")
                .list.get(i, null_on_oob=True)
                .alias(f"BidQuantity{i + 1}"),
                pl.col("AskPrice").list.get(i, null_on_oob=True).alias(f"AskPrice{i + 1}"),
                pl.col("AskQuantity")
                .list.get(i, null_on_oob=True)
                .alias(f"AskQuantity{i + 1}"),
            ]
        )

    return out.with_columns(expand).drop(["BidPrice", "BidQuantity", "AskPrice", "AskQuantity"])


def rebuild_l2_from_l3(
    l3: pl.LazyFrame | pl.DataFrame,
    *,
    group_cols: Sequence[str],
    timestamp_col: str = "EventTimestamp",
    time_index: Literal["event", "timebucket"] = "timebucket",
    bucket_seconds: int = 1,
    top_n: int = 10,
    market_states: Iterable[str] | None = None,
) -> pl.LazyFrame:
    deltas = compile_l3_deltas(
        l3,
        group_cols=group_cols,
        timestamp_col=timestamp_col,
        time_index=time_index,
        bucket_seconds=bucket_seconds,
        market_states=market_states,
    )
    time_col = "TimeBucket" if time_index == "timebucket" else "__event_time"
    return rebuild_level_book_from_deltas(
        deltas,
        group_cols=group_cols,
        time_col=time_col,
        top_n=top_n,
    )
