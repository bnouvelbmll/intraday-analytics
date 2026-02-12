from __future__ import annotations

from typing import List, Literal, Optional, Union

import polars as pl
from pydantic import BaseModel, Field, ConfigDict

from basalt.analytics_base import BaseAnalytics, MarketState
from basalt.analytics_registry import register_analytics
from basalt.analytics.utils import apply_market_state_filter


class CBBOPreprocessConfig(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "cbbo_preprocess",
                "tier": "pre",
                "desc": "Pre: combine L2 across listings into a composite book.",
                "outputs": ["CBBO"],
                "schema_keys": ["cbbo_preprocess"],
            }
        }
    )

    ENABLED: bool = True
    index_by: Literal["InstrumentId", "PrimaryListingId"] = Field(
        "InstrumentId",
        description="Index output by InstrumentId or PrimaryListingId.",
    )
    levels_in: int = Field(10, ge=1, description="L2 depth levels to ingest.")
    top_n: int = Field(10, ge=1, le=10, description="Top N price levels to output.")
    time_index: Literal["event", "timebucket"] = Field(
        "timebucket",
        description="Use EventTimestamp or TimeBucket as the time index.",
    )
    emit_last_event_ts: bool = Field(
        False,
        description="When time_index='timebucket', include LastEventTimestamp.",
    )
    market_states: Optional[Union[MarketState, List[MarketState]]] = Field(
        default_factory=lambda: ["CONTINUOUS_TRADING"],
        description="Optional MarketState filter.",
    )
    include_mic_wide: bool = Field(
        False,
        description="Include per-MIC prefixed L2 columns for debugging.",
    )
    mic_whitelist: Optional[List[str]] = Field(
        None,
        description="Optional list of MICs to include.",
    )


@register_analytics("cbbo_preprocess", config_attr="cbbo_preprocess", needs_ref=True)
class CBBOPreprocess(BaseAnalytics):
    """
    Builds a combined L2 book per InstrumentId from per-listing L2 snapshots.
    """

    REQUIRES = ["l2"]
    BATCH_GROUP_BY = "InstrumentId"

    def __init__(self, ref: pl.DataFrame, config: CBBOPreprocessConfig):
        self.ref = ref
        self.config = config
        super().__init__("cbbo_preprocess", {})

    def _primary_listing_map(self) -> pl.DataFrame:
        if self.ref is None or self.ref.is_empty():
            return pl.DataFrame({"InstrumentId": [], "PrimaryListingId": []})
        cols = self.ref.columns
        if "InstrumentId" not in cols or "ListingId" not in cols:
            return pl.DataFrame({"InstrumentId": [], "PrimaryListingId": []})
        if "IsPrimary" in cols:
            primary = (
                self.ref.sort(
                    ["InstrumentId", "IsPrimary"],
                    descending=[False, True],
                )
                .group_by("InstrumentId")
                .agg(pl.col("ListingId").first().alias("PrimaryListingId"))
            )
        else:
            primary = (
                self.ref.group_by("InstrumentId")
                .agg(pl.col("ListingId").first().alias("PrimaryListingId"))
            )
        return primary

    def _available_levels(self, cols: list[str]) -> list[int]:
        levels = []
        for i in range(1, self.config.levels_in + 1):
            if f"BidPrice{i}" in cols and f"AskPrice{i}" in cols:
                levels.append(i)
        return levels

    def compute(self) -> pl.LazyFrame:
        l2 = self.l2.lazy() if isinstance(self.l2, pl.DataFrame) else self.l2
        if l2 is None:
            raise ValueError("CBBOPreprocess requires L2 data.")
        if self.ref is None:
            raise ValueError("CBBOPreprocess requires ref with InstrumentId.")

        schema_cols = l2.collect_schema().names()
        levels = self._available_levels(schema_cols)
        if not levels:
            raise ValueError("No L2 price levels found to combine.")

        ref_cols = [c for c in ["ListingId", "InstrumentId", "MIC", "IsPrimary"] if c in self.ref.columns]
        ref_df = self.ref.select(ref_cols)

        l2 = l2.join(ref_df.lazy(), on="ListingId", how="left")
        if "MIC" in schema_cols:
            l2 = l2.with_columns(
                MIC=pl.coalesce([pl.col("MIC"), pl.col("MIC_right")])
            ).drop([c for c in ["MIC_right"] if c in l2.collect_schema().names()])

        if self.config.mic_whitelist:
            l2 = l2.filter(pl.col("MIC").is_in(self.config.mic_whitelist))

        market_states = self.config.market_states
        if isinstance(market_states, str):
            market_states = [market_states]
        if market_states is not None and "MarketState" in schema_cols:
            l2 = apply_market_state_filter(l2, market_states)

        time_col = "EventTimestamp" if self.config.time_index == "event" else "TimeBucket"

        primary_map = self._primary_listing_map()
        if not primary_map.is_empty():
            l2 = l2.join(primary_map.lazy(), on="InstrumentId", how="left")
        else:
            l2 = l2.with_columns(PrimaryListingId=pl.lit(None))

        index_col = "InstrumentId"
        if self.config.index_by == "PrimaryListingId" and "PrimaryListingId" in l2.collect_schema().names():
            index_col = "PrimaryListingId"
        if self.config.time_index != "event" and "TimeBucket" not in schema_cols:
            raise ValueError("TimeBucket missing but time_index='timebucket' requested.")

        self.join_keys = [index_col, time_col]

        frames: list[pl.LazyFrame] = []
        for i in levels:
            frames.append(
                l2.select(
                    [
                        pl.col(index_col),
                        pl.col(time_col),
                        pl.col(f"BidPrice{i}").alias("Price"),
                        pl.col(f"BidQuantity{i}").alias("Quantity"),
                        pl.col(f"BidNumOrders{i}").alias("Count"),
                        pl.lit("Bid").alias("Side"),
                    ]
                )
            )
            frames.append(
                l2.select(
                    [
                        pl.col(index_col),
                        pl.col(time_col),
                        pl.col(f"AskPrice{i}").alias("Price"),
                        pl.col(f"AskQuantity{i}").alias("Quantity"),
                        pl.col(f"AskNumOrders{i}").alias("Count"),
                        pl.lit("Ask").alias("Side"),
                    ]
                )
            )

        long_df = pl.concat(frames).filter(pl.col("Price").is_not_null())
        long_df = long_df.with_columns(
            pl.col("Quantity").fill_null(0),
            pl.col("Count").fill_null(0),
        )

        grouped = (
            long_df.group_by([index_col, time_col, "Side", "Price"])
            .agg(
                [
                    pl.col("Quantity").sum().alias("Quantity"),
                    pl.col("Count").sum().alias("Count"),
                ]
            )
        )

        bid = grouped.filter(pl.col("Side") == "Bid").sort(
            [index_col, time_col, "Price"],
            descending=[False, False, True],
        )
        ask = grouped.filter(pl.col("Side") == "Ask").sort(
            [index_col, time_col, "Price"],
            descending=[False, False, False],
        )

        bid_out = bid.group_by([index_col, time_col], maintain_order=True).agg(
            [
                pl.col("Price").head(self.config.top_n).alias("BidPrice"),
                pl.col("Quantity").head(self.config.top_n).alias("BidQuantity"),
                pl.col("Count").head(self.config.top_n).alias("BidNumOrders"),
            ]
        )
        ask_out = ask.group_by([index_col, time_col], maintain_order=True).agg(
            [
                pl.col("Price").head(self.config.top_n).alias("AskPrice"),
                pl.col("Quantity").head(self.config.top_n).alias("AskQuantity"),
                pl.col("Count").head(self.config.top_n).alias("AskNumOrders"),
            ]
        )

        def _expand(df: pl.LazyFrame, prefix: str, col: str) -> list[pl.Expr]:
            return [
                pl.col(col)
                .list.get(i, null_on_oob=True)
                .alias(f"{prefix}{i + 1}")
                for i in range(self.config.top_n)
            ]

        out = bid_out.join(ask_out, on=[index_col, time_col], how="full")
        out = out.with_columns(
            _expand(out, "BidPrice", "BidPrice")
            + _expand(out, "BidQuantity", "BidQuantity")
            + _expand(out, "BidNumOrders", "BidNumOrders")
            + _expand(out, "AskPrice", "AskPrice")
            + _expand(out, "AskQuantity", "AskQuantity")
            + _expand(out, "AskNumOrders", "AskNumOrders")
        ).drop(
            [
                "BidPrice",
                "BidQuantity",
                "BidNumOrders",
                "AskPrice",
                "AskQuantity",
                "AskNumOrders",
            ]
        )

        if self.config.emit_last_event_ts and self.config.time_index != "event":
            last_ts = (
                l2.group_by([index_col, time_col])
                .agg(pl.col("EventTimestamp").max().alias("LastEventTimestamp"))
            )
            out = out.join(last_ts, on=[index_col, time_col], how="left")

        if self.config.include_mic_wide:
            mic_cols = []
            for i in levels:
                mic_cols.extend(
                    [
                        f"BidPrice{i}",
                        f"AskPrice{i}",
                        f"BidQuantity{i}",
                        f"AskQuantity{i}",
                        f"BidNumOrders{i}",
                        f"AskNumOrders{i}",
                    ]
                )
            l2_mic = l2.group_by([index_col, time_col, "MIC"]).agg(
                [pl.col(c).last().alias(c) for c in mic_cols if c in schema_cols]
            )
            wide = l2_mic.pivot(
                values=[c for c in mic_cols if c in schema_cols],
                index=[index_col, time_col],
                columns="MIC",
                aggregate_function="first",
            )
            out = out.join(wide, on=[index_col, time_col], how="left")

        self.df = out
        return out
