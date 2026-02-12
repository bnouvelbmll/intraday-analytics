from __future__ import annotations

from typing import Iterable, List, Literal, Optional, Union

import polars as pl
from pydantic import BaseModel, ConfigDict, Field

from basalt.analytics_base import BaseAnalytics, MarketState
from basalt.analytics_registry import register_analytics
from basalt.rebuilder import rebuild_l2_from_l3


class CBBOfromL3PreprocessConfig(BaseModel):
    """
    Rebuild a combined L2/CBBO-like book directly from L3 deltas.

    Notes on time settings:
    - `time_index="event"`: output is event-timestamp indexed; `time_bucket_seconds`
      is ignored.
    - `time_index="timebucket"`: output is bucketed; `time_bucket_seconds` is used
      to build ceil-truncated bucket labels.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "cbbofroml3_preprocess",
                "tier": "pre",
                "desc": "Pre: rebuild combined CBBO book from L3 deltas.",
                "outputs": ["BidPrice1", "AskPrice1", "BidQuantity1", "AskQuantity1"],
                "schema_keys": ["cbbofroml3_preprocess"],
            }
        }
    )

    ENABLED: bool = True
    index_by: Literal["InstrumentId", "PrimaryListingId"] = Field(
        "InstrumentId",
        description="Index output by InstrumentId or PrimaryListingId.",
    )
    time_index: Literal["event", "timebucket"] = Field(
        "timebucket",
        description="Use EventTimestamp or TimeBucket as the time index.",
    )
    time_bucket_seconds: int = Field(
        60,
        ge=1,
        description=(
            "Bucket size in seconds used when time_index='timebucket'. "
            "Ignored when time_index='event'."
        ),
    )
    top_n: int = Field(10, ge=1, le=10, description="Top N levels to output.")
    emit_last_event_ts: bool = Field(
        False,
        description="When time_index='timebucket', include LastEventTimestamp.",
    )
    market_states: Optional[Union[MarketState, List[MarketState]]] = Field(
        default_factory=lambda: ["CONTINUOUS_TRADING"],
        description="Optional MarketState filter.",
    )


@register_analytics(
    "cbbofroml3_preprocess",
    config_attr="cbbofroml3_preprocess",
    needs_ref=True,
)
class CBBOfromL3Preprocess(BaseAnalytics):
    REQUIRES = ["l3"]
    BATCH_GROUP_BY = "InstrumentId"

    def __init__(self, ref: pl.DataFrame, config: CBBOfromL3PreprocessConfig):
        self.ref = ref
        self.config = config
        super().__init__("cbbofroml3_preprocess", {})

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
            primary = self.ref.group_by("InstrumentId").agg(
                pl.col("ListingId").first().alias("PrimaryListingId")
            )
        return primary

    def _market_states(self) -> Iterable[str] | None:
        states = self.config.market_states
        if states is None:
            return None
        if isinstance(states, str):
            return [states]
        return states

    def compute(self) -> pl.LazyFrame:
        if self.ref is None:
            raise ValueError("CBBOfromL3Preprocess requires ref with InstrumentId.")
        l3 = self.l3.lazy() if isinstance(self.l3, pl.DataFrame) else self.l3
        if l3 is None:
            raise ValueError("CBBOfromL3Preprocess requires L3 data.")

        ref_cols = [c for c in ["ListingId", "InstrumentId", "IsPrimary"] if c in self.ref.columns]
        if "ListingId" not in ref_cols or "InstrumentId" not in ref_cols:
            raise ValueError("Reference must include ListingId and InstrumentId.")
        l3 = l3.join(self.ref.select(ref_cols).lazy(), on="ListingId", how="left")

        index_col = "InstrumentId"
        if self.config.index_by == "PrimaryListingId":
            primary_map = self._primary_listing_map()
            l3 = l3.join(primary_map.lazy(), on="InstrumentId", how="left")
            index_col = "PrimaryListingId"

        out = rebuild_l2_from_l3(
            l3,
            group_cols=[index_col],
            timestamp_col="EventTimestamp",
            time_index=self.config.time_index,
            bucket_seconds=int(self.config.time_bucket_seconds),
            top_n=self.config.top_n,
            market_states=self._market_states(),
        )

        if self.config.time_index == "event":
            out = out.rename({"__event_time": "EventTimestamp"})
            self.join_keys = [index_col, "EventTimestamp"]
        else:
            self.join_keys = [index_col, "TimeBucket"]
            if self.config.emit_last_event_ts:
                last_ts = (
                    l3.with_columns(
                        TimeBucket=pl.when(
                            pl.col("EventTimestamp")
                            == pl.col("EventTimestamp").dt.truncate(
                                f"{int(self.config.time_bucket_seconds)}s"
                            )
                        )
                        .then(pl.col("EventTimestamp"))
                        .otherwise(
                            pl.col("EventTimestamp").dt.truncate(
                                f"{int(self.config.time_bucket_seconds)}s"
                            )
                            + pl.duration(seconds=int(self.config.time_bucket_seconds))
                        )
                    )
                    .group_by([index_col, "TimeBucket"])
                    .agg(pl.col("EventTimestamp").max().alias("LastEventTimestamp"))
                )
                out = out.join(last_ts, on=[index_col, "TimeBucket"], how="left")

        self.df = out
        return out
