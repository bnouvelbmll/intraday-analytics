from __future__ import annotations

from typing import List, Tuple, Literal

import polars as pl
import pandas as pd
from pydantic import BaseModel, Field, ConfigDict, model_validator

from basalt.analytics_base import BaseAnalytics, analytic_doc
from basalt.analytics_registry import register_analytics


class ExternalEventsAnalyticsConfig(BaseModel):
    """
    Timeline control based on an external/source event stream.

    This module defines the event timeline used by downstream analytics in
    `timeline_mode="event"` passes by exposing `ListingId` + `TimeBucket`
    from a selected source pass/table.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "external_events",
                "tier": "post",
                "desc": "Time control: use external/source event timestamps as timeline.",
                "outputs": ["ListingId", "TimeBucket"],
                "schema_keys": ["external_events"],
            }
        }
    )

    ENABLED: bool = Field(
        True,
        description="Enable or disable external-events timeline generation for this pass.",
    )
    source_pass: str = Field(
        "pass1",
        description="Source pass or input table to use for event timestamps.",
    )
    symbol_col: str = Field(
        "ListingId",
        description="Symbol identifier column in source.",
    )
    time_col: str = Field(
        "TimeBucket",
        description="Timestamp column in source.",
    )
    passthrough_columns: List[str] = Field(
        default_factory=list,
        description="Optional source columns to preserve in external event rows.",
    )
    radius_seconds: float = Field(
        0.0,
        ge=0.0,
        description="Time radius (in seconds) around each anchor event.",
    )
    directions: Literal["both", "backward", "forward"] = Field(
        "both",
        description="Which directions to generate around each anchor event.",
    )
    extra_observations: int = Field(
        0,
        ge=0,
        description="Number of extra observations in each enabled direction.",
        json_schema_extra={
            "long_doc": "If > 0, generates synthetic timestamps in each selected direction.\n"
            "Rows are tagged with EventContextIndex (<0 before, >0 after, 0 at anchor)."
        },
    )
    scale_factor: float = Field(
        1.0,
        ge=1.0,
        description="1.0 => arithmetic spacing; >1.0 => geometric spacing base.",
        json_schema_extra={
            "long_doc": "Examples with radius=10s and extra_observations=3:\n"
            "scale_factor=1.0 -> 3.33s, 6.67s, 10s\n"
            "scale_factor=10.0 -> 0.1s, 1s, 10s"
        },
    )

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_context(cls, data):
        if not isinstance(data, dict):
            return data
        out = dict(data)
        has_legacy = any(
            k in out
            for k in (
                "context_before",
                "context_after",
                "context_horizon_seconds",
                "context_distribution",
                "geometric_base",
            )
        )
        if not has_legacy:
            return out
        before = int(out.get("context_before", 0) or 0)
        after = int(out.get("context_after", 0) or 0)
        horizon = float(out.get("context_horizon_seconds", 0.0) or 0.0)
        dist = str(out.get("context_distribution", "arithmetic") or "arithmetic")
        base = float(out.get("geometric_base", 2.0) or 2.0)

        if "radius_seconds" not in out:
            out["radius_seconds"] = horizon
        if "extra_observations" not in out:
            out["extra_observations"] = max(before, after)
        if "directions" not in out:
            if before > 0 and after > 0:
                out["directions"] = "both"
            elif before > 0:
                out["directions"] = "backward"
            elif after > 0:
                out["directions"] = "forward"
            else:
                out["directions"] = "both"
        if "scale_factor" not in out:
            out["scale_factor"] = 1.0 if dist == "arithmetic" else max(base, 1.0)
        return out


@analytic_doc(
    module="external_events",
    pattern=r"^EventAnchorTime$",
    template="Original event timestamp before context offsets are applied.",
    unit="Timestamp",
    description=(
        "Anchor timestamp from the selected source event row; context rows are "
        "generated around this anchor."
    ),
)
@analytic_doc(
    module="external_events",
    pattern=r"^EventContextIndex$",
    template="Relative context index around the anchor event.",
    unit="Index",
    description=(
        "Discrete index where 0 is anchor, negative values are backward offsets, "
        "and positive values are forward offsets."
    ),
)
@analytic_doc(
    module="external_events",
    pattern=r"^EventDeltaSeconds$",
    template="Signed time offset from anchor event in seconds.",
    unit="Seconds",
    description=(
        "Time distance between context timestamp and EventAnchorTime; negative is "
        "before anchor and positive is after anchor."
    ),
)
def _register_external_events_docs():
    return None


@register_analytics("external_events", config_attr="external_event_analytics")
class ExternalEventsAnalytics(BaseAnalytics):
    REQUIRES: List[str] = []

    def __init__(self, config: ExternalEventsAnalyticsConfig):
        super().__init__(
            "external_events",
            {},
            join_keys=["ListingId", "TimeBucket", "EventAnchorTime", "EventContextIndex"],
        )
        self.config = config

    @staticmethod
    def build_context_offsets(
        *,
        radius_seconds: float,
        directions: str,
        extra_observations: int,
        scale_factor: float,
    ) -> list[Tuple[int, float]]:
        if radius_seconds <= 0 or extra_observations <= 0:
            return [(0, 0.0)]
        before = extra_observations if directions in {"both", "backward"} else 0
        after = extra_observations if directions in {"both", "forward"} else 0

        def _magnitudes(n: int) -> list[float]:
            if n <= 0:
                return []
            if scale_factor > 1.0:
                denom = scale_factor**n - 1.0
                if denom <= 0:
                    return [radius_seconds * (i / n) for i in range(1, n + 1)]
                return [
                    radius_seconds * ((scale_factor**i - 1.0) / denom)
                    for i in range(1, n + 1)
                ]
            return [radius_seconds * (i / n) for i in range(1, n + 1)]

        out: list[Tuple[int, float]] = []
        before_mags = _magnitudes(before)
        for i, mag in enumerate(before_mags, start=1):
            out.append((-i, -float(mag)))
        out.append((0, 0.0))
        after_mags = _magnitudes(after)
        for i, mag in enumerate(after_mags, start=1):
            out.append((i, float(mag)))
        return out

    def compute(self) -> pl.LazyFrame:
        source_df = self.context.get(self.config.source_pass)
        if source_df is None:
            raise ValueError(
                f"Source '{self.config.source_pass}' not found in context."
            )
        if isinstance(source_df, pl.LazyFrame):
            df = source_df
        elif isinstance(source_df, pl.DataFrame):
            df = source_df.lazy()
        else:
            raise ValueError("Unsupported external events source type.")

        schema = df.collect_schema().names()
        if self.config.symbol_col not in schema:
            raise ValueError(f"symbol_col '{self.config.symbol_col}' missing from source.")
        if self.config.time_col not in schema:
            raise ValueError(f"time_col '{self.config.time_col}' missing from source.")

        cols = [self.config.symbol_col, self.config.time_col]
        for c in self.config.passthrough_columns:
            if c in schema and c not in cols:
                cols.append(c)

        out = df.select(cols)
        rename_map = {}
        if self.config.symbol_col != "ListingId":
            rename_map[self.config.symbol_col] = "ListingId"
        if self.config.time_col != "TimeBucket":
            rename_map[self.config.time_col] = "TimeBucket"
        if rename_map:
            out = out.rename(rename_map)
        out = out.with_columns(
            EventAnchorTime=pl.col("TimeBucket"),
            EventContextIndex=pl.lit(0).cast(pl.Int64),
            EventDeltaSeconds=pl.lit(0.0),
        )

        offsets = self.build_context_offsets(
            radius_seconds=self.config.radius_seconds,
            directions=self.config.directions,
            extra_observations=self.config.extra_observations,
            scale_factor=self.config.scale_factor,
        )
        if len(offsets) == 1:
            return out.sort(["ListingId", "EventAnchorTime", "EventContextIndex"])

        base_pdf = out.collect().to_pandas()
        frames = []
        for idx, delta in offsets:
            temp = base_pdf.copy()
            temp["EventContextIndex"] = idx
            temp["EventDeltaSeconds"] = float(delta)
            temp["TimeBucket"] = temp["EventAnchorTime"] + pd.to_timedelta(delta, unit="s")
            frames.append(temp)
        merged = pd.concat(frames, ignore_index=True)
        return (
            pl.from_pandas(merged)
            .lazy()
            .sort(["ListingId", "EventAnchorTime", "EventContextIndex", "TimeBucket"])
            .unique(
                subset=["ListingId", "EventAnchorTime", "EventContextIndex", "TimeBucket"],
                keep="first",
            )
        )
