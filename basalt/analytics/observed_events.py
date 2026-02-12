from __future__ import annotations

from typing import List, Optional, Literal

import pandas as pd
import polars as pl
from pydantic import BaseModel, Field, ConfigDict

from basalt.analytics_base import BaseAnalytics
from basalt.analytics_registry import register_analytics


class ObservedEventsAnalyticsConfig(BaseModel):
    """
    Event detection on derived indicators (SMA / EWMA).

    Emits sparse rows when local minima/maxima are detected on an indicator.
    Intended to run as a dedicated pass (sparse output).
    """

    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "observed_events",
                "tier": "post",
                "desc": "Postprocessing: observed events (local min/max on SMA/EWMA).",
                "outputs": ["EventType", "IndicatorValue"],
                "schema_keys": ["observed_events"],
            }
        }
    )

    ENABLED: bool = True
    metric_prefix: Optional[str] = Field(
        None,
        description="Optional prefix (not used for event rows).",
    )
    source_pass: str = Field(
        "pass1",
        description="Pass name to use as input.",
    )
    input_col: str = Field(
        "Close",
        description="Input column for indicator.",
    )
    indicator: Literal["sma", "ewma"] = Field(
        "sma",
        description="Indicator to compute (sma or ewma).",
    )
    window: int = Field(
        5,
        gt=1,
        description="Window size for indicator.",
    )
    event_types: List[Literal["local_min", "local_max"]] = Field(
        default_factory=lambda: ["local_min", "local_max"],
        description="Which event types to emit.",
    )


@register_analytics("observed_events", config_attr="observed_events_analytics")
class ObservedEventsAnalytics(BaseAnalytics):
    REQUIRES: List[str] = []

    def __init__(self, config: ObservedEventsAnalyticsConfig):
        super().__init__(
            "observed_events",
            {},
            join_keys=["ListingId", "TimeBucket"],
            metric_prefix=config.metric_prefix,
        )
        self.config = config

    def compute(self) -> pl.LazyFrame:
        source_df = self.context.get(self.config.source_pass)
        if source_df is None:
            raise ValueError(
                f"Source pass '{self.config.source_pass}' not found in context."
            )
        if isinstance(source_df, pl.LazyFrame):
            df = source_df.collect()
        elif isinstance(source_df, pl.DataFrame):
            df = source_df
        else:
            raise ValueError("Unsupported source pass type.")

        pdf = df.to_pandas()
        if "TimeBucket" not in pdf.columns:
            raise ValueError("TimeBucket column required for event detection.")
        if self.config.input_col not in pdf.columns:
            raise ValueError(f"Input column {self.config.input_col} not found.")

        pdf = pdf.sort_values(["ListingId", "TimeBucket"])
        group = pdf.groupby("ListingId", sort=False)[self.config.input_col]
        if self.config.indicator == "ewma":
            indicator = group.apply(lambda s: s.ewm(span=self.config.window).mean())
        else:
            indicator = group.apply(lambda s: s.rolling(self.config.window).mean())
        pdf["__indicator"] = indicator.reset_index(level=0, drop=True)

        prev_val = pdf["__indicator"].groupby(pdf["ListingId"]).shift(1)
        next_val = pdf["__indicator"].groupby(pdf["ListingId"]).shift(-1)

        events = []
        if "local_min" in self.config.event_types:
            mask = (pdf["__indicator"] < prev_val) & (pdf["__indicator"] < next_val)
            events.append(pdf.loc[mask].assign(EventType="local_min"))
        if "local_max" in self.config.event_types:
            mask = (pdf["__indicator"] > prev_val) & (pdf["__indicator"] > next_val)
            events.append(pdf.loc[mask].assign(EventType="local_max"))

        if not events:
            return pl.DataFrame(
                columns=["ListingId", "TimeBucket", "EventType", "Indicator", "IndicatorValue"]
            ).lazy()

        out = pd.concat(events, ignore_index=True)
        out["Indicator"] = self.config.indicator
        out["IndicatorValue"] = out["__indicator"]
        out = out[["ListingId", "TimeBucket", "EventType", "Indicator", "IndicatorValue"]]
        return pl.from_pandas(out).lazy()
