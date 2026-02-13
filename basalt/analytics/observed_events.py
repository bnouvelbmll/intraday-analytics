from __future__ import annotations

from typing import List, Optional, Literal

import polars as pl
from pydantic import BaseModel, Field, ConfigDict

from basalt.analytics_base import BaseAnalytics, analytic_doc
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

    ENABLED: bool = Field(
        True,
        description="Enable or disable observed-event detection for this pass.",
    )
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


@analytic_doc(
    module="observed_events",
    pattern=r"^EventType$",
    template="Detected event label (local minimum or local maximum).",
    unit="Label",
    description=(
        "Categorical signal emitted by event detector for each selected timestamp."
    ),
)
@analytic_doc(
    module="observed_events",
    pattern=r"^Indicator$",
    template="Indicator family used for event detection.",
    unit="Label",
    description="Indicator source name used to compute event turning points.",
)
@analytic_doc(
    module="observed_events",
    pattern=r"^IndicatorValue$",
    template="Indicator value at detected event timestamp.",
    unit="Value",
    description=(
        "Numeric indicator magnitude (SMA/EWMA) at the emitted event row."
    ),
)
def _register_observed_events_docs():
    return None


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
            ldf = source_df
        elif isinstance(source_df, pl.DataFrame):
            ldf = source_df.lazy()
        else:
            raise ValueError("Unsupported source pass type.")

        schema = ldf.collect_schema().names()
        if "TimeBucket" not in schema:
            raise ValueError("TimeBucket column required for event detection.")
        if self.config.input_col not in schema:
            raise ValueError(f"Input column {self.config.input_col} not found.")
        sorted_ldf = ldf.sort(["ListingId", "TimeBucket"])

        if self.config.indicator == "ewma":
            indicator_expr = (
                pl.col(self.config.input_col)
                .cast(pl.Float64)
                .ewm_mean(span=self.config.window)
                .over("ListingId")
            )
        else:
            indicator_expr = (
                pl.col(self.config.input_col)
                .cast(pl.Float64)
                .rolling_mean(window_size=self.config.window)
                .over("ListingId")
            )

        with_indicator = sorted_ldf.with_columns(indicator_expr.alias("__indicator"))
        with_neighbors = with_indicator.with_columns(
            pl.col("__indicator").shift(1).over("ListingId").alias("__prev_indicator"),
            pl.col("__indicator").shift(-1).over("ListingId").alias("__next_indicator"),
        )

        local_min = (pl.col("__indicator") < pl.col("__prev_indicator")) & (
            pl.col("__indicator") < pl.col("__next_indicator")
        )
        local_max = (pl.col("__indicator") > pl.col("__prev_indicator")) & (
            pl.col("__indicator") > pl.col("__next_indicator")
        )

        if "local_min" in self.config.event_types and "local_max" in self.config.event_types:
            event_type_expr = (
                pl.when(local_min)
                .then(pl.lit("local_min"))
                .when(local_max)
                .then(pl.lit("local_max"))
                .otherwise(pl.lit(None, dtype=pl.String))
            )
        elif "local_min" in self.config.event_types:
            event_type_expr = pl.when(local_min).then(pl.lit("local_min")).otherwise(
                pl.lit(None, dtype=pl.String)
            )
        elif "local_max" in self.config.event_types:
            event_type_expr = pl.when(local_max).then(pl.lit("local_max")).otherwise(
                pl.lit(None, dtype=pl.String)
            )
        else:
            return pl.DataFrame(
                schema={
                    "ListingId": pl.Int64,
                    "TimeBucket": pl.Datetime("ns"),
                    "EventType": pl.String,
                    "Indicator": pl.String,
                    "IndicatorValue": pl.Float64,
                }
            ).lazy()

        return (
            with_neighbors.with_columns(event_type_expr.alias("EventType"))
            .filter(pl.col("EventType").is_not_null())
            .select(
                [
                    "ListingId",
                    "TimeBucket",
                    "EventType",
                    pl.lit(self.config.indicator).alias("Indicator"),
                    pl.col("__indicator").alias("IndicatorValue"),
                ]
            )
        )
