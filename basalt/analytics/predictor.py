from __future__ import annotations

from typing import List, Optional, Literal

import pandas as pd
import polars as pl
from pydantic import BaseModel, ConfigDict, Field

from basalt.analytics_base import BaseAnalytics, analytic_doc
from basalt.analytics_registry import register_analytics


class PredictorAnalyticsConfig(BaseModel):
    """
    Identify leading predictors of a target signal using lagged correlations.

    For each predictor/target pair, the module evaluates correlations across
    configured lags and returns the best (highest absolute correlation).
    """

    model_config = ConfigDict(
        json_schema_extra={
            "ui": {
                "module": "predictor",
                "tier": "post",
                "desc": "Postprocessing: lagged correlations to find leading predictors.",
                "outputs": [
                    "Target",
                    "Predictor",
                    "BestLag",
                    "BestCorr",
                    "Confidence",
                    "AnticipationSteps",
                    "AnticipationSeconds",
                ],
                "schema_keys": ["predictor"],
            }
        }
    )

    ENABLED: bool = Field(
        False,
        description="Enable or disable predictor analytics for this pass.",
        json_schema_extra={
            "long_doc": "Toggle to enable or disable predictor analytics.\n"
            "When disabled, the module is skipped entirely.\n",
        },
    )
    source_pass: str = Field(
        "pass1",
        description="Pass name to use as input.",
        json_schema_extra={
            "long_doc": "Selects which pass output is analyzed.\n"
            "Use a pass containing the target and predictor series.\n",
        },
    )
    target_column: Optional[str] = Field(
        None,
        description="Target metric to predict (future values).",
        json_schema_extra={
            "long_doc": "The series to predict. Predictor correlations are computed\n"
            "against this target at future lags.\n",
        },
    )
    predictor_columns: Optional[List[str]] = Field(
        None,
        description="Predictor columns (defaults to numeric columns except target).",
        json_schema_extra={
            "long_doc": "Explicit list of predictors. If omitted, all numeric columns\n"
            "excluding group keys and the target column are used.\n",
        },
    )
    group_by: List[str] = Field(
        default_factory=lambda: ["ListingId"],
        description="Grouping columns for predictor search.",
        json_schema_extra={
            "long_doc": "Groups define independent predictor searches.\n"
            "Typical grouping is per ListingId, optionally with TimeBucket.\n",
        },
    )
    lags: List[int] = Field(
        default_factory=lambda: [1, 2, 3, 5, 10],
        description="Positive lags (in rows) to test for predictor lead.",
        json_schema_extra={
            "long_doc": "Each lag tests whether predictor at time t explains target at t+lag.\n"
            "Larger lags imply longer anticipation windows.\n",
        },
    )
    method: Literal["pearson", "spearman"] = Field(
        "pearson",
        description="Correlation method for lag testing.",
        json_schema_extra={
            "long_doc": "Pearson measures linear correlation; Spearman measures rank correlation.\n",
        },
    )
    min_periods: int = Field(
        30,
        description="Minimum paired observations required per lag.",
        json_schema_extra={
            "long_doc": "If fewer observations are available at a lag, the lag is skipped.\n",
        },
    )
    time_col: Optional[str] = Field(
        "TimeBucket",
        description="Optional time column for anticipation seconds.",
        json_schema_extra={
            "long_doc": "If provided and parseable, the module estimates a median time delta\n"
            "and returns anticipation in seconds for the best lag.\n",
        },
    )
    top_k: int = Field(
        5,
        description="Return the top-K predictors per group (by absolute correlation).",
        json_schema_extra={
            "long_doc": "Limits output to the strongest predictors per group.\n"
            "Set to a larger value to keep more candidates.\n"
            "If <= 0, all predictors are returned.\n",
        },
    )
    include_t_value: bool = Field(
        False,
        description="Include t-statistic for the best correlation.",
        json_schema_extra={
            "long_doc": "If enabled, computes the t-statistic for the best correlation.\n"
            "Uses t = r * sqrt((n-2)/(1-r^2)) with df=n-2.\n",
        },
    )
    include_p_value: bool = Field(
        False,
        description="Include two-sided p-value for the best correlation.",
        json_schema_extra={
            "long_doc": "If enabled, computes a two-sided p-value for the best correlation.\n"
            "Requires SciPy (scipy.stats.t). If SciPy is not available, an error is raised.\n",
        },
    )


@analytic_doc(
    module="predictor",
    pattern=r"^Target$",
    template="Target metric whose future values are being predicted.",
    unit="Label",
    description="Target series used for lagged correlation analysis.",
)
@analytic_doc(
    module="predictor",
    pattern=r"^Predictor$",
    template="Predictor metric with best leading correlation to Target.",
    unit="Label",
    description="Predictor series selected by maximum absolute lagged correlation.",
)
@analytic_doc(
    module="predictor",
    pattern=r"^BestLag$",
    template="Lag (in rows) that maximizes absolute correlation.",
    unit="Count",
    description="Positive lag indicates predictor leads the target by that many rows.",
)
@analytic_doc(
    module="predictor",
    pattern=r"^BestCorr$",
    template="Correlation at the best lag between Predictor and Target.",
    unit="Correlation",
    description="Correlation coefficient at the optimal lag.",
)
@analytic_doc(
    module="predictor",
    pattern=r"^Confidence$",
    template="Strength of the predictor relationship.",
    unit="Index",
    description=(
        "Confidence is defined as absolute correlation scaled by sqrt(n).\n"
        "It is a heuristic score, not a statistical p-value."
    ),
)
@analytic_doc(
    module="predictor",
    pattern=r"^AnticipationSteps$",
    template="Lead time in rows implied by the best lag.",
    unit="Count",
    description="Number of rows the predictor leads the target.",
)
@analytic_doc(
    module="predictor",
    pattern=r"^AnticipationSeconds$",
    template="Lead time in seconds implied by the best lag.",
    unit="Seconds",
    description="Estimated from median time delta when time_col is available.",
)
@analytic_doc(
    module="predictor",
    pattern=r"^TStatistic$",
    template="T-statistic for the best lagged correlation.",
    unit="Index",
    description="Computed as r * sqrt((n-2)/(1-r^2)) with df=n-2.",
)
@analytic_doc(
    module="predictor",
    pattern=r"^PValue$",
    template="Two-sided p-value for the best lagged correlation.",
    unit="Probability",
    description="Two-sided p-value from the t-distribution with df=n-2.",
)
def _register_predictor_docs():
    return None


@register_analytics("predictor", config_attr="predictor_analytics")
class PredictorAnalytics(BaseAnalytics):
    REQUIRES: List[str] = []

    def __init__(self, config: PredictorAnalyticsConfig):
        super().__init__("predictor", {}, join_keys=config.group_by)
        self.config = config

    def compute(self) -> pl.LazyFrame:
        if not self.config.ENABLED:
            return pl.DataFrame().lazy()

        if not self.config.target_column:
            return pl.DataFrame().lazy()
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
        group_cols = list(self.config.group_by or [])
        for col in group_cols:
            if col not in pdf.columns:
                raise ValueError(f"Group column {col} not found.")

        if self.config.target_column not in pdf.columns:
            raise ValueError(
                f"Target column {self.config.target_column} not found."
            )

        if self.config.predictor_columns:
            predictors = [
                c for c in self.config.predictor_columns if c in pdf.columns
            ]
        else:
            predictors = (
                pdf.select_dtypes(include=["number"])
                .columns.difference(group_cols + [self.config.target_column])
                .tolist()
            )

        if not predictors:
            return pl.DataFrame().lazy()

        lags = sorted({lag for lag in self.config.lags if lag > 0})
        if not lags:
            return pl.DataFrame().lazy()

        rows = []
        if group_cols:
            groups = pdf.groupby(group_cols, dropna=False)
        else:
            groups = [((), pdf)]

        for key, frame in groups:
            if group_cols and not isinstance(key, tuple):
                key = (key,)

            frame = frame.sort_values(
                self.config.time_col
            ) if self.config.time_col in frame.columns else frame

            target = frame[self.config.target_column]
            time_seconds = None
            if self.config.time_col and self.config.time_col in frame.columns:
                time_values = pd.to_datetime(frame[self.config.time_col], errors="coerce")
                deltas = time_values.diff().dropna().dt.total_seconds()
                if not deltas.empty and deltas.median() > 0:
                    time_seconds = float(deltas.median())

            candidates = []
            for predictor in predictors:
                best = None
                best_lag = None
                best_n = None
                for lag in lags:
                    shifted = frame[predictor].shift(0)
                    aligned_target = target.shift(-lag)
                    valid = shifted.notna() & aligned_target.notna()
                    n = int(valid.sum())
                    if n < self.config.min_periods:
                        continue
                    corr = shifted[valid].corr(
                        aligned_target[valid], method=self.config.method
                    )
                    if corr is None or pd.isna(corr):
                        continue
                    score = abs(corr)
                    if best is None or score > best:
                        best = corr
                        best_lag = lag
                        best_n = n

                if best is None:
                    continue

                candidates.append((predictor, best, best_lag, best_n))

            if not candidates:
                continue

            candidates.sort(key=lambda x: abs(x[1]), reverse=True)
            if self.config.top_k > 0:
                candidates = candidates[: int(self.config.top_k)]

            for predictor, best, best_lag, best_n in candidates:
                confidence = abs(best) * (best_n ** 0.5) if best_n else None
                anticipation_seconds = (
                    best_lag * time_seconds if time_seconds and best_lag else None
                )
                t_stat = None
                p_value = None
                if self.config.include_t_value or self.config.include_p_value:
                    if best_n and best_n > 2 and abs(best) < 1:
                        t_stat = float(
                            best
                            * ((best_n - 2) / (1 - best**2)) ** 0.5
                        )
                    if self.config.include_p_value:
                        if t_stat is None:
                            p_value = None
                        else:
                            try:
                                from scipy import stats  # type: ignore
                            except Exception as exc:
                                raise ValueError(
                                    "SciPy is required to compute p-values."
                                ) from exc
                            p_value = float(
                                2 * stats.t.sf(abs(t_stat), df=best_n - 2)
                            )

                row = {
                    "Target": self.config.target_column,
                    "Predictor": predictor,
                    "BestLag": best_lag,
                    "BestCorr": float(best),
                    "Confidence": float(confidence) if confidence is not None else None,
                    "AnticipationSteps": best_lag,
                    "AnticipationSeconds": anticipation_seconds,
                    "TStatistic": t_stat if self.config.include_t_value else None,
                    "PValue": p_value if self.config.include_p_value else None,
                }
                if group_cols:
                    row.update(dict(zip(group_cols, key)))
                rows.append(row)

        return pl.from_pandas(pd.DataFrame(rows)).lazy()
