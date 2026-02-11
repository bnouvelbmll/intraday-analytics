from __future__ import annotations

"""
Core analytics abstractions used by modules and specs.

This file is the single source of truth for how analytics are defined, expanded,
and computed in the framework. It keeps the responsibilities separated:

- Specs describe *what* an analytic is and how to build expressions from configs.
- Module base classes describe *how* data is grouped and aggregated.
- Helpers provide a consistent loop for expanding configs into expressions.

If you are adding a new analytic, start by creating an `AnalyticSpec` subclass
and then wire it into a module that derives from `BaseAnalytics` or
`BaseTWAnalytics`. The intent is to make the path from config -> expressions ->
grouped output explicit and uniform across modules.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type
from abc import ABC, abstractmethod
import inspect

import polars as pl
from pydantic import BaseModel

from basalt.analytics.common import (
    _register_metric_doc,
    _register_metric_hint,
)
from basalt.utils import dc, ffill_with_shifts


@dataclass(frozen=True)
class AnalyticDoc:
    """
    Doc entry describing the output pattern and its human-readable template.

    Each spec can contribute documentation metadata using either the `DOCS`
    list or the `@analytic_expression` decorator. The `pattern` describes which
    output names the doc applies to (regex string), and the `template` should
    be a short sentence that can be formatted with any captured groups.
    """

    pattern: str
    template: str
    module: str | None = None
    unit: str | None = None
    group: str | None = None
    group_role: str | None = None
    group_semantics: str | None = None
    description: str | None = None

    def to_dict(self) -> dict:
        return {
            "module": self.module,
            "pattern": self.pattern,
            "template": self.template,
            "unit": self.unit,
            "group": self.group,
            "group_role": self.group_role,
            "group_semantics": self.group_semantics,
            "description": self.description,
        }


@dataclass(frozen=True)
class AnalyticHint:
    """
    Hint entry used by aggregation helpers to infer defaults.

    Hints let downstream consumers (like schema utilities or generic aggregation
    helpers) map output patterns to default aggregations or weighting columns.
    They are optional but provide a consistent, discoverable way to attach
    aggregation semantics to a family of outputs.
    """

    pattern: str
    default_agg: str
    module: str | None = None
    weight_col: str | None = None

    def to_dict(self) -> dict:
        return {
            "module": self.module,
            "pattern": self.pattern,
            "default_agg": self.default_agg,
            "weight_col": self.weight_col,
        }


@dataclass
class AnalyticContext:
    """
    Shared context passed into analytic specs during expression build.

    - `base_df` is the grouped or filtered frame used by a module.
    - `cache` is a small dict for per-module shared values (e.g. bucket size).
    - `context` allows cross-analytic coordination within a pipeline pass.

    Specs should treat this as read-only; the module owns how it is built.
    """

    base_df: pl.LazyFrame
    cache: Dict[str, Any]
    context: Dict[str, Any]


def analytic_expression(
    name: str,
    *,
    pattern: str | None = None,
    unit: str | None = None,
    group: str | None = None,
    group_role: str | None = None,
    group_semantics: str | None = None,
):
    """
    Decorator for spec methods to register doc metadata by pattern.

    This is a convenience for pairing an expression method with the
    documentation of its outputs. If `pattern` is set and the method has a
    docstring, the docstring becomes the documentation template for that
    output family.
    """

    def _decorator(fn):
        setattr(fn, "_analytic_expression_name", name)
        if pattern:
            doc = inspect.cleandoc(fn.__doc__ or "")
            if doc:
                parts = doc.split("\n\n", 1)
                template = parts[0].strip().replace("\n", " ")
                description = parts[1].strip() if len(parts) > 1 else None
                setattr(
                    fn,
                    "_analytic_doc",
                    AnalyticDoc(
                        pattern=pattern,
                        template=template,
                        unit=unit,
                        group=group,
                        group_role=group_role,
                        group_semantics=group_semantics,
                        description=description,
                    ),
                )
        return fn

    return _decorator


class AnalyticSpec(ABC):
    """
    Spec that maps config variants to Polars expressions.

    A spec is intentionally small: it does not own grouping or data selection.
    Instead it translates a config (and its expanded variants) into one or more
    Polars expressions that the module can aggregate. The module decides the
    grouping columns and the final aggregation call.
    """

    MODULE: str = ""
    DOCS: List[AnalyticDoc] = []
    HINTS: List[AnalyticHint] = []
    ConfigModel: Type[BaseModel] = BaseModel
    REGISTER: bool = True
    EXPRESSIONS: Dict[str, Any] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        expressions: Dict[str, Any] = {}
        for value in cls.__dict__.values():
            name = getattr(value, "_analytic_expression_name", None)
            if name:
                expressions[name] = value
        if expressions:
            cls.EXPRESSIONS = expressions
        if not getattr(cls, "REGISTER", True):
            return
        for doc in getattr(cls, "DOCS", []):
            module = doc.module or cls.MODULE
            _register_metric_doc({**doc.to_dict(), "module": module})
        for expression_fn in expressions.values():
            doc = getattr(expression_fn, "_analytic_doc", None)
            if doc is None:
                continue
            module = doc.module or cls.MODULE
            _register_metric_doc({**doc.to_dict(), "module": module})
        for hint in getattr(cls, "HINTS", []):
            module = hint.module or cls.MODULE
            _register_metric_hint({**hint.to_dict(), "module": module})

    def expand_config(self, config: BaseModel) -> List[Dict[str, Any]]:
        """
        Expand a config into variant dictionaries for expression generation.

        Most config models expose an `expand()` method to turn a combinatorial
        config into many variants (e.g. sides x levels). If a config does not
        implement `expand()`, we use `model_dump()` as a single variant.
        """
        expand = getattr(config, "expand", None)
        if callable(expand):
            return expand()
        return [config.model_dump()]

    @abstractmethod
    def expressions(
        self,
        ctx: AnalyticContext,
        config: BaseModel,
        variant: Dict[str, Any],
    ) -> List[pl.Expr]:
        """
        Build expressions for a single config variant.

        The returned expressions should be ready to aggregate (e.g. `last()`,
        `mean()`, `sum()`), and should use consistent naming to match the
        documentation patterns.
        """
        raise NotImplementedError

    def default_expressions(self, ctx: AnalyticContext) -> List[pl.Expr]:
        """
        Fallback expressions when no explicit configs are provided.

        Some modules expose a default output set for convenience. By keeping it
        here, the module can still build a valid output even when no specific
        analytic configs are defined.
        """
        return []


class BaseAnalytics(ABC):
    """
    Base class for analytics modules built around simple group-by aggregation.

    Modules deriving from this class are responsible for:
    - selecting and preparing the input table(s),
    - building expressions via one or more `AnalyticSpec` objects,
    - grouping and aggregating to a final LazyFrame.
    """

    BATCH_GROUP_BY: str | None = None

    def __init__(
        self,
        name: str,
        specific_fill_cols=None,
        join_keys: List[str] = ["ListingId", "TimeBucket"],
        metric_prefix: str | None = None,
    ):
        self.name = name
        self.join_keys = join_keys
        self.df: Optional[pl.DataFrame] = None
        self.l2 = None
        self.l3 = None
        self.trades = None
        self.marketstate = None
        self.specific_fill_cols = specific_fill_cols or {}
        self.context: Dict[str, Any] = {}
        self.metric_prefix = metric_prefix or ""

    @abstractmethod
    def compute(self, **kwargs) -> pl.LazyFrame:
        """
        Computes the analytics for the module and returns a LazyFrame.

        Implementations should set `self.df` as a side effect so that the join
        step can access the computed output later in the pipeline.
        """
        raise NotImplementedError

    def join(
        self, base_df: pl.LazyFrame, other_specific_cols, default_ffill=False
    ) -> pl.LazyFrame:
        """
        Joins the computed analytics to a base DataFrame.

        This method handles the merging of the module's results with the main
        DataFrame being built by the pipeline. It also provides options for
        filling null values that may result from the join.
        """
        if self.df is None:
            raise ValueError(f"{self.name} has no computed data.")

        r = dc(
            base_df.join(
                self.df, on=self.join_keys, how="full", suffix=f"_{self.name}"
            ),
            f"_{self.name}",
        )

        ra = {}
        rccc = r.collect_schema().names()
        for c, a in {**self.specific_fill_cols, **(other_specific_cols or {})}.items():
            if c in rccc:
                if a == "zero":
                    ra[c] = pl.col(c).fill_null(0)
                elif a == "last":
                    ra[c] = pl.col(c).forward_fill().over("ListingId")
        if default_ffill:
            for c in rccc:
                if c not in ra and c not in self.join_keys:
                    ra[c] = pl.col(c).forward_fill().over("ListingId")

        if ra:
            r = r.with_columns(**ra)
        return r

    def apply_prefix(self, name: str) -> str:
        """Apply the module prefix to an output name if configured."""
        if self.metric_prefix:
            return f"{self.metric_prefix}{name}"
        return name

    def _ohlc_names(self, req: Any, variant: Dict[str, Any]) -> dict[str, str]:
        names = {}
        source = variant["source"]
        open_mode = variant.get("open_mode", "")
        open_mode_key = str(open_mode) if open_mode else ""
        open_mode_name = {
            "event": "",
            "prev_close": "C",
        }.get(
            open_mode_key,
            open_mode_key.replace("_", " ").title().replace(" ", ""),
        )
        for ohlc in ["Open", "High", "Low", "Close"]:
            variant_with_ohlc = {
                **variant,
                "ohlc": ohlc,
                "openMode": open_mode_name,
            }
            default_name = f"{source}{ohlc}"
            alias = (
                req.output_name_pattern.format(**variant_with_ohlc)
                if req.output_name_pattern
                else default_name
            )
            names[ohlc] = self.apply_prefix(alias)
        return names

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


class BaseTWAnalytics(BaseAnalytics):
    """
    A base class for time-weighted analytics modules.

    This class extends `BaseAnalytics` and provides a framework for computing
    time-weighted analytics. The `tw_analytics` method must be implemented
    by subclasses.
    """

    def __init__(
        self,
        name: str,
        specific_fill_cols=None,
        nanoseconds=None,
        metric_prefix: str | None = None,
    ):
        super().__init__(
            name,
            specific_fill_cols=specific_fill_cols,
            metric_prefix=metric_prefix,
        )
        self.nanoseconds = nanoseconds
        self._tables = {"l2": None, "l3": None, "trades": None}

    @abstractmethod
    def tw_analytics(self, **tables):
        """
        Performs time-weighted analytics calculations on resampled tables.
        """
        raise NotImplemented

    def compute(self) -> pl.LazyFrame:
        """
        Computes the time-weighted analytics for the module.

        This method resamples the input tables to a common time grid and then
        computes the time difference (DT) between consecutive events. This DT
        is used for time-weighting the analytics.

        The resampling strategy involves the following steps:
        1.  For each time bucket, we find the last event of the previous bucket,
            the events within the current bucket, and the first event of the
            next bucket.
        2.  We use `ffill_with_shifts` to create a dense time grid by forward-
            filling the data from the last known event. This ensures that each
            time bucket has a defined state at its boundaries.
        3.  We then calculate the `DT` column, which represents the time
            duration (in nanoseconds) for which a given state was valid. This
            is used as the weight in the time-weighted average calculation.
        """
        nanoseconds = self.nanoseconds
        gcol_list = ["MIC", "ListingId", "Ticker", "TimeBucket", "CurrencyCode"]
        rt = {}

        for tn in self._tables.keys():
            t = getattr(self, tn)
            if t is None:
                continue
            tcn = t.collect_schema().names()
            col_list = [c for c in tcn if c not in gcol_list]
            if "TimeBucket" not in tcn:
                raise ValueError("TimeBucket is required for analytics")

            t2_twr = ffill_with_shifts(
                t,
                gcol_list,
                "EventTimestamp",
                col_list,
                [(pl.duration(nanoseconds=nanoseconds * o)) for o in [-1, 0, 1]],
                lambda x: pl.when(x == x.dt.truncate(f"{nanoseconds}ns"))
                .then(x)
                .otherwise(
                    x.dt.truncate(f"{nanoseconds}ns")
                    + pl.duration(nanoseconds=nanoseconds)
                ),
            )

            t2_twr = t2_twr.with_columns(
                ((pl.col("ListingId").diff(-1)).cast(bool).cast(int)).alias("DP"),
            ).with_columns(
                (
                    (1 - pl.col("DP"))
                    * ((-pl.col("EventTimestamp").diff(-1)).clip(0, 10**12))
                ).alias("DT")
            )

            rt[tn] = t2_twr

        self.df = self.tw_analytics(**rt)
        return self.df


def build_expressions(
    ctx: AnalyticContext,
    spec_configs: List[tuple[AnalyticSpec, List[BaseModel]]],
) -> List[pl.Expr]:
    """
    Expand configs across specs into a flat list of Polars expressions.

    This helper encapsulates the common loop:
    spec -> config -> variants -> expressions. It keeps the module compute
    methods compact and ensures a uniform expansion pattern across analytics.
    """
    expressions: List[pl.Expr] = []
    for spec, configs in spec_configs:
        for config in configs:
            for variant in spec.expand_config(config):
                expressions.extend(spec.expressions(ctx, config, variant))
    return expressions


def apply_metric_prefix(ctx: AnalyticContext, name: str) -> str:
    """Apply a metric prefix stored in context cache to an output name."""
    prefix = ctx.cache.get("metric_prefix") if ctx and ctx.cache else ""
    if prefix:
        return f"{prefix}{name}"
    return name


def run_analytics_from_config(
    config: dict,
    context: dict,
    **kwargs,
) -> pl.LazyFrame:
    """
    Run analytics based on a configuration dictionary.
    """
    from basalt.analytics_registry import get_analytics_registry

    analytics_registry = get_analytics_registry()
    analytics_class = analytics_registry.get(config["name"])
    if analytics_class is None:
        raise ValueError(f"Unknown analytics module: {config['name']}")

    return run_analytics(analytics_class, config, context, **kwargs)


def run_analytics(
    analytics_class: Type[BaseAnalytics],
    config: dict,
    context: dict,
    **kwargs,
) -> pl.LazyFrame:
    """
    Run a single analytics module.
    """
    module_instance = analytics_class(config=config, **kwargs)
    module_instance.set_context(context)
    return module_instance.compute()
