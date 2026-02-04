from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type
from abc import ABC, abstractmethod

import polars as pl
from pydantic import BaseModel

from intraday_analytics.analytics.common import _register_metric_doc, _register_metric_hint


@dataclass(frozen=True)
class AnalyticDoc:
    pattern: str
    template: str
    module: str | None = None
    unit: str | None = None
    group: str | None = None
    group_role: str | None = None
    group_semantics: str | None = None

    def to_dict(self) -> dict:
        return {
            "module": self.module,
            "pattern": self.pattern,
            "template": self.template,
            "unit": self.unit,
            "group": self.group,
            "group_role": self.group_role,
            "group_semantics": self.group_semantics,
        }


@dataclass(frozen=True)
class AnalyticHint:
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
    base_df: pl.LazyFrame
    cache: Dict[str, Any]
    context: Dict[str, Any]


def analytic_handler(
    name: str,
    *,
    pattern: str | None = None,
    unit: str | None = None,
    group: str | None = None,
    group_role: str | None = None,
    group_semantics: str | None = None,
):
    def _decorator(fn):
        setattr(fn, "_analytic_handler_name", name)
        if pattern:
            doc = (fn.__doc__ or "").strip()
            if doc:
                setattr(
                    fn,
                    "_analytic_doc",
                    AnalyticDoc(
                        pattern=pattern,
                        template=doc,
                        unit=unit,
                        group=group,
                        group_role=group_role,
                        group_semantics=group_semantics,
                    ),
                )
        return fn

    return _decorator


class AnalyticSpec(ABC):
    MODULE: str = ""
    DOCS: List[AnalyticDoc] = []
    HINTS: List[AnalyticHint] = []
    ConfigModel: Type[BaseModel] = BaseModel
    REGISTER: bool = True
    HANDLERS: Dict[str, Any] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        handlers: Dict[str, Any] = {}
        for value in cls.__dict__.values():
            name = getattr(value, "_analytic_handler_name", None)
            if name:
                handlers[name] = value
        if handlers:
            cls.HANDLERS = handlers
        if not getattr(cls, "REGISTER", True):
            return
        for doc in getattr(cls, "DOCS", []):
            module = doc.module or cls.MODULE
            _register_metric_doc({**doc.to_dict(), "module": module})
        for handler in handlers.values():
            doc = getattr(handler, "_analytic_doc", None)
            if doc is None:
                continue
            module = doc.module or cls.MODULE
            _register_metric_doc({**doc.to_dict(), "module": module})
        for hint in getattr(cls, "HINTS", []):
            module = hint.module or cls.MODULE
            _register_metric_hint({**hint.to_dict(), "module": module})

    def expand_config(self, config: BaseModel) -> List[Dict[str, Any]]:
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
        raise NotImplementedError

    def default_expressions(self, ctx: AnalyticContext) -> List[pl.Expr]:
        return []


# Backwards-compat aliases
MetricDoc = AnalyticDoc
MetricHint = AnalyticHint
MetricContext = AnalyticContext
MetricSpec = AnalyticSpec
