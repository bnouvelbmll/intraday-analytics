from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Type
import importlib
import pkgutil

from intraday_analytics.bases import BaseAnalytics


@dataclass(frozen=True)
class AnalyticsEntry:
    name: str
    cls: Type[BaseAnalytics]
    config_attr: str | None
    needs_ref: bool = False


_REGISTRY: Dict[str, AnalyticsEntry] = {}
_DISCOVERED = False


def register_analytics(name: str, config_attr: str | None, needs_ref: bool = False):
    def _decorator(cls: Type[BaseAnalytics]):
        _REGISTRY[name] = AnalyticsEntry(
            name=name, cls=cls, config_attr=config_attr, needs_ref=needs_ref
        )
        return cls

    return _decorator


def discover_analytics():
    global _DISCOVERED
    if _DISCOVERED:
        return

    import intraday_analytics.analytics as analytics_pkg

    for mod in pkgutil.iter_modules(
        analytics_pkg.__path__, analytics_pkg.__name__ + "."
    ):
        importlib.import_module(mod.name)

    importlib.import_module("intraday_analytics.dense_analytics")
    _DISCOVERED = True


def build_module_registry(pass_config, ref) -> Dict[str, Callable[[], BaseAnalytics]]:
    discover_analytics()
    registry: Dict[str, Callable[[], BaseAnalytics]] = {}

    for name, entry in _REGISTRY.items():
        if entry.config_attr:
            cfg = getattr(pass_config, entry.config_attr)
        else:
            cfg = None

        if entry.needs_ref:
            registry[name] = lambda entry=entry, cfg=cfg: entry.cls(ref, cfg)
        else:
            registry[name] = (
                lambda entry=entry, cfg=cfg: entry.cls(cfg)
                if cfg is not None
                else entry.cls()
            )

    return registry
