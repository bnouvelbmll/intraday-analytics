from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Type
import importlib
import pkgutil

from basalt.analytics_base import BaseAnalytics
from basalt.plugins import get_plugin_analytics_packages, get_plugin_module_config_models


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

    import basalt.analytics as analytics_pkg

    for mod in pkgutil.iter_modules(
        analytics_pkg.__path__, analytics_pkg.__name__ + "."
    ):
        importlib.import_module(mod.name)

    for pkg_name in get_plugin_analytics_packages():
        try:
            pkg = importlib.import_module(pkg_name)
        except Exception:
            continue
        for mod in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + "."):
            importlib.import_module(mod.name)

    importlib.import_module("basalt.time.dense")
    importlib.import_module("basalt.time.external_events")
    _DISCOVERED = True


def build_module_registry(pass_config, ref) -> Dict[str, Callable[[], BaseAnalytics]]:
    discover_analytics()
    registry: Dict[str, Callable[[], BaseAnalytics]] = {}
    extension_models = get_plugin_module_config_models()

    for name, entry in _REGISTRY.items():
        if entry.config_attr:
            cfg = getattr(pass_config, entry.config_attr, None)
            if cfg is None and hasattr(pass_config, "extension_configs"):
                ext = pass_config.extension_configs or {}
                raw_cfg = ext.get(entry.config_attr, ext.get(name))
                if raw_cfg is not None:
                    model = extension_models.get(entry.config_attr) or extension_models.get(name)
                    if model is not None and hasattr(model, "model_validate"):
                        try:
                            cfg = model.model_validate(raw_cfg)
                        except Exception:
                            cfg = raw_cfg
                    else:
                        cfg = raw_cfg
        else:
            cfg = None

        if entry.needs_ref:
            registry[name] = lambda entry=entry, cfg=cfg: entry.cls(ref, cfg)
        else:
            registry[name] = lambda entry=entry, cfg=cfg: (
                entry.cls(cfg) if cfg is not None else entry.cls()
            )

    return registry


def resolve_batch_group_by(module_names: list[str]) -> str | None:
    discover_analytics()
    group_keys = set()
    for name in module_names:
        entry = _REGISTRY.get(name)
        if not entry:
            continue
        group_key = getattr(entry.cls, "BATCH_GROUP_BY", None)
        if group_key:
            group_keys.add(group_key)
    if not group_keys:
        return None
    if len(group_keys) > 1:
        raise ValueError(
            f"Multiple batch grouping keys requested: {sorted(group_keys)}"
        )
    return next(iter(group_keys))


def get_registered_entries() -> Dict[str, AnalyticsEntry]:
    discover_analytics()
    return dict(_REGISTRY)
