from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PluginInfo:
    name: str
    source: str
    provides: tuple[str, ...] = ()
    analytics_packages: tuple[str, ...] = ()
    cli_extensions: tuple[str, ...] = ()
    module_configs: tuple[dict[str, Any], ...] = ()
    error: str | None = None


def _entry_points(group: str):
    try:
        from importlib.metadata import entry_points
    except Exception:
        try:
            from importlib_metadata import entry_points  # type: ignore[import-not-found]
        except Exception:
            return []
    try:
        return list(entry_points(group=group))
    except TypeError:
        return list(entry_points().get(group, []))


def _normalize_plugin(name: str, payload: Any, source: str) -> PluginInfo:
    if isinstance(payload, dict):
        module_configs = payload.get("module_configs") or []
        if not isinstance(module_configs, (list, tuple)):
            module_configs = []
        return PluginInfo(
            name=str(payload.get("name") or name),
            source=source,
            provides=tuple(str(x) for x in (payload.get("provides") or [])),
            analytics_packages=tuple(
                str(x) for x in (payload.get("analytics_packages") or [])
            ),
            cli_extensions=tuple(str(x) for x in (payload.get("cli_extensions") or [])),
            module_configs=tuple(
                x for x in module_configs if isinstance(x, dict)
            ),
        )
    if isinstance(payload, (list, tuple, set)):
        return PluginInfo(
            name=name,
            source=source,
            provides=tuple(str(x) for x in payload),
        )
    return PluginInfo(name=name, source=source, provides=(str(payload),))


def discover_plugins() -> list[PluginInfo]:
    discovered: list[PluginInfo] = []
    for ep in _entry_points("basalt.plugins"):
        try:
            obj = ep.load()
            payload = obj() if callable(obj) else obj
            discovered.append(
                _normalize_plugin(ep.name, payload, source=f"entrypoint:{ep.name}")
            )
        except Exception as exc:
            discovered.append(
                PluginInfo(
                    name=ep.name,
                    source=f"entrypoint:{ep.name}",
                    error=str(exc),
                )
            )
    return discovered


def get_plugin_analytics_packages() -> list[str]:
    packages: list[str] = []
    seen = set()
    for plugin in discover_plugins():
        for pkg in plugin.analytics_packages:
            if pkg not in seen:
                seen.add(pkg)
                packages.append(pkg)
    return packages


def list_plugins_payload() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for plugin in discover_plugins():
        rows.append(
            {
                "name": plugin.name,
                "source": plugin.source,
                "provides": list(plugin.provides),
                "analytics_packages": list(plugin.analytics_packages),
                "cli_extensions": list(plugin.cli_extensions),
                "module_configs": list(plugin.module_configs),
                "status": "error" if plugin.error else "ok",
                "error": plugin.error,
            }
        )
    return rows


def get_plugin_module_configs() -> list[dict[str, Any]]:
    configs: list[dict[str, Any]] = []
    for plugin in discover_plugins():
        for spec in plugin.module_configs:
            if isinstance(spec, dict):
                configs.append(dict(spec))
    return configs


def get_plugin_module_config_models() -> dict[str, Any]:
    models: dict[str, Any] = {}
    for spec in get_plugin_module_configs():
        key = spec.get("config_key")
        model = spec.get("model")
        module = spec.get("module")
        if key and model is not None:
            models[str(key)] = model
        if module and model is not None:
            models[str(module)] = model
    return models
