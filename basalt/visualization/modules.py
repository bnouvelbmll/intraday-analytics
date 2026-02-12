from __future__ import annotations

from typing import Iterable

from basalt.visualization.plots import PlotModule, default_plot_modules


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


def discover_plot_modules() -> list[PlotModule]:
    modules: list[PlotModule] = list(default_plot_modules())
    for ep in _entry_points("basalt.visualization.modules"):
        try:
            loaded = ep.load()
            value = loaded() if callable(loaded) else loaded
        except Exception:
            continue
        if isinstance(value, PlotModule):
            modules.append(value)
            continue
        if isinstance(value, Iterable):
            for item in value:
                if isinstance(item, PlotModule):
                    modules.append(item)
    return modules

