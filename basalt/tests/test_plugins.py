from __future__ import annotations

import basalt.plugins as plugins


class _EP:
    def __init__(self, name, payload):
        self.name = name
        self._payload = payload

    def load(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def test_discover_plugins_and_analytics_packages(monkeypatch):
    def _entry_points(group: str):
        assert group == "basalt.plugins"
        return [
            _EP(
                "a",
                lambda: {
                    "name": "plugin-a",
                    "analytics_packages": ["basalt.preprocessors"],
                    "provides": ["preprocessors"],
                },
            ),
            _EP("broken", RuntimeError("boom")),
        ]

    monkeypatch.setattr(plugins, "_entry_points", _entry_points)
    found = plugins.discover_plugins()
    assert any(p.name == "plugin-a" and not p.error for p in found)
    assert any(p.name == "broken" and p.error for p in found)

    pkgs = plugins.get_plugin_analytics_packages()
    assert pkgs == ["basalt.preprocessors"]


def test_list_plugins_payload(monkeypatch):
    monkeypatch.setattr(
        plugins,
        "discover_plugins",
        lambda: [
            plugins.PluginInfo(
                name="x",
                source="entrypoint:x",
                provides=("a",),
                analytics_packages=("basalt.analytics.characteristics",),
                cli_extensions=("dagster",),
            )
        ],
    )
    rows = plugins.list_plugins_payload()
    assert rows[0]["name"] == "x"
    assert rows[0]["status"] == "ok"
    assert rows[0]["cli_extensions"] == ["dagster"]
