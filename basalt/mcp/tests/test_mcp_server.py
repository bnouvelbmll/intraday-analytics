from __future__ import annotations

from types import SimpleNamespace

import basalt.mcp.server as server


class _FakeMCP:
    def __init__(self, _name):
        self.tools = {}
        self.calls = []

    def tool(self):
        def _decorator(fn):
            self.tools[fn.__name__] = fn
            return fn

        return _decorator

    def run(self, **kwargs):
        self.calls.append(kwargs)
        return {"ran": kwargs}


def test_create_server_registers_tools(monkeypatch):
    monkeypatch.setitem(__import__("sys").modules, "fastmcp", SimpleNamespace(FastMCP=_FakeMCP))
    mcp = server.create_server()
    expected = {
        "capabilities",
        "list_plugins",
        "list_metrics",
        "inspect_metric_source",
        "configure_job",
        "run_job",
        "recent_runs",
        "success_rate",
        "materialized_partitions",
        "optimize_run",
        "optimize_summary",
    }
    assert expected.issubset(set(mcp.tools.keys()))


def test_run_server_typeerror_fallback(monkeypatch):
    class _TypeErrMCP(_FakeMCP):
        def run(self, **kwargs):
            if kwargs:
                raise TypeError("old api")
            return {"ran": "fallback"}

    monkeypatch.setitem(__import__("sys").modules, "fastmcp", SimpleNamespace(FastMCP=_TypeErrMCP))
    out = server.run_server(transport="stdio", port=9000)
    assert out == {"ran": "fallback"}
