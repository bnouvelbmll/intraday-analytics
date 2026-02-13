from __future__ import annotations

from types import SimpleNamespace

import basalt.mcp.server as server


class _FakeMCP:
    def __init__(self, _name):
        self.tools = {}

    def tool(self):
        def _decorator(fn):
            self.tools[fn.__name__] = fn
            return fn

        return _decorator

    def run(self, **_kwargs):
        return {"ok": True}


def test_server_tool_calls_route_to_service(monkeypatch):
    monkeypatch.setitem(__import__("sys").modules, "fastmcp", SimpleNamespace(FastMCP=_FakeMCP))
    mcp = server.create_server()

    import basalt.mcp.service as service

    monkeypatch.setattr(service, "list_capabilities", lambda: {"capabilities": {"x": True}})
    monkeypatch.setattr(service, "list_plugins", lambda: {"plugins": [{"name": "p"}]})
    monkeypatch.setattr(
        service,
        "list_metrics",
        lambda **kwargs: {"metrics": [{"module": "l2", "pattern": "Spread"}], "count": 1},
    )
    monkeypatch.setattr(
        service,
        "inspect_metric_source",
        lambda **kwargs: {"metric": kwargs["metric"], "matches": []},
    )
    monkeypatch.setattr(
        service,
        "run_job",
        lambda **kwargs: {"executor": kwargs["executor"], "result": {"pipeline": kwargs["pipeline"]}},
    )
    monkeypatch.setattr(
        service,
        "optimize_summary",
        lambda **kwargs: {"exists": True, "output_dir": kwargs["output_dir"]},
    )

    assert mcp.tools["capabilities"]() == {"capabilities": {"x": True}}
    assert mcp.tools["list_plugins"]() == {"plugins": [{"name": "p"}]}
    assert mcp.tools["list_metrics"](module="l2")["count"] == 1
    assert mcp.tools["inspect_metric_source"](metric="Spread") == {
        "metric": "Spread",
        "matches": [],
    }
    assert mcp.tools["run_job"](pipeline="demo/01_ohlcv_bars.py", executor="direct") == {
        "executor": "direct",
        "result": {"pipeline": "demo/01_ohlcv_bars.py"},
    }
    assert mcp.tools["optimize_summary"](output_dir="/tmp/x") == {
        "exists": True,
        "output_dir": "/tmp/x",
    }


def test_server_optimize_run_routes_arguments(monkeypatch):
    monkeypatch.setitem(__import__("sys").modules, "fastmcp", SimpleNamespace(FastMCP=_FakeMCP))
    mcp = server.create_server()

    calls = {}
    import basalt.mcp.service as service

    def _fake_optimize_run(**kwargs):
        calls.update(kwargs)
        return {"optimize": {"ok": True}}

    monkeypatch.setattr(service, "optimize_run", _fake_optimize_run)
    out = mcp.tools["optimize_run"](
        pipeline="demo/02_multi_pass.py",
        trials=3,
        executor="direct",
        model_factory="basalt.optimize.presets:simple_linear_model_factory",
        dataset_builder="basalt.optimize.presets:dataset_builder_from_last_pass_output",
        objectives="mae,directional_accuracy",
        objective="directional_accuracy",
        search_generator="basalt.optimize.presets:history_guided_generator",
        output_dir="opt_out",
    )
    assert out == {"optimize": {"ok": True}}
    assert calls["pipeline"] == "demo/02_multi_pass.py"
    assert calls["trials"] == 3
    assert calls["executor"] == "direct"
    assert calls["objective"] == "directional_accuracy"
