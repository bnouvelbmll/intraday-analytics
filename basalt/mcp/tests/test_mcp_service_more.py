from __future__ import annotations

from types import ModuleType, SimpleNamespace
import sys

import pytest

import basalt.mcp.service as service


def test_to_plain_model_dump_and_object():
    class _M:
        def model_dump(self):
            return {"a": 1}

    class _O:
        id = "1"
        status = "ok"

    assert service._to_plain(_M()) == {"a": 1}
    assert service._to_plain(_O())["status"] == "ok"


def test_list_capabilities(monkeypatch):
    monkeypatch.setattr(service.importlib.util, "find_spec", lambda name: object() if name == "fastmcp" else None)
    out = service.list_capabilities()
    assert out["capabilities"]["pipeline"] is True
    assert out["capabilities"]["fastmcp"] is True


def test_list_plugins(monkeypatch):
    monkeypatch.setattr(
        "basalt.plugins.list_plugins_payload",
        lambda: [{"name": "x", "status": "ok"}],
    )
    out = service.list_plugins()
    assert out["plugins"][0]["name"] == "x"


def test_list_metrics_and_source():
    out = service.list_metrics(module="external_events", limit=5)
    assert out["count"] >= 1
    src = service.inspect_metric_source(
        metric="EventAnchorTime",
        module="external_events",
        context_lines=3,
    )
    assert src["metric"] == "EventAnchorTime"
    assert isinstance(src["matches"], list)


def test_run_job_unsupported_executor():
    with pytest.raises(ValueError, match="Unsupported executor"):
        service.run_job(pipeline="x.py", executor="bad")


def test_recent_runs_pipeline_and_error():
    out = service.recent_runs(executor="pipeline")
    assert out["executor"] == "pipeline"
    with pytest.raises(ValueError, match="Unsupported executor"):
        service.recent_runs(executor="bad")


def test_recent_runs_dagster_branch(monkeypatch):
    class _Run:
        def __init__(self, status):
            self.run_id = "r"
            self.job_name = "j"
            self.status = status
            self.start_time = 1
            self.end_time = 2

    fake_dagster = ModuleType("dagster")
    fake_dagster.DagsterInstance = SimpleNamespace(get=lambda: SimpleNamespace(get_runs=lambda limit: [_Run("SUCCESS"), _Run("FAILED")]))
    monkeypatch.setitem(sys.modules, "dagster", fake_dagster)
    out = service.recent_runs(executor="dagster", limit=2)
    assert out["executor"] == "dagster"
    assert len(out["runs"]) == 2


def test_materialized_partitions(monkeypatch):
    class _Mat:
        def __init__(self, partition):
            self.partition = partition

    class _Step:
        def __init__(self, partition):
            self.materialization = _Mat(partition)

    class _DE:
        def __init__(self, p):
            self.step_materialization_data = _Step(p)

    class _Entry:
        def __init__(self, p, ts):
            self.dagster_event = _DE(p)
            self.timestamp = ts

    class _Rec:
        def __init__(self, p, ts):
            self.event_log_entry = _Entry(p, ts)

    fake_dagster = ModuleType("dagster")
    fake_dagster.DagsterEventType = SimpleNamespace(ASSET_MATERIALIZATION="x")
    fake_dagster.AssetKey = lambda parts: parts
    fake_dagster.EventRecordsFilter = lambda *a, **k: None
    fake_dagster.DagsterInstance = SimpleNamespace(
        get=lambda: SimpleNamespace(get_event_records=lambda *a, **k: [_Rec("P1", 2), _Rec("P1", 1), _Rec(None, 3)])
    )
    monkeypatch.setitem(sys.modules, "dagster", fake_dagster)
    out = service.materialized_partitions(asset_key="A/B")
    assert out["asset_key"] == "A/B"
    assert out["partitions"][0]["count"] >= 1
