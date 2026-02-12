from __future__ import annotations

from pathlib import Path
from types import ModuleType, SimpleNamespace
import sys

import yaml

from basalt.mcp import service
from basalt.mcp.cli_ext import get_cli_extension


def test_mcp_cli_extension_registered():
    ext = get_cli_extension()
    assert "mcp" in ext


def test_configure_job_updates_bmll_jobs_defaults(tmp_path):
    pipeline = tmp_path / "demo_pipeline.py"
    pipeline.write_text("USER_CONFIG = {}\n", encoding="utf-8")

    out = service.configure_job(
        pipeline=str(pipeline),
        executor="ec2",
        instance_size=64,
        conda_env="py311-latest",
        max_runtime_hours=5,
    )
    assert out["executor"] == "ec2"

    yaml_path = pipeline.with_suffix(".yaml")
    data = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    user = data.get("USER_CONFIG", {})
    assert user["BMLL_JOBS"]["enabled"] is True
    assert user["BMLL_JOBS"]["default_instance_size"] == 64
    assert user["BMLL_JOBS"]["default_conda_env"] == "py311-latest"
    assert user["BMLL_JOBS"]["max_runtime_hours"] == 5


def test_run_job_pipeline_dispatch(monkeypatch):
    called = {}

    def _fake_pipeline_run(**kwargs):
        called.update(kwargs)
        return {"ok": True}

    import basalt.basalt as basalt_main

    monkeypatch.setattr(basalt_main, "_pipeline_run", _fake_pipeline_run)
    out = service.run_job(pipeline="demo/01_ohlcv_bars.py", executor="pipeline")
    assert out["executor"] == "pipeline"
    assert called["pipeline"] == "demo/01_ohlcv_bars.py"
    assert out["result"]["ok"] is True


def test_recent_runs_ec2_and_success_rate(monkeypatch):
    class _Run:
        def __init__(self, run_id, name, state):
            self.id = run_id
            self.name = name
            self.state = state

    runs = [_Run("1", "a", "SUCCESS"), _Run("2", "b", "FAILED"), _Run("3", "c", "SUCCESS")]
    compute = SimpleNamespace(get_job_runs=lambda state=None: runs)

    fake_bmll = ModuleType("bmll")
    fake_bmll.compute = compute
    monkeypatch.setitem(sys.modules, "bmll", fake_bmll)

    payload = service.recent_runs(executor="ec2", limit=10)
    assert payload["executor"] == "ec2"
    assert len(payload["runs"]) == 3

    stats = service.success_rate(executor="ec2", limit=10)
    assert stats["lookback"] == 3
    assert stats["successes"] == 2
    assert stats["failures"] == 1
