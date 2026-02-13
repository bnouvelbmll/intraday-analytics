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
    mcp_cli = ext["mcp"]
    assert hasattr(mcp_cli, "serve")
    for name in (
        "capabilities",
        "configure",
        "run",
        "recent_runs",
        "success_rate",
        "materialized_partitions",
        "optimize_run",
        "optimize_summary",
    ):
        assert not hasattr(mcp_cli, name)


def test_configure_job_updates_bmll_jobs_defaults(tmp_path):
    pipeline = tmp_path / "demo_pipeline.py"
    pipeline.write_text("USER_CONFIG = {}\n", encoding="utf-8")

    out = service.configure_job(
        pipeline=str(pipeline),
        executor="bmll",
        instance_size=64,
        conda_env="py311-latest",
        max_runtime_hours=5,
    )
    assert out["executor"] == "bmll"

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


def test_run_job_ec2_dispatch(monkeypatch):
    called = {}

    def _fake_ec2_run(**kwargs):
        called.update(kwargs)
        return {"instance_id": "i-123"}

    import basalt.executors.aws_ec2 as ec2_mod

    monkeypatch.setattr(ec2_mod, "ec2_run", _fake_ec2_run)
    out = service.run_job(
        pipeline="demo/01_ohlcv_bars.py",
        executor="ec2",
        name="test-ec2",
    )
    assert out["executor"] == "ec2"
    assert called["pipeline"] == "demo/01_ohlcv_bars.py"
    assert called["name"] == "test-ec2"


def test_capabilities_includes_bmll():
    caps = service.list_capabilities()["capabilities"]
    assert "bmll" in caps


def test_recent_runs_bmll_and_success_rate(monkeypatch):
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

    payload = service.recent_runs(executor="bmll", limit=10)
    assert payload["executor"] == "bmll"
    assert len(payload["runs"]) == 3

    stats = service.success_rate(executor="bmll", limit=10)
    assert stats["lookback"] == 3
    assert stats["successes"] == 2
    assert stats["failures"] == 1


def test_optimize_run_and_summary(monkeypatch, tmp_path):
    import basalt.optimize.core as core

    def _fake_optimize_pipeline(**kwargs):
        out_dir = tmp_path / "opt"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "summary.json").write_text('{"best":{"score":1.23}}', encoding="utf-8")
        (out_dir / "trials.jsonl").write_text('{"trial_id":1}\n', encoding="utf-8")
        return {"output_dir": str(out_dir), "best": {"score": 1.23}}

    monkeypatch.setattr(core, "optimize_pipeline", _fake_optimize_pipeline)
    monkeypatch.setattr(
        "basalt.optimize.cli_ext._load_search_space",
        lambda **kwargs: {"params": {}},
    )
    out = service.optimize_run(
        pipeline="demo/01_ohlcv_bars.py",
        search_space={"params": {}},
        trials=1,
    )
    assert "optimize" in out
    summary = service.optimize_summary(output_dir=str(tmp_path / "opt"))
    assert summary["exists"] is True
    assert summary["trial_rows"] == 1
