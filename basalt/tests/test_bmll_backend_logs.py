from __future__ import annotations

from pathlib import Path
import types
import sys

from basalt.configuration import BMLLJobConfig
from basalt.executors.bmll.backend import submit_instance_job


class _FakeJob:
    def __init__(self):
        self.id = "job-123"
        self.updated = None

    def update(self, **kwargs):
        self.updated = kwargs

    def execute(self):
        return {"ok": True}

    def delete(self):
        return None


def test_submit_instance_job_sets_basalt_log_path(monkeypatch, tmp_path):
    script = tmp_path / "run.py"
    script.write_text("print('ok')\n", encoding="utf-8")

    fake_job = _FakeJob()

    fake_compute = types.ModuleType("bmll.compute")
    fake_compute.Bootstrap = lambda area, path, args=None: {
        "area": area,
        "path": path,
        "args": args or [],
    }
    fake_compute.CronTrigger = lambda expr: {"expr": expr}
    fake_compute.JobTrigger = lambda name, trigger: {"name": name, "trigger": trigger}
    fake_compute.create_job = lambda **_kwargs: fake_job

    fake_bmll = types.ModuleType("bmll")
    fake_bmll.compute = fake_compute

    monkeypatch.setitem(sys.modules, "bmll", fake_bmll)
    monkeypatch.setitem(sys.modules, "bmll.compute", fake_compute)

    cfg = BMLLJobConfig(
        jobs_dir=str(tmp_path / "_bmll_jobs"),
        project_root=str(tmp_path),
        log_path="job_run_logs",
        delete_job_after=False,
    )

    submit_instance_job(
        script_path=str(script),
        name="t1",
        config=cfg,
    )
    assert fake_job.updated is not None
    assert fake_job.updated["log_path"] == "basalt/_bmll_jobs/job-123/logs"
    assert fake_job.updated["log_area"] == "user"
