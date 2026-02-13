from __future__ import annotations

from types import ModuleType, SimpleNamespace
import sys

import pytest

from basalt.optimize.tracking import (
    MlflowTracker,
    WandbTracker,
    create_tracker,
)


def test_create_tracker_invalid():
    with pytest.raises(ValueError, match="tracker must be one of"):
        create_tracker(tracker="bad")


def test_mlflow_tracker_logs(monkeypatch):
    calls = {"params": [], "metrics": [], "tags": [], "start": 0, "end": 0}

    class _Ctx:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    fake = ModuleType("mlflow")
    fake.set_tracking_uri = lambda uri: calls.setdefault("uri", uri)
    fake.set_experiment = lambda exp: calls.setdefault("exp", exp)
    fake.start_run = lambda **kwargs: (calls.__setitem__("start", calls["start"] + 1) or _Ctx())
    fake.end_run = lambda: calls.__setitem__("end", calls["end"] + 1)
    fake.log_params = lambda d: calls["params"].append(d)
    fake.log_metrics = lambda d: calls["metrics"].append(d)
    fake.log_param = lambda k, v: calls["params"].append({k: v})
    fake.log_metric = lambda k, v: calls["metrics"].append({k: v})
    fake.set_tags = lambda d: calls["tags"].append(d)
    monkeypatch.setitem(sys.modules, "mlflow", fake)

    t = MlflowTracker(tracking_uri="u", experiment="e", run_name="r", tags={"team": "x"})
    t.start(pipeline="p.py", maximize=True, metadata={"executor": "direct"})
    t.log_trial(
        trial_id=1,
        status="ok",
        score=1.2,
        params={"a": 1},
        error=None,
        executor="direct",
        duration_seconds=0.1,
        executor_result=None,
    )
    t.finish({"successful_trials": 1, "submitted_trials": 0, "failed_trials": 0, "best": {"score": 1.2}})
    assert calls["start"] >= 2  # parent + nested trial
    assert calls["end"] == 1
    assert calls["tags"]


def test_wandb_tracker_logs(monkeypatch):
    logs = []
    finished = {"v": False}

    class _Run:
        def log(self, payload, step=None):
            logs.append((payload, step))

        def finish(self):
            finished["v"] = True

    fake = ModuleType("wandb")
    fake.init = lambda **kwargs: _Run()
    monkeypatch.setitem(sys.modules, "wandb", fake)

    t = WandbTracker(project="p", run_name="r", tags={"x": 1}, mode="offline")
    t.start(pipeline="p.py", maximize=False, metadata={"executor": "bmll"})
    t.log_trial(
        trial_id=3,
        status="submitted",
        score=None,
        params={"k": "v"},
        error=None,
        executor="bmll",
        duration_seconds=0.2,
        executor_result={"run_id": "r1"},
    )
    t.finish({"successful_trials": 0, "submitted_trials": 1, "failed_trials": 0, "best": None})
    assert logs
    assert finished["v"] is True
