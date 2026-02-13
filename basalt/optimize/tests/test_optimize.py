from __future__ import annotations

import json
import textwrap

import pytest

from basalt.optimize.cli_ext import _load_search_space, get_cli_extension
from basalt.optimize.core import apply_overrides, optimize_pipeline, sample_params


def test_apply_overrides_nested_paths():
    cfg = {"passes": [{"name": "p1", "time_bucket_seconds": 1}]}
    out = apply_overrides(cfg, {"passes[0].time_bucket_seconds": 5, "A.B": 2})
    assert out["passes"][0]["time_bucket_seconds"] == 5
    assert out["A"]["B"] == 2
    # Original payload remains untouched.
    assert cfg["passes"][0]["time_bucket_seconds"] == 1


def test_sample_params_supports_types():
    import random

    rng = random.Random(123)
    out = sample_params(
        {
            "params": {
                "a": {"type": "choice", "values": [1, 2]},
                "b": {"type": "int", "low": 2, "high": 4},
                "c": {"type": "float", "low": 1.0, "high": 2.0},
                "d": {"type": "bool"},
            }
        },
        rng=rng,
    )
    assert out["a"] in {1, 2}
    assert 2 <= out["b"] <= 4
    assert 1.0 <= out["c"] <= 2.0
    assert isinstance(out["d"], bool)


def test_load_search_space_json_and_file(tmp_path):
    payload = {"params": {"A.B": {"type": "choice", "values": [1]}}}
    file_path = tmp_path / "space.yaml"
    file_path.write_text("params:\n  A.B:\n    type: choice\n    values: [1]\n", encoding="utf-8")
    from_file = _load_search_space(search_space_file=str(file_path))
    assert from_file == payload
    from_json = _load_search_space(search_space=json.dumps(payload))
    assert from_json == payload


def test_load_search_space_requires_input():
    with pytest.raises(ValueError, match="Provide --search_space_file or --search_space"):
        _load_search_space()


def test_optimize_cli_extension_shape():
    ext = get_cli_extension()
    assert "optimize" in ext
    assert hasattr(ext["optimize"], "run")


def test_optimize_direct_requires_score_fn(tmp_path):
    module_path = tmp_path / "demo_mod.py"
    module_path.write_text(
        textwrap.dedent(
            """
            USER_CONFIG = {"START_DATE": "2025-01-01", "END_DATE": "2025-01-01"}
            def get_universe(date):
                return {"ListingId": [1], "MIC": ["X"]}
            """
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="provide score_fn or"):
        optimize_pipeline(
            pipeline=str(module_path),
            search_space={"params": {}},
            trials=1,
            executor="direct",
            score_fn=None,
            output_dir=str(tmp_path / "out"),
        )


def test_optimize_bmll_dispatch_without_score_fn(tmp_path, monkeypatch):
    module_path = tmp_path / "demo_mod.py"
    module_path.write_text(
        textwrap.dedent(
            """
            USER_CONFIG = {"START_DATE": "2025-01-01", "END_DATE": "2025-01-01", "A": 1}
            def get_universe(date):
                return {"ListingId": [1], "MIC": ["X"]}
            """
        ),
        encoding="utf-8",
    )

    class _Job:
        id = 111

    class _Run:
        id = 222

    calls: list[dict] = []

    def _fake_bmll_job_run(**kwargs):
        calls.append(kwargs)
        return (_Job(), _Run())

    import basalt.cli as cli

    monkeypatch.setattr(cli, "bmll_job_run", _fake_bmll_job_run)
    out = optimize_pipeline(
        pipeline=str(module_path),
        search_space={"params": {"A": {"type": "choice", "values": [2]}}},
        trials=1,
        executor="bmll",
        score_fn=None,
        output_dir=str(tmp_path / "out"),
    )
    assert out["executor"] == "bmll"
    assert out["successful_trials"] == 0
    assert out["submitted_trials"] == 1
    assert out["failed_trials"] == 0
    assert out["best"] is None
    assert calls and calls[0]["config_file"].endswith("demo_mod.py")
    payload = json.loads(calls[0]["user_config_json"])
    assert payload["A"] == 2


def test_optimize_pipeline_uses_tracker(tmp_path, monkeypatch):
    module_path = tmp_path / "demo_mod.py"
    module_path.write_text(
        textwrap.dedent(
            """
            USER_CONFIG = {"START_DATE": "2025-01-01", "END_DATE": "2025-01-01", "A": 1}
            def get_universe(date):
                return {"ListingId": [1], "MIC": ["X"]}
            """
        ),
        encoding="utf-8",
    )
    tracker_calls = {"start": 0, "trial": 0, "finish": 0}

    class _Tracker:
        name = "mock"

        def start(self, **kwargs):
            tracker_calls["start"] += 1

        def log_trial(self, **kwargs):
            tracker_calls["trial"] += 1

        def finish(self, summary):
            tracker_calls["finish"] += 1

    import basalt.optimize.core as core

    monkeypatch.setattr(core, "create_tracker", lambda **kwargs: _Tracker())
    out = optimize_pipeline(
        pipeline=str(module_path),
        search_space={"params": {"A": {"type": "choice", "values": [2]}}},
        trials=1,
        executor="bmll",
        score_fn=None,
        tracker="wandb",
        output_dir=str(tmp_path / "out"),
    )
    assert out["tracker"] == "mock"
    assert tracker_calls == {"start": 1, "trial": 1, "finish": 1}


def test_optimize_pipeline_with_model_objective_and_generator(tmp_path):
    module_path = tmp_path / "demo_mod.py"
    module_path.write_text(
        textwrap.dedent(
            """
            USER_CONFIG = {"START_DATE": "2025-01-01", "END_DATE": "2025-01-01", "A": 1}
            def get_universe(date):
                return {"ListingId": [1], "MIC": ["X"]}
            """
        ),
        encoding="utf-8",
    )
    import basalt.optimize.core as core

    original = core._run_pipeline_with_config
    core._run_pipeline_with_config = lambda module, user_config: {"ok": True}
    try:
        out = optimize_pipeline(
            pipeline=str(module_path),
            search_space={"params": {"A": {"type": "choice", "values": [1, 2]}}},
            trials=1,
            executor="direct",
            score_fn=None,
            model_factory="basalt.optimize.tests.test_optimize_integration_helpers:model_factory",
            dataset_builder="basalt.optimize.tests.test_optimize_integration_helpers:dataset_builder",
            objectives="basalt.optimize.tests.test_optimize_integration_helpers:objectives",
            search_generator="basalt.optimize.tests.test_optimize_integration_helpers:generator",
            output_dir=str(tmp_path / "out"),
        )
    finally:
        core._run_pipeline_with_config = original
    assert out["successful_trials"] == 1
    assert out["best"] is not None


def test_optimize_pipeline_accepts_objective_names(tmp_path):
    module_path = tmp_path / "demo_mod.py"
    module_path.write_text(
        textwrap.dedent(
            """
            USER_CONFIG = {"START_DATE": "2025-01-01", "END_DATE": "2025-01-01", "A": 1}
            def get_universe(date):
                return {"ListingId": [1], "MIC": ["X"]}
            """
        ),
        encoding="utf-8",
    )
    import basalt.optimize.core as core

    original = core._run_pipeline_with_config
    core._run_pipeline_with_config = lambda module, user_config: {"ok": True}
    try:
        out = optimize_pipeline(
            pipeline=str(module_path),
            search_space={"params": {"A": {"type": "choice", "values": [1, 2]}}},
            trials=1,
            executor="direct",
            score_fn=None,
            model_factory="basalt.optimize.tests.test_optimize_integration_helpers:model_factory",
            dataset_builder="basalt.optimize.tests.test_optimize_integration_helpers:dataset_builder",
            objectives="mae,directional_accuracy",
            output_dir=str(tmp_path / "out"),
        )
    finally:
        core._run_pipeline_with_config = original
    assert out["successful_trials"] == 1
    assert out["best"] is not None
