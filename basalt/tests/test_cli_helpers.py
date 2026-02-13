from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import basalt.cli as cli


def test_deep_merge_nested():
    out = cli._deep_merge({"a": {"x": 1}, "b": 2}, {"a": {"y": 3}, "c": 4})
    assert out == {"a": {"x": 1, "y": 3}, "b": 2, "c": 4}


def test_load_yaml_config_missing(tmp_path):
    assert cli._load_yaml_config(str(tmp_path / "missing.yaml")) == {}


def test_extract_user_config_variants():
    assert cli._extract_user_config({"USER_CONFIG": {"A": 1}}) == {"A": 1}
    assert cli._extract_user_config({"A": 1}) == {"A": 1}


def test_resolve_user_config_yaml_precedence(tmp_path):
    py = tmp_path / "p.py"
    py.write_text("x=1\n", encoding="utf-8")
    py.with_suffix(".yaml").write_text("USER_CONFIG:\n  A: 2\n", encoding="utf-8")
    out = cli.resolve_user_config({"A": 1, "B": 3}, str(py), "yaml_overrides")
    assert out == {"A": 2, "B": 3}
    out2 = cli.resolve_user_config({"A": 1, "B": 3}, str(py), "python_overrides")
    assert out2 == {"A": 1, "B": 3}


def test_format_cli_args():
    args = cli._format_cli_args({"a": 1, "b": True, "c": False, "d": None})
    assert "--a=1" in args
    assert "--b=True" in args
    assert "--c=False" in args
    assert all("--d" not in x for x in args)


def test_write_basalt_run_script(tmp_path):
    cfg = cli.BMLLJobConfig(jobs_dir=str(tmp_path))
    script = cli._write_basalt_run_script(cfg, ["--date=2026-01-01"])
    text = Path(script).read_text(encoding="utf-8")
    assert "python -m basalt.basalt run --date=2026-01-01" in text


def test_load_bmll_job_config_yaml(tmp_path):
    yaml_path = tmp_path / "p.yaml"
    yaml_path.write_text(
        "USER_CONFIG:\n  BMLL_JOBS:\n    enabled: true\n    default_instance_size: 32\n",
        encoding="utf-8",
    )
    cfg = cli._load_bmll_job_config(str(yaml_path))
    assert cfg.enabled is True
    assert cfg.default_instance_size == 32
    assert cfg.project_root == str(tmp_path.resolve())
    assert cfg.jobs_dir == str((tmp_path.resolve() / "_bmll_jobs"))


def test_load_bmll_job_config_demo_pipeline_yaml(tmp_path):
    demo = tmp_path / "demo.py"
    demo.write_text("USER_CONFIG={}\n", encoding="utf-8")
    demo.with_suffix(".yaml").write_text(
        "USER_CONFIG:\n  BMLL_JOBS:\n    enabled: true\n    default_instance_size: 48\n",
        encoding="utf-8",
    )
    defs = tmp_path / "defs.yaml"
    defs.write_text(
        f"DEMO_PIPELINE: {demo.name}\nPIPELINE_OVERRIDES:\n  {demo.stem}:\n    BMLL_JOBS:\n      default_instance_size: 64\n",
        encoding="utf-8",
    )
    cfg = cli._load_bmll_job_config(str(defs))
    assert cfg.enabled is True
    assert cfg.default_instance_size == 64


def test_load_bmll_job_config_defaults_resolve_to_runtime_path(tmp_path):
    cfg = cli._load_bmll_job_config(str(tmp_path / "missing.yaml"))
    assert cfg.project_root == str(tmp_path.resolve())
    assert cfg.jobs_dir == str((tmp_path.resolve() / "_bmll_jobs"))


def test_bmll_job_run_requires_config():
    with pytest.raises(SystemExit, match="Provide --config_file"):
        cli.bmll_job_run()


def test_bmll_job_run_delegates(monkeypatch, tmp_path):
    py = tmp_path / "p.py"
    py.write_text("USER_CONFIG={}\n", encoding="utf-8")
    monkeypatch.setattr(cli, "_load_bmll_job_config", lambda _: cli.BMLLJobConfig(jobs_dir=str(tmp_path)))
    monkeypatch.setattr(cli, "_write_basalt_run_script", lambda *_args, **_kwargs: str(tmp_path / "r.sh"))
    monkeypatch.setattr(cli, "_default_basalt_job_name", lambda _c, _n: "name")
    captured = {}
    monkeypatch.setattr(cli, "submit_instance_job", lambda *a, **k: captured.setdefault("kwargs", k) or {"ok": True})
    out = cli.bmll_job_run(config_file=str(py), date="2026-01-01")
    assert "kwargs" in captured
    assert out is not None


def test_dagster_sync_builds_command(monkeypatch, tmp_path):
    calls = {}
    script = tmp_path / "basalt" / "dagster" / "scripts" / "s3_bulk_sync_db.py"
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text("print('ok')\n", encoding="utf-8")
    monkeypatch.setattr(cli.Path, "resolve", lambda self: tmp_path / "basalt" / "cli.py")
    monkeypatch.setattr(cli.subprocess, "check_call", lambda cmd: calls.setdefault("cmd", cmd))
    cli.dagster_sync(tables="all", start_date="2026-01-01", end_date="2026-01-02")
    assert "sync" in calls["cmd"]


def test_run_cli_accepts_user_config_json(monkeypatch):
    captured = {}

    class _Cfg:
        def __init__(self, **kwargs):
            captured["cfg"] = kwargs

    monkeypatch.setattr(cli, "AnalyticsConfig", _Cfg)
    monkeypatch.setattr(cli, "run_multiday_pipeline", lambda **kwargs: captured.setdefault("run", kwargs))
    cli.run_cli(
        {"A": 1},
        default_get_universe=lambda d: {"ListingId": [1]},
        user_config_json=json.dumps({"START_DATE": "2026-01-01", "END_DATE": "2026-01-01", "X": 9}),
    )
    assert captured["cfg"]["X"] == 9
