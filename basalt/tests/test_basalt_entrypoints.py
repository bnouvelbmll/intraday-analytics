from __future__ import annotations

import io
import sys
import textwrap
import types
from pathlib import Path

import pytest

import basalt.basalt as basalt_main


def test_disable_fire_pager_sets_default(monkeypatch):
    monkeypatch.delenv("PAGER", raising=False)
    basalt_main._disable_fire_pager()
    assert "PAGER" in __import__("os").environ


def test_schema_utils_wrapper_pipeline_uses_yaml_user_config(tmp_path, monkeypatch):
    py_path = tmp_path / "demo_pipeline.py"
    py_path.write_text("USER_CONFIG = {}\n", encoding="utf-8")
    yaml_path = py_path.with_suffix(".yaml")
    yaml_path.write_text("USER_CONFIG:\n  LEVELS: 3\n", encoding="utf-8")

    captured = {}

    def _fake_main():
        captured["argv"] = list(sys.argv)
        return 123

    monkeypatch.setattr(basalt_main.schema_utils, "main", _fake_main)

    rc = basalt_main._schema_utils(pipeline=str(py_path), full=True)
    assert rc == 123
    assert "--config" in captured["argv"]
    cfg_idx = captured["argv"].index("--config")
    cfg_path = Path(captured["argv"][cfg_idx + 1])
    assert cfg_path.exists()
    assert '"LEVELS": 3' in cfg_path.read_text(encoding="utf-8")


def test_config_ui_wrapper(monkeypatch):
    seen = {}

    def _fake_main():
        seen["argv"] = list(sys.argv)
        return 7

    monkeypatch.setattr(basalt_main.config_ui, "main", _fake_main)
    rc = basalt_main._config_ui("--foo", "bar")
    assert rc == 7
    assert seen["argv"][0] == "config_ui"


def test_analytics_explain_wrapper(monkeypatch):
    seen = {}

    def _fake_main():
        seen["argv"] = list(sys.argv)
        return 8

    monkeypatch.setattr(basalt_main, "analytics_explain_main", _fake_main)
    rc = basalt_main._analytics_explain("--column", "X")
    assert rc == 8
    assert seen["argv"][0] == "analytics_explain"


def test_load_pipeline_module_from_path(tmp_path):
    module_path = tmp_path / "mod1.py"
    module_path.write_text("VALUE = 5\n", encoding="utf-8")
    mod = basalt_main._load_pipeline_module(str(module_path))
    assert mod.VALUE == 5


def test_load_pipeline_module_from_import_name(tmp_path, monkeypatch):
    module_path = tmp_path / "mod2.py"
    module_path.write_text("VALUE = 11\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    mod = basalt_main._load_pipeline_module("mod2")
    assert mod.VALUE == 11


def test_help_requested(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["basalt", "--help"])
    assert basalt_main._help_requested() is True


def test_pipeline_run_help(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["basalt", "pipeline", "run", "--help"])
    assert basalt_main._pipeline_run() is None
    out = capsys.readouterr().out
    assert "Usage: basalt pipeline run" in out


def test_pipeline_run_requires_pipeline(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["basalt", "pipeline", "run"])
    with pytest.raises(SystemExit, match="Provide --pipeline"):
        basalt_main._pipeline_run()


def test_pipeline_run_requires_contract(tmp_path, monkeypatch):
    no_user = tmp_path / "no_user.py"
    no_user.write_text("def get_universe(date):\n    return {}\n", encoding="utf-8")

    with pytest.raises(SystemExit, match="USER_CONFIG"):
        basalt_main._pipeline_run(pipeline=str(no_user))

    no_universe = tmp_path / "no_universe.py"
    no_universe.write_text("USER_CONFIG = {}\n", encoding="utf-8")

    with pytest.raises(SystemExit, match="get_universe"):
        basalt_main._pipeline_run(pipeline=str(no_universe))


def test_pipeline_run_delegates_to_run_cli(tmp_path, monkeypatch):
    module_path = tmp_path / "ok_mod.py"
    module_path.write_text(
        textwrap.dedent(
            """
            USER_CONFIG = {"X": 1}
            def get_universe(date):
                return {"ListingId": [1], "MIC": ["X"]}
            def get_pipeline():
                return "p"
            """
        ),
        encoding="utf-8",
    )
    captured = {}

    def _fake_run_cli(user_config, get_universe, get_pipeline=None, **kwargs):
        captured["user_config"] = user_config
        captured["kwargs"] = kwargs
        captured["pipeline"] = get_pipeline
        return "ok"

    monkeypatch.setattr(basalt_main, "run_cli", _fake_run_cli)
    out = basalt_main._pipeline_run(pipeline=str(module_path), batch_freq="W")
    assert out == "ok"
    assert captured["user_config"]["X"] == 1
    assert callable(captured["pipeline"])
    assert captured["kwargs"]["batch_freq"] == "W"


def test_job_run_help(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["basalt", "ec2", "run", "--help"])
    assert basalt_main._job_run() is None
    assert "Usage: basalt ec2 run" in capsys.readouterr().out


def test_job_run_delegates(monkeypatch):
    monkeypatch.setattr(basalt_main, "_ec2_run", lambda **kwargs: kwargs)
    out = basalt_main._job_run(pipeline="x.py", dry_run=True)
    assert out["pipeline"] == "x.py"


def test_ec2_and_k8s_missing_modules_raise(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def _fail_import(name, *args, **kwargs):
        if name.startswith("basalt.executors.aws_ec2") or name.startswith(
            "basalt.executors.kubernetes"
        ):
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fail_import)

    with pytest.raises(SystemExit, match="bmll-basalt-aws-ec2"):
        basalt_main._ec2_run(pipeline="x")
    with pytest.raises(SystemExit, match="bmll-basalt-aws-ec2"):
        basalt_main._ec2_install(pipeline="x")
    with pytest.raises(SystemExit, match="bmll-basalt-kubernetes"):
        basalt_main._k8s_run()
    with pytest.raises(SystemExit, match="bmll-basalt-kubernetes"):
        basalt_main._k8s_install()


def test_load_cli_extensions_handles_mixed_entries(monkeypatch):
    class _EP:
        def __init__(self, name, payload):
            self.name = name
            self._payload = payload

        def load(self):
            if isinstance(self._payload, Exception):
                raise self._payload
            return self._payload

    def _entry_points(*, group=None):
        assert group == "basalt.cli"
        return [
            _EP("x", lambda: {"extra": object()}),
            _EP("y", lambda: "val"),
            _EP("z", RuntimeError("boom")),
        ]

    import importlib.metadata as ilm

    monkeypatch.setattr(ilm, "entry_points", _entry_points)

    err = io.StringIO()
    monkeypatch.setattr(sys, "stderr", err)
    out = basalt_main._load_cli_extensions()
    assert "extra" in out
    assert out["y"] == "val"
    assert "Skipping CLI extension z" in err.getvalue()


def test_main_invokes_fire_with_extensions(monkeypatch):
    called = {}

    monkeypatch.setattr(basalt_main, "_load_cli_extensions", lambda: {"x": 1})

    def _fake_fire(root):
        called["root"] = root

    monkeypatch.setattr(basalt_main.fire, "Fire", _fake_fire)
    monkeypatch.setattr(sys, "argv", ["basalt"])

    basalt_main.main()
    assert "analytics" in called["root"]
    assert called["root"]["x"] == 1


def test_plugins_cli_list(monkeypatch):
    monkeypatch.setattr(
        basalt_main,
        "list_plugins_payload",
        lambda: [{"name": "dagster", "status": "ok"}],
    )
    out = basalt_main.PluginsCLI.list()
    assert out == [{"name": "dagster", "status": "ok"}]
