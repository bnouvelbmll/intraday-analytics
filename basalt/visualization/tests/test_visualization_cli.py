from __future__ import annotations

from basalt.visualization.cli_ext import get_cli_extension


def test_visualization_cli_extension_shape():
    ext = get_cli_extension()
    assert "viz" in ext
    assert hasattr(ext["viz"], "run")


def test_visualization_cli_run_builds_streamlit_command(monkeypatch):
    ext = get_cli_extension()
    calls = {"call": None, "popen": []}

    def _fake_call(cmd, env=None):
        calls["call"] = (cmd, env)
        return 0

    class _P:
        def terminate(self):
            return None

    def _fake_popen(cmd, env=None):
        calls["popen"].append((cmd, env))
        return _P()

    monkeypatch.setattr("subprocess.call", _fake_call)
    monkeypatch.setattr("subprocess.Popen", _fake_popen)
    rc = ext["viz"].run(
        pipeline="demo/02_multi_pass.py",
        auth_mode="password",
        access_password="secret",
        tunnel="serveo",
        ingress="test.serveo.net",
        headless=True,
        server_port=8555,
    )
    assert rc == 0
    assert calls["call"] is not None
    cmd, env = calls["call"]
    assert "streamlit" in cmd
    assert "--server.port" in cmd
    assert "8555" in cmd
    assert env["UI_PASSWORD_ENABLED"] == "password"
    assert env["ACCESS_PASSWORD"] == "secret"
    assert calls["popen"], "Expected tunnel popen call"
