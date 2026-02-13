from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import os


def _app_path() -> str:
    return str(Path(__file__).with_name("app.py"))


class VisualizationCLI:
    @staticmethod
    def run(
        *,
        pipeline: str,
        config_precedence: str = "yaml_overrides",
        headless: bool = False,
        server_port: int = 8501,
        auth_mode: str = "none",
        access_password: str | None = None,
        enable_cors: bool = False,
        tunnel: str = "none",
        ingress: str | None = None,
        tunnel_host: str = "serveo.net",
        ssh_forward_enabled: bool = False,
        authorized_keys: str | None = None,
    ):
        """
        Launch Streamlit explorer for a pipeline config.
        """
        tunnel = str(tunnel).strip().lower()
        auth_mode = str(auth_mode).strip().lower()
        env = os.environ.copy()
        # Prevent first-run interactive email prompt (breaks tunnel startup).
        env.setdefault("STREAMLIT_BROWSER_GATHER_USAGE_STATS", "false")
        if auth_mode == "password":
            env["UI_PASSWORD_ENABLED"] = "password"
            if access_password:
                env["ACCESS_PASSWORD"] = access_password
        elif auth_mode in {"none", "false"}:
            env["UI_PASSWORD_ENABLED"] = "false"
        else:
            raise ValueError("auth_mode must be one of: none,password")

        if ssh_forward_enabled and authorized_keys:
            helper = Path(__file__).resolve().parents[2] / "archives" / "lobv" / "allow_ssh_in.py"
            if helper.exists():
                subprocess.Popen(
                    [
                        sys.executable,
                        str(helper),
                        str(server_port),
                        "basalt-viz",
                        str(authorized_keys),
                    ],
                    env=env,
                )

        tunnel_proc = None
        if tunnel == "serveo":
            if not ingress:
                raise ValueError("Provide --ingress when tunnel=serveo")
            tunnel_proc = subprocess.Popen(
                [
                    "ssh",
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-R",
                    f"{ingress}:80:localhost:{int(server_port)}",
                    str(tunnel_host),
                ],
                env=env,
            )
        elif tunnel != "none":
            raise ValueError("tunnel must be one of: none,serveo")

        cmd = [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            _app_path(),
            "--server.port",
            str(server_port),
            "--server.headless",
            "true" if headless else "false",
            "--server.enableCORS",
            "true" if enable_cors else "false",
            "--browser.gatherUsageStats",
            "false",
            "--",
            "--pipeline",
            str(pipeline),
            "--config_precedence",
            str(config_precedence),
        ]
        try:
            return subprocess.call(cmd, env=env)
        finally:
            if tunnel_proc is not None:
                try:
                    tunnel_proc.terminate()
                except Exception:
                    pass


def get_cli_extension():
    return {"viz": VisualizationCLI}
