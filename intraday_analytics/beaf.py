from __future__ import annotations

import fire
import sys

from intraday_analytics.cli import (
    bmll_job_install,
    bmll_job_run,
    dagster_scheduler_install,
    dagster_scheduler_uninstall,
    dagster_sync,
    run_cli,
)
from intraday_analytics import schema_utils
from intraday_analytics import config_ui
from intraday_analytics.analytics_explain import main as analytics_explain_main
from intraday_analytics.dagster_remote import run_dagster_pipeline, run_dagster_remote


def _schema_utils(*args, **kwargs):
    arg_list = list(args)
    for key, value in kwargs.items():
        flag = f"--{key.replace('_','-')}"
        if isinstance(value, bool):
            if value:
                arg_list.append(flag)
        else:
            arg_list.extend([flag, str(value)])
    if "--pipeline" in arg_list:
        from intraday_analytics.cli import _load_yaml_config, _extract_user_config
        import json
        import tempfile
        from pathlib import Path

        idx = arg_list.index("--pipeline")
        if idx + 1 < len(arg_list):
            pipeline_path = arg_list[idx + 1]
            yaml_path = Path(pipeline_path).with_suffix(".yaml")
            data = _load_yaml_config(str(yaml_path))
            user_cfg = _extract_user_config(data) if isinstance(data, dict) else {}
            temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
            temp.write(json.dumps(user_cfg).encode("utf-8"))
            temp.close()
            arg_list[idx:idx + 2] = ["--config", temp.name]
    sys.argv = ["schema_utils", *arg_list]
    return schema_utils.main()


def _config_ui(*args):
    sys.argv = ["config_ui", *args]
    return config_ui.main()

def _analytics_explain(*args):
    sys.argv = ["analytics_explain", *args]
    return analytics_explain_main()

def _pipeline_run(*, pipeline: str | None = None, **kwargs):
    if pipeline and "config_file" not in kwargs:
        kwargs["config_file"] = pipeline
    return run_cli(**kwargs)


def _job_run(*, pipeline: str | None = None, **kwargs):
    if pipeline and "config_file" not in kwargs:
        kwargs["config_file"] = pipeline
    return bmll_job_run(**kwargs)


def _dagster_install(
    *,
    pipeline: str,
    cron: str,
    timezone: str = "UTC",
    schedule_name: str | None = None,
    enable_automaterialize: bool = True,
    auto_materialize_latest_days: int | None = None,
    partition: str | None = None,
):
    from intraday_analytics.cli import _load_yaml_config, _extract_user_config, _save_yaml_user_config
    from pathlib import Path

    yaml_path = Path(pipeline).with_suffix(".yaml")
    data = _load_yaml_config(str(yaml_path))
    user_cfg = _extract_user_config(data) if isinstance(data, dict) else {}
    schedules = list(user_cfg.get("SCHEDULES", []) or [])

    if schedule_name is None:
        schedule_name = Path(pipeline).stem

    entry = {
        "name": schedule_name,
        "enabled": True,
        "cron": cron,
        "timezone": timezone,
    }
    if partition:
        # partition format: "date=YYYY-MM-DD,universe=mic=XLON"
        parts = {}
        for item in partition.split(","):
            if "=" in item:
                key, value = item.split("=", 1)
                parts[key.strip()] = value.strip()
        entry["partitions"] = [parts]

    replaced = False
    for idx, sched in enumerate(schedules):
        if sched.get("name") == schedule_name:
            schedules[idx] = entry
            replaced = True
            break
    if not replaced:
        schedules.append(entry)

    user_cfg["SCHEDULES"] = schedules
    if enable_automaterialize:
        user_cfg["AUTO_MATERIALIZE_ENABLED"] = True
        if auto_materialize_latest_days is not None:
            user_cfg["AUTO_MATERIALIZE_LATEST_DAYS"] = int(auto_materialize_latest_days)

    _save_yaml_user_config(yaml_path, user_cfg)


def _dagster_uninstall(
    *,
    pipeline: str,
    schedule_name: str | None = None,
    disable_automaterialize: bool = False,
):
    from intraday_analytics.cli import _load_yaml_config, _extract_user_config, _save_yaml_user_config
    from pathlib import Path

    yaml_path = Path(pipeline).with_suffix(".yaml")
    data = _load_yaml_config(str(yaml_path))
    user_cfg = _extract_user_config(data) if isinstance(data, dict) else {}
    schedules = list(user_cfg.get("SCHEDULES", []) or [])

    if schedule_name is None:
        schedule_name = Path(pipeline).stem

    schedules = [s for s in schedules if s.get("name") != schedule_name]
    user_cfg["SCHEDULES"] = schedules
    if disable_automaterialize:
        user_cfg["AUTO_MATERIALIZE_ENABLED"] = False

    _save_yaml_user_config(yaml_path, user_cfg)


def _dagster_run(
    *,
    run_id: str | None = None,
    instance_ref_file: str | None = None,
    pipeline: str | None = None,
    job: str | None = None,
    partition: str | None = None,
):
    if run_id and instance_ref_file:
        return run_dagster_remote(
            run_id=run_id,
            instance_ref_file=instance_ref_file,
            set_exit_code_on_failure=True,
        )
    if pipeline:
        return run_dagster_pipeline(
            pipeline_file=pipeline,
            job_name=job,
            partition=partition,
        )
    raise SystemExit("Provide either run_id+instance_ref_file or pipeline.")


def _dagster_ui(
    *,
    pipeline: str | None = None,
    workspace: str | None = None,
    host: str = "0.0.0.0",
    port: int = 3000,
    dagster_home: str | None = None,
):
    import os
    import subprocess

    if not pipeline and not workspace:
        raise SystemExit("Provide --pipeline or --workspace.")
    cmd = ["dagster-webserver", "-h", host, "-p", str(port)]
    if workspace:
        cmd.extend(["-w", workspace])
    else:
        cmd.extend(["-f", pipeline])
    env = os.environ.copy()
    if dagster_home:
        env["DAGSTER_HOME"] = dagster_home
    return subprocess.call(cmd, env=env)


def main():
    fire.Fire(
        {
            "analytics": {"list": _schema_utils, "explain": _analytics_explain},
            "pipeline": {"run": _pipeline_run, "config": _config_ui},
            "job": {"run": _job_run, "install": bmll_job_install},
            "dagster": {
                "run": _dagster_run,
                "install": _dagster_install,
                "uninstall": _dagster_uninstall,
                "sync": dagster_sync,
                "scheduler": {
                    "install": dagster_scheduler_install,
                    "uninstall": dagster_scheduler_uninstall,
                },
                "ui": _dagster_ui,
            },
        }
    )


if __name__ == "__main__":
    main()
