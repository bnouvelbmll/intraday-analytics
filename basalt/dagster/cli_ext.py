from __future__ import annotations

import subprocess
from pathlib import Path


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
    from basalt.cli import _load_yaml_config, _extract_user_config
    from basalt.config_ui import _save_yaml_user_config

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
    from basalt.cli import _load_yaml_config, _extract_user_config
    from basalt.config_ui import _save_yaml_user_config

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
        from basalt.dagster.dagster_remote import run_dagster_remote

        return run_dagster_remote(
            run_id=run_id,
            instance_ref_file=instance_ref_file,
            set_exit_code_on_failure=True,
        )
    if pipeline:
        from basalt.dagster.dagster_remote import run_dagster_pipeline

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


class DagsterSchedulerCLI:
    @staticmethod
    def install(*args, **kwargs):
        from basalt.cli import dagster_scheduler_install

        return dagster_scheduler_install(*args, **kwargs)

    @staticmethod
    def uninstall(*args, **kwargs):
        from basalt.cli import dagster_scheduler_uninstall

        return dagster_scheduler_uninstall(*args, **kwargs)


class DagsterCLI:
    scheduler = DagsterSchedulerCLI

    @staticmethod
    def run(*args, **kwargs):
        return _dagster_run(*args, **kwargs)

    @staticmethod
    def install(*args, **kwargs):
        return _dagster_install(*args, **kwargs)

    @staticmethod
    def uninstall(*args, **kwargs):
        return _dagster_uninstall(*args, **kwargs)

    @staticmethod
    def sync(*args, **kwargs):
        from basalt.cli import dagster_sync

        return dagster_sync(*args, **kwargs)

    @staticmethod
    def ui(*args, **kwargs):
        return _dagster_ui(*args, **kwargs)


def get_cli_extension():
    return {"dagster": DagsterCLI}
