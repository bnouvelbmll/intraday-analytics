from __future__ import annotations

import os
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


def _disable_fire_pager() -> None:
    if "PAGER" not in os.environ:
        os.environ["PAGER"] = "cat"


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

def _load_pipeline_module(path_or_name: str):
    from pathlib import Path
    import importlib.util
    import importlib

    path = Path(path_or_name)
    if path.exists():
        spec = importlib.util.spec_from_file_location(path.stem, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Unable to load pipeline: {path_or_name}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[attr-defined]
        return module
    return importlib.import_module(path_or_name)


def _help_requested() -> bool:
    return any(arg in {"-h", "--help"} for arg in sys.argv)


def _pipeline_run(*, pipeline: str | None = None, **kwargs):
    if _help_requested():
        print(
            "Usage: beaf pipeline run --pipeline <path_or_module> [options]\n"
            "Options are forwarded to the pipeline CLI (e.g. --start_date, --end_date, --batch_freq)."
        )
        return None
    if not pipeline:
        raise SystemExit("Provide --pipeline <path_or_module>")
    module = _load_pipeline_module(pipeline)
    if not hasattr(module, "USER_CONFIG"):
        raise SystemExit("Pipeline module must define USER_CONFIG.")
    if not hasattr(module, "get_universe"):
        raise SystemExit("Pipeline module must define get_universe(date).")
    config_file = getattr(module, "__file__", None) or pipeline
    kwargs.setdefault("config_file", config_file)
    return run_cli(
        module.USER_CONFIG,
        module.get_universe,
        get_pipeline=getattr(module, "get_pipeline", None),
        **kwargs,
    )


def _job_run(*, pipeline: str | None = None, **kwargs):
    if _help_requested():
        from fire import helptext

        print(helptext.HelpText(bmll_job_run))
        return None
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


class AnalyticsCLI:
    @staticmethod
    def list(*args, **kwargs):
        return _schema_utils(*args, **kwargs)

    @staticmethod
    def explain(*args, **kwargs):
        return _analytics_explain(*args, **kwargs)


class PipelineCLI:
    @staticmethod
    def run(*args, **kwargs):
        return _pipeline_run(*args, **kwargs)

    @staticmethod
    def config(*args, **kwargs):
        return _config_ui(*args, **kwargs)


class JobCLI:
    @staticmethod
    def run(*args, **kwargs):
        return _job_run(*args, **kwargs)

    @staticmethod
    def install(*args, **kwargs):
        return bmll_job_install(*args, **kwargs)


class DagsterSchedulerCLI:
    @staticmethod
    def install(*args, **kwargs):
        return dagster_scheduler_install(*args, **kwargs)

    @staticmethod
    def uninstall(*args, **kwargs):
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
        return dagster_sync(*args, **kwargs)

    @staticmethod
    def ui(*args, **kwargs):
        return _dagster_ui(*args, **kwargs)


class BeafCLI:
    analytics = AnalyticsCLI
    pipeline = PipelineCLI
    job = JobCLI
    dagster = DagsterCLI


def main():
    _disable_fire_pager()
    fire.Fire(BeafCLI)


if __name__ == "__main__":
    main()
