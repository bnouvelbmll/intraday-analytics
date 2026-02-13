import importlib
import logging
import os
import time
from pathlib import Path
from functools import partial
from typing import Callable, Optional
import inspect
import subprocess
import sys
import json

import fire
import yaml

from basalt.configuration import AnalyticsConfig, BMLLJobConfig
from basalt.executors.bmll_jobs import submit_instance_job
from basalt.orchestrator import run_multiday_pipeline


def _disable_fire_pager() -> None:
    if "PAGER" not in os.environ:
        os.environ["PAGER"] = "cat"


def _apply_job_prefix(name: str, prefix: str) -> str:
    if name.startswith(prefix):
        return name
    return f"{prefix} {name}"


def _default_basalt_job_name(config_file: str | None, name: str | None) -> str:
    base = Path(config_file).stem if config_file else "job"
    prefix = f"[basalt][{base}][pipeline]"
    if name:
        return _apply_job_prefix(name, prefix)
    return prefix


def _default_basalt_scheduler_name(
    pipeline: str | None, workspace: str | None, name: str | None
) -> str:
    if name:
        return _apply_job_prefix(name, "[basalt][dagster][scheduler]")
    if pipeline:
        base = Path(pipeline).stem
    elif workspace:
        base = Path(workspace).stem
    else:
        base = "scheduler"
    return f"[basalt][dagster][scheduler][{base}]"


def _get_universe_from_spec(date, spec: str):
    module_name, value = (
        (spec.split("=", 1) + [None])[:2] if "=" in spec else (spec, None)
    )
    module = importlib.import_module(f"basalt.universes.{module_name}")
    if not hasattr(module, "get_universe"):
        raise ValueError(
            f"Universe module {module_name} must define get_universe(date, value)."
        )
    return module.get_universe(date, value)


def _load_universe_override(spec: str) -> Callable:
    """
    Load a universe selector from basalt/universes/<name>.py.

    Spec format: "<module>=<value>" or "<module>".
    Module must expose get_universe(date, value).
    """
    return partial(_get_universe_from_spec, spec=spec)


def _load_yaml_config(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def _extract_user_config(data: dict) -> dict:
    if "USER_CONFIG" in data and isinstance(data["USER_CONFIG"], dict):
        return data["USER_CONFIG"]
    return data


def resolve_user_config(
    user_config: dict,
    module_file: Optional[str],
    precedence: str = "yaml_overrides",
) -> dict:
    if not module_file:
        return user_config
    yaml_path = str(Path(module_file).with_suffix(".yaml"))
    yaml_data = _load_yaml_config(yaml_path)
    if not yaml_data:
        return user_config
    yaml_config = _extract_user_config(yaml_data)
    if precedence == "python_overrides":
        return {**yaml_config, **user_config}
    return {**user_config, **yaml_config}


def _deep_merge(a: dict, b: dict) -> dict:
    out = dict(a)
    for key, value in b.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _load_bmll_job_config(config_file: Optional[str]) -> BMLLJobConfig:
    if not config_file:
        return BMLLJobConfig()
    path = Path(config_file)
    user_config: dict = {}

    if path.suffix == ".py":
        yaml_path = path.with_suffix(".yaml")
        if yaml_path.exists():
            yaml_data = _load_yaml_config(str(yaml_path))
            user_config = _extract_user_config(yaml_data)
    elif path.suffix == ".yaml":
        yaml_data = _load_yaml_config(str(path))
        # Detect dagster demo definitions.yaml format
        if isinstance(yaml_data, dict) and "DEMO_PIPELINE" in yaml_data:
            demo_pipeline = yaml_data.get("DEMO_PIPELINE")
            overrides = yaml_data.get("PIPELINE_OVERRIDES", {}) or {}
            demo_path = Path(demo_pipeline) if demo_pipeline else None
            if demo_path and not demo_path.is_absolute():
                demo_path = (path.parent / demo_path).resolve()
            if demo_path and demo_path.exists():
                demo_yaml = demo_path.with_suffix(".yaml")
                demo_data = _load_yaml_config(str(demo_yaml)) if demo_yaml.exists() else {}
                user_config = _extract_user_config(demo_data)
                stem = demo_path.stem
                override_cfg = overrides.get(str(demo_pipeline)) or overrides.get(stem) or {}
                if override_cfg:
                    user_config = _deep_merge(user_config, override_cfg)
        else:
            user_config = _extract_user_config(yaml_data)

    if not user_config:
        return BMLLJobConfig()
    try:
        return AnalyticsConfig(**user_config).BMLL_JOBS
    except Exception:
        return BMLLJobConfig()


def _format_cli_args(args: dict) -> list[str]:
    formatted = []
    for key, value in args.items():
        if value is None:
            continue
        if isinstance(value, bool):
            if value:
                formatted.append(f"--{key}=True")
            else:
                formatted.append(f"--{key}=False")
        else:
            formatted.append(f"--{key}={value}")
    return formatted


def _write_basalt_run_script(job_config: BMLLJobConfig, args: list[str]) -> str:
    jobs_dir = Path(job_config.jobs_dir)
    jobs_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    script_path = jobs_dir / f"basalt_run_{stamp}.sh"
    cmd = " ".join(["python", "-m", "basalt.basalt", "run", *args])
    script_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                cmd,
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(script_path, 0o755)
    return str(script_path)


def _write_dagster_scheduler_script(
    job_config: BMLLJobConfig,
    *,
    pipeline: Optional[str],
    workspace: Optional[str],
    dagster_home: Optional[str],
    runtime_hours: int,
) -> str:
    jobs_dir = Path(job_config.jobs_dir)
    jobs_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    script_path = jobs_dir / f"basalt_dagster_scheduler_{stamp}.py"
    cmd = ["dagster-daemon", "run"]
    if workspace:
        cmd.extend(["-w", workspace])
    elif pipeline:
        cmd.extend(["-f", pipeline])
    else:
        raise ValueError("pipeline or workspace is required for dagster scheduler.")
    timeout_seconds = int(max(runtime_hours, 1) * 3600)
    script_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import os",
                "import subprocess",
                "import sys",
                "",
                f"cmd = {cmd!r}",
                f"timeout = {timeout_seconds}",
                "",
                "env = os.environ.copy()",
                f"dagster_home = {repr(dagster_home)}",
                "if dagster_home:",
                "    env['DAGSTER_HOME'] = dagster_home",
                "try:",
                "    subprocess.run(cmd, check=True, env=env, timeout=timeout)",
                "except subprocess.TimeoutExpired:",
                "    sys.exit(0)",
                "except subprocess.CalledProcessError as exc:",
                "    sys.stderr.write(str(exc) + \"\\n\")",
                "    sys.exit(exc.returncode or 1)",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(script_path, 0o755)
    return str(script_path)


def _write_scheduler_metadata(job_config: BMLLJobConfig, name: str, payload: dict) -> str:
    jobs_dir = Path(job_config.jobs_dir)
    jobs_dir.mkdir(parents=True, exist_ok=True)
    meta_path = jobs_dir / f"dagster_scheduler_{name}.json"
    meta_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return str(meta_path)


def _load_scheduler_metadata(job_config: BMLLJobConfig, name: str) -> Optional[dict]:
    jobs_dir = Path(job_config.jobs_dir)
    meta_path = jobs_dir / f"dagster_scheduler_{name}.json"
    if not meta_path.exists():
        return None
    return _load_yaml_config(str(meta_path))


def dagster_scheduler_install(
    *,
    pipeline: Optional[str] = None,
    workspace: Optional[str] = None,
    name: Optional[str] = None,
    interval_hours: int = 4,
    instance_size: Optional[int] = 16,
    conda_env: Optional[str] = None,
    dagster_home: Optional[str] = None,
    max_runtime_hours: int = 1,
    cron_timezone: Optional[str] = None,
    overwrite_existing: bool = False,
):
    """
    Install a cron-triggered BMLL job that runs the Dagster daemon periodically.
    """
    job_config = _load_bmll_job_config(pipeline or workspace)
    scheduler_name = _default_basalt_scheduler_name(pipeline, workspace, name)
    dow = getattr(job_config, "scheduler_days", "MON-SAT") or "MON-SAT"
    if interval_hours == 24:
        cron = f"0 0 ? * {dow} *"
    else:
        cron = f"0 */{interval_hours} ? * {dow} *"
    script_path = _write_dagster_scheduler_script(
        job_config,
        pipeline=pipeline,
        workspace=workspace,
        dagster_home=dagster_home,
        runtime_hours=max_runtime_hours,
    )
    job = submit_instance_job(
        script_path,
        name=scheduler_name,
        instance_size=instance_size or job_config.default_instance_size,
        conda_env=conda_env,
        cron=cron,
        cron_timezone=cron_timezone,
        max_runtime_hours=max_runtime_hours,
        delete_after=False,
        config=job_config,
        on_name_conflict="overwrite" if overwrite_existing else "fail",
    )
    payload = {
        "job_id": getattr(job, "id", None),
        "name": scheduler_name,
        "cron": cron,
        "cron_timezone": cron_timezone,
        "pipeline": pipeline,
        "workspace": workspace,
        "instance_size": instance_size,
        "conda_env": conda_env or job_config.default_conda_env,
        "dagster_home": dagster_home,
        "max_runtime_hours": max_runtime_hours,
    }
    _write_scheduler_metadata(job_config, scheduler_name, payload)
    return payload


def dagster_scheduler_uninstall(
    *,
    name: Optional[str] = None,
    pipeline: Optional[str] = None,
    workspace: Optional[str] = None,
):
    job_config = _load_bmll_job_config(pipeline or workspace)
    scheduler_name = _default_basalt_scheduler_name(pipeline, workspace, name)
    meta = _load_scheduler_metadata(job_config, scheduler_name) or {}
    job_id = meta.get("job_id")
    try:
        from bmll import compute

        job = None
        if job_id and hasattr(compute, "get_job"):
            job = compute.get_job(job_id)
        if job is None and hasattr(compute, "get_jobs"):
            jobs = compute.get_jobs()
            for candidate in jobs:
                if getattr(candidate, "name", None) == scheduler_name:
                    job = candidate
                    break
        if job is not None and hasattr(job, "delete"):
            job.delete()
    except Exception:
        pass


def bmll_job_run(
    *,
    config_file: Optional[str] = None,
    name: Optional[str] = None,
    instance_size: Optional[int] = None,
    conda_env: Optional[str] = None,
    delete_after: Optional[bool] = None,
    cron: Optional[str] = None,
    cron_timezone: Optional[str] = None,
    overwrite_existing: bool = False,
    **kwargs,
):
    """
    Submit a remote BMLL job that runs the standard CLI (`basalt run`).
    Requires --config_file (or use `basalt job run --pipeline ...`).
    """
    if not config_file:
        raise SystemExit("Provide --config_file (or use basalt job run --pipeline ...).")
    job_config = _load_bmll_job_config(config_file)
    if not job_config.enabled:
        logging.warning("BMLL_JOBS.enabled is False; job will still be submitted.")
    cli_args = {}
    if config_file:
        cli_args["config_file"] = config_file
    cli_args.update(kwargs)
    allowed = set(inspect.signature(run_cli).parameters.keys())
    cli_args = {k: v for k, v in cli_args.items() if k in allowed}
    script_path = _write_basalt_run_script(job_config, _format_cli_args(cli_args))
    job_name = _default_basalt_job_name(config_file, name)
    return submit_instance_job(
        script_path,
        name=job_name,
        instance_size=instance_size,
        conda_env=conda_env,
        cron=cron,
        cron_timezone=cron_timezone,
        delete_after=delete_after,
        config=job_config,
        on_name_conflict="overwrite" if overwrite_existing else "fail",
    )


def bmll_job_install(
    *,
    config_file: Optional[str] = None,
    name: Optional[str] = None,
    instance_size: Optional[int] = None,
    conda_env: Optional[str] = None,
    cron: Optional[str] = None,
    cron_timezone: Optional[str] = None,
    overwrite_existing: bool = False,
    **kwargs,
):
    """
    Install a cron-triggered BMLL job that runs the standard CLI (`basalt run`).
    """
    return bmll_job_run(
        config_file=config_file,
        name=name,
        instance_size=instance_size,
        conda_env=conda_env,
        delete_after=False,
        cron=cron,
        cron_timezone=cron_timezone,
        overwrite_existing=overwrite_existing,
        **kwargs,
    )


def dagster_sync(
    *,
    tables: str | None = "all",
    start_date: str | None = None,
    end_date: str | None = None,
    paths: str | None = None,
    emit_observations: bool = True,
    emit_materializations: bool = True,
    force_refresh: bool = False,
    batch_size: int = 500,
    run_id: str | None = None,
    job_name: str | None = None,
    seed_with_api: bool = False,
    asset_key_prefix: str | None = "BMLL",
    refresh_status_cache: bool = True,
):
    """
    Run the bulk S3 -> Dagster event sync (direct DB insertion).
    """
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "basalt" / "dagster" / "scripts" / "s3_bulk_sync_db.py"
    if not script_path.exists():
        raise FileNotFoundError(f"Bulk sync script not found: {script_path}")
    cmd = [
        sys.executable,
        str(script_path),
        "sync",
        "--tables",
        tables or "all",
    ]
    if start_date:
        cmd.extend(["--start_date", start_date])
    if end_date:
        cmd.extend(["--end_date", end_date])
    if paths:
        cmd.extend(["--paths", paths])
    cmd.extend(["--emit_observations", str(bool(emit_observations))])
    cmd.extend(["--emit_materializations", str(bool(emit_materializations))])
    cmd.extend(["--force_refresh", str(bool(force_refresh))])
    cmd.extend(["--batch_size", str(batch_size)])
    if run_id:
        cmd.extend(["--run_id", run_id])
    if job_name:
        cmd.extend(["--job_name", job_name])
    cmd.extend(["--seed_with_api", str(bool(seed_with_api))])
    if asset_key_prefix is not None:
        cmd.extend(["--asset_key_prefix", asset_key_prefix])
    cmd.extend(["--refresh_status_cache", str(bool(refresh_status_cache))])
    subprocess.check_call(cmd)


def run_cli(
    user_config: dict,
    default_get_universe: Callable,
    get_pipeline: Optional[Callable] = None,
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    datasetname: Optional[str] = None,
    temp_dir: Optional[str] = None,
    prepare_data_mode: Optional[str] = None,
    batch_freq: Optional[str] = None,
    num_workers: Optional[int] = None,
    eager_execution: bool = False,
    universe: Optional[str] = None,
    config_file: Optional[str] = None,
    config_precedence: str = "yaml_overrides",
    user_config_json: Optional[str] = None,
) -> None:
    """
    Run a standard CLI flow using a base USER_CONFIG and a default universe.
    """
    if user_config_json:
        payload = json.loads(user_config_json)
        if not isinstance(payload, dict):
            raise ValueError("user_config_json must decode to a JSON object.")
        user_config = payload
    else:
        user_config = resolve_user_config(user_config, config_file, config_precedence)
    config_overrides = {}
    if date:
        config_overrides["START_DATE"] = date
        config_overrides["END_DATE"] = date
    if start_date:
        config_overrides["START_DATE"] = start_date
    if end_date:
        config_overrides["END_DATE"] = end_date
    if datasetname:
        config_overrides["DATASETNAME"] = datasetname
    if universe:
        config_overrides["UNIVERSE"] = universe
    if temp_dir:
        config_overrides["TEMP_DIR"] = temp_dir
    if prepare_data_mode:
        config_overrides["PREPARE_DATA_MODE"] = prepare_data_mode
    if batch_freq is not None:
        config_overrides["BATCH_FREQ"] = batch_freq
    if num_workers is not None:
        config_overrides["NUM_WORKERS"] = num_workers
    if eager_execution:
        config_overrides["EAGER_EXECUTION"] = True

    config = AnalyticsConfig(**{**user_config, **config_overrides})

    get_universe = default_get_universe
    if universe:
        get_universe = _load_universe_override(universe)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    run_multiday_pipeline(
        config=config,
        get_universe=get_universe,
        get_pipeline=get_pipeline,
    )


def main():
    _disable_fire_pager()
    if len(sys.argv) > 1 and sys.argv[1] == "bmll_job_run":
        sys.argv.pop(1)
        fire.Fire(bmll_job_run)
    elif len(sys.argv) > 1 and sys.argv[1] == "bmll_job_install":
        sys.argv.pop(1)
        fire.Fire(bmll_job_install)
    else:
        fire.Fire(run_cli)
