from __future__ import annotations

import os
import shutil
import time
from pathlib import Path
from typing import Any, Optional

import cloudpickle

from intraday_analytics.configuration import BMLLJobConfig


USER_ROOT = Path("/home/bmll/user")
ORG_ROOT = Path("/home/bmll/organisation")


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _relativize_to_area(path: Path) -> tuple[str, str]:
    if path.is_absolute():
        if USER_ROOT in path.parents or path == USER_ROOT:
            return "user", str(path.relative_to(USER_ROOT))
        if ORG_ROOT in path.parents or path == ORG_ROOT:
            return "organisation", str(path.relative_to(ORG_ROOT))
    return "user", str(path)


def _default_bootstrap_content(project_root: Path, pythonpath: list[str]) -> str:
    pythonpath_entries = [str(project_root)] + pythonpath
    joined = ":".join(pythonpath_entries) + ":${PYTHONPATH}"
    return "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            f"export PYTHONPATH=\"{joined}\"",
            f"cd {project_root}",
            "if [ -f requirements.txt ]; then",
            "  python -m pip install -r requirements.txt",
            "fi",
        ]
    )


def convert_dagster_cron_to_bmll(expr: str) -> str:
    parts = [p for p in expr.strip().split() if p]
    if len(parts) == 6:
        return expr
    if len(parts) != 5:
        return expr
    minute, hour, dom, month, dow = parts
    year = "*"
    if dom != "*" and dow != "*":
        dow = "?"
    elif dom == "*" and dow == "*":
        dom = "?"
    elif dom == "*" and dow != "*":
        dom = "?"
    elif dom != "*" and dow == "*":
        dow = "?"
    return " ".join([minute, hour, dom, month, dow, year])


def ensure_default_bootstrap(config: BMLLJobConfig) -> tuple[str, str, list[Any]]:
    if config.default_bootstrap:
        area, rel_path = _relativize_to_area(Path(config.default_bootstrap))
        return area, rel_path, list(config.default_bootstrap_args or [])

    jobs_dir = Path(config.jobs_dir)
    _ensure_dir(jobs_dir)
    bootstrap_path = jobs_dir / "bootstrap.sh"
    if not bootstrap_path.exists():
        content = _default_bootstrap_content(
            Path(config.project_root), list(config.pythonpath_prefixes or [])
        )
        bootstrap_path.write_text(content, encoding="utf-8")
        os.chmod(bootstrap_path, 0o755)

    area, rel_path = _relativize_to_area(bootstrap_path)
    return area, rel_path, list(config.default_bootstrap_args or [])


def _copy_script_to_jobs_dir(script_path: Path, jobs_dir: Path) -> Path:
    _ensure_dir(jobs_dir)
    name = script_path.name
    stamp = time.strftime("%Y%m%d_%H%M%S")
    dest = jobs_dir / f"{script_path.stem}_{stamp}{script_path.suffix}"
    shutil.copy2(script_path, dest)
    return dest


def _prepare_script_path(script_path: str, config: BMLLJobConfig) -> tuple[str, str]:
    path = Path(script_path)
    if not path.is_absolute():
        # treat as relative to jobs dir
        path = Path(config.jobs_dir) / path
    if not path.exists():
        raise FileNotFoundError(f"Script not found: {path}")

    area, rel_path = _relativize_to_area(path)
    if area in {"user", "organisation"}:
        return area, rel_path

    dest = _copy_script_to_jobs_dir(path, Path(config.jobs_dir))
    area, rel_path = _relativize_to_area(dest)
    return area, rel_path


def _check_concurrency(config: BMLLJobConfig) -> None:
    if not config.max_concurrent_instances:
        return
    try:
        from bmll import compute

        runs = compute.get_job_runs(state="active")
        if hasattr(runs, "__len__") and len(runs) >= config.max_concurrent_instances:
            raise RuntimeError(
                f"Active job runs ({len(runs)}) exceed max_concurrent_instances="
                f"{config.max_concurrent_instances}"
            )
    except Exception:
        return


def submit_instance_job(
    script_path: str,
    *,
    name: Optional[str] = None,
    instance_size: Optional[int] = None,
    conda_env: Optional[str] = None,
    max_runtime_hours: Optional[int] = None,
    cron: Optional[str] = None,
    cron_timezone: Optional[str] = None,
    delete_after: Optional[bool] = None,
    script_parameters: Optional[dict] = None,
    config: Optional[BMLLJobConfig] = None,
):
    config = config or BMLLJobConfig()
    _check_concurrency(config)

    area, rel_path = _prepare_script_path(script_path, config)
    bootstrap_area, bootstrap_path, bootstrap_args = ensure_default_bootstrap(config)

    from bmll.compute import Bootstrap, CronTrigger, JobTrigger, create_job

    bootstraps = [Bootstrap(area=bootstrap_area, path=bootstrap_path, args=bootstrap_args)]

    triggers = None
    if cron:
        cron_expr = cron
        if config.cron_format == "dagster":
            cron_expr = convert_dagster_cron_to_bmll(cron)
        trigger = CronTrigger(cron_expr)
        name_suffix = f"_{cron_timezone}" if cron_timezone else ""
        triggers = [JobTrigger(name=f"{name or 'bmll_job'}_cron{name_suffix}", trigger=trigger)]

    job = create_job(
        compute_type="instance",
        name=name,
        instance_size=instance_size or config.default_instance_size,
        max_runtime_hours=max_runtime_hours or config.max_runtime_hours,
        script_area=area,
        script_path=rel_path,
        conda_env=conda_env or config.default_conda_env,
        log_area=config.log_area,
        log_path=config.log_path,
        visibility=config.visibility,
        bootstraps=bootstraps,
        triggers=triggers,
        **(script_parameters or {}),
    )

    if job and hasattr(job, "id"):
        desired_log_path = (
            f"intraday-metrics/_bmll_jobs/{job.id}/logs"
            if config.log_path == "job_run_logs"
            else config.log_path
        )
        try:
            job.update(log_path=desired_log_path, log_area=config.log_area)
        except Exception:
            pass

    if cron:
        return job

    run = job.execute()
    if delete_after if delete_after is not None else config.delete_job_after:
        job.delete()
    return job, run


def submit_function_job(
    func,
    *args,
    name: Optional[str] = None,
    instance_size: Optional[int] = None,
    conda_env: Optional[str] = None,
    delete_after: Optional[bool] = None,
    config: Optional[BMLLJobConfig] = None,
    **kwargs,
):
    config = config or BMLLJobConfig()
    jobs_dir = Path(config.jobs_dir)
    _ensure_dir(jobs_dir)

    stamp = time.strftime("%Y%m%d_%H%M%S")
    payload_path = jobs_dir / f"payload_{stamp}.pkl"
    result_path = jobs_dir / f"result_{stamp}.pkl"
    runner_path = jobs_dir / f"run_payload_{stamp}.py"

    payload_path.write_bytes(cloudpickle.dumps((func, args, kwargs, str(result_path))))

    runner_code = "\n".join(
        [
            "import cloudpickle",
            "from pathlib import Path",
            "",
            f"payload_path = Path({payload_path!r})",
            "func, args, kwargs, result_path = cloudpickle.loads(payload_path.read_bytes())",
            "result = func(*args, **kwargs)",
            "Path(result_path).write_bytes(cloudpickle.dumps(result))",
        ]
    )
    runner_path.write_text(runner_code, encoding="utf-8")

    return submit_instance_job(
        str(runner_path),
        name=name or getattr(func, "__name__", "bmll_function_job"),
        instance_size=instance_size,
        conda_env=conda_env,
        delete_after=delete_after,
        config=config,
    )


def bmll_job(
    *,
    memory_gb: Optional[int] = None,
    delete_after: bool = True,
    config: Optional[BMLLJobConfig] = None,
    **job_kwargs,
):
    def decorator(func):
        def wrapper(*args, **kwargs):
            return submit_function_job(
                func,
                *args,
                instance_size=memory_gb,
                delete_after=delete_after,
                config=config,
                **job_kwargs,
                **kwargs,
            )

        return wrapper

    return decorator
