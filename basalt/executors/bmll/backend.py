from __future__ import annotations

import os
import shutil
import time
import logging
import re
from pathlib import Path
from typing import Any, Optional

import cloudpickle

from basalt.configuration import BMLLJobConfig


USER_ROOT = Path("/home/bmll/user")
ORG_ROOT = Path("/home/bmll/organisation")
logger = logging.getLogger(__name__)


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
            "echo \"[basalt bootstrap] start $(date -Iseconds)\"",
            "echo \"[basalt bootstrap] pwd=$(pwd) user=$(id -un)\"",
            "echo \"[basalt bootstrap] conda_prefix=${CONDA_PREFIX:-<unset>} env=${CONDA_DEFAULT_ENV:-<unset>}\"",
            "echo \"[basalt bootstrap] python=$(command -v python || true)\"",
            "echo \"[basalt bootstrap] python version: $(python --version 2>&1 || true)\"",
            "echo \"[basalt bootstrap] mounted dirs (findmnt /home /home/bmll /home/bmll/user):\"",
            "findmnt /home /home/bmll /home/bmll/user 2>/dev/null || true",
            "echo \"[basalt bootstrap] mount output (filtered for bmll/home):\"",
            "mount | grep -E '/home|bmll' || true",
            "echo \"[basalt bootstrap] /home listing:\"",
            "ls -la /home 2>/dev/null || true",
            "echo \"[basalt bootstrap] /home/bmll listing:\"",
            "ls -la /home/bmll 2>/dev/null || true",
            "echo \"[basalt bootstrap] /home/bmll/user listing:\"",
            "ls -la /home/bmll/user 2>/dev/null || true",
            f"export PYTHONPATH=\"{joined}\"",
            f"PROJECT_ROOT=\"{project_root}\"",
            "if [ -d \"$PROJECT_ROOT\" ]; then",
            "  cd \"$PROJECT_ROOT\"",
            "else",
            "  echo \"[basalt bootstrap] PROJECT_ROOT not found: $PROJECT_ROOT\" >&2",
            "  BASALT_MOD_DIR=\"$(python -c \"import os, importlib.util; spec=importlib.util.find_spec('basalt'); print(os.path.dirname(spec.origin) if spec and spec.origin else '')\" 2>/dev/null || true)\"",
            "  BASALT_ROOT=\"$(dirname \"$BASALT_MOD_DIR\")\"",
            "  if [ -n \"$BASALT_MOD_DIR\" ] && [ -d \"$BASALT_ROOT\" ]; then",
            "    cd \"$BASALT_ROOT\"",
            "  else",
            "    cd \"$HOME\"",
            "  fi",
            "fi",
            "echo \"[basalt bootstrap] selected workdir=$(pwd)\"",
            "if [ -f setup.py ]; then",
            "  # Install local basalt checkout (core + discovered local plugins) in the active conda env.",
            "  BASALT_DIST=all python -m pip install -e .",
            "fi",
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


def _validate_bmll_cron(expr: str) -> tuple[bool, str]:
    parts = [p for p in expr.strip().split() if p]
    if len(parts) != 6:
        return False, f"Expected 6 fields, got {len(parts)}"

    def _match(pattern: str, value: str) -> bool:
        return re.fullmatch(pattern, value) is not None

    def _num_or_star(value: str, lo: int, hi: int) -> bool:
        if value == "*":
            return True
        if _match(r"0/\d+", value):
            step = int(value.split("/", 1)[1])
            return step > 0
        if _match(r"\d+", value):
            num = int(value)
            return lo <= num <= hi
        if _match(r"\d+-\d+", value):
            a, b = [int(x) for x in value.split("-", 1)]
            return lo <= a <= hi and lo <= b <= hi and a <= b
        if _match(r"\d+(,\d+)+", value):
            nums = [int(x) for x in value.split(",")]
            return all(lo <= n <= hi for n in nums)
        return False

    minute, hour, dom, month, dow, year = parts
    if not _num_or_star(minute, 0, 59):
        return False, f"Invalid minute field: {minute}"
    if not _num_or_star(hour, 0, 23):
        return False, f"Invalid hour field: {hour}"
    if dom not in {"*", "?"} and not _num_or_star(dom, 1, 31):
        return False, f"Invalid day-of-month field: {dom}"
    if not _num_or_star(month, 1, 12):
        return False, f"Invalid month field: {month}"
    if dow in {"*", "?"}:
        pass
    elif _match(r"(SUN|MON|TUE|WED|THU|FRI|SAT)(-(SUN|MON|TUE|WED|THU|FRI|SAT))?", dow):
        pass
    elif _match(r"(SUN|MON|TUE|WED|THU|FRI|SAT)(,(SUN|MON|TUE|WED|THU|FRI|SAT))+", dow):
        pass
    elif not _num_or_star(dow, 1, 7):
        return False, f"Invalid day-of-week field: {dow}"
    if year != "*" and not _num_or_star(year, 1970, 2199):
        return False, f"Invalid year field: {year}"
    if dom != "?" and dow != "?":
        return False, "Day-of-month and day-of-week cannot both be specified; use '?' for one."
    return True, "ok"


def _expand_cron_without_slash(expr: str) -> list[str]:
    """
    Expand simple cron expressions that use step syntax (e.g. "*/6") into
    multiple cron expressions without "/" which BMLL cron does not accept.
    Only supports step values on minute or hour fields when other fields
    are wildcards or compatible with the BMLL format.
    """
    parts = [p for p in expr.strip().split() if p]
    if len(parts) != 6:
        return [expr]
    minute, hour, dom, month, dow, year = parts

    def _expand_step(field: str, lo: int, hi: int) -> list[str] | None:
        if field.startswith("*/"):
            try:
                step = int(field.split("/", 1)[1])
            except Exception:
                return None
            if step <= 0:
                return None
            return [str(v) for v in range(lo, hi + 1, step)]
        return None

    minute_vals = _expand_step(minute, 0, 59)
    hour_vals = _expand_step(hour, 0, 23)

    if minute_vals and hour_vals:
        # Too many triggers; not supported for now.
        return [expr]

    if minute_vals:
        return [
            f"{m} {hour} {dom} {month} {dow} {year}"
            for m in minute_vals
        ]

    if hour_vals:
        return [
            f"{minute} {h} {dom} {month} {dow} {year}"
            for h in hour_vals
        ]

    return [expr]


def ensure_default_bootstrap(config: BMLLJobConfig) -> tuple[str, str, list[Any]]:
    if config.default_bootstrap:
        area, rel_path = _relativize_to_area(Path(config.default_bootstrap))
        return area, rel_path, list(config.default_bootstrap_args or [])

    jobs_dir = Path(config.jobs_dir)
    _ensure_dir(jobs_dir)
    bootstrap_path = jobs_dir / "bootstrap.sh"
    should_rewrite = not bootstrap_path.exists()
    if bootstrap_path.exists():
        try:
            current = bootstrap_path.read_text(encoding="utf-8")
            if (
                "/home/bmll/user/basalt" in current
                or "BASALT_DIST=all python -m pip install -e ." not in current
            ):
                should_rewrite = True
        except Exception:
            should_rewrite = True

    if should_rewrite:
        content = _default_bootstrap_content(
            Path(config.project_root), list(config.pythonpath_prefixes or [])
        )
        bootstrap_path.write_text(content, encoding="utf-8")
        os.chmod(bootstrap_path, 0o755)

    area, rel_path = _relativize_to_area(bootstrap_path)
    return area, rel_path, list(config.default_bootstrap_args or [])


def _copy_script_to_jobs_dir(script_path: Path, jobs_dir: Path) -> Path:
    _ensure_dir(jobs_dir)
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
    on_name_conflict: str = "fail",
):
    config = config or BMLLJobConfig()
    _check_concurrency(config)

    area, rel_path = _prepare_script_path(script_path, config)
    bootstrap_area, bootstrap_path, bootstrap_args = ensure_default_bootstrap(config)

    from bmll.compute import Bootstrap, CronTrigger, JobTrigger, create_job

    if name:
        try:
            from bmll import compute

            existing = None
            if hasattr(compute, "get_jobs"):
                for candidate in compute.get_jobs():
                    if getattr(candidate, "name", None) == name:
                        existing = candidate
                        break
            if existing is not None:
                if on_name_conflict == "overwrite":
                    try:
                        existing.delete()
                    except Exception:
                        pass
                else:
                    raise RuntimeError(f"BMLL job with name '{name}' already exists.")
        except RuntimeError:
            raise
        except Exception:
            pass

    bootstraps = [Bootstrap(area=bootstrap_area, path=bootstrap_path, args=bootstrap_args)]

    triggers = None
    if cron:
        cron_expr = cron
        if config.cron_format == "dagster":
            cron_expr = convert_dagster_cron_to_bmll(cron)
        expanded = _expand_cron_without_slash(cron_expr)
        triggers = []
        name_suffix = f"_{cron_timezone}" if cron_timezone else ""
        for idx, expr in enumerate(expanded):
            ok, reason = _validate_bmll_cron(expr)
            if not ok:
                raise ValueError(
                    f"Invalid BMLL cron expression '{expr}': {reason}"
                )
            trigger = CronTrigger(expr)
            suffix = f"_{idx}" if len(expanded) > 1 else ""
            triggers.append(
                JobTrigger(
                    name=f"{name or 'bmll_job'}_cron{name_suffix}{suffix}",
                    trigger=trigger,
                )
            )

    payload = dict(
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
    try:
        job = create_job(**payload)
    except Exception as exc:
        logger.error(
            "BMLL create_job failed. payload=%s error=%s",
            payload,
            exc,
            exc_info=True,
        )
        raise RuntimeError(f"Failed to create BMLL job: {exc}") from exc

    if job and hasattr(job, "id"):
        desired_log_path = (
            f"basalt/_bmll_jobs/{job.id}/logs"
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
