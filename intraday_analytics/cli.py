import importlib
import logging
import os
import time
from pathlib import Path
from functools import partial
from typing import Callable, Optional
import inspect

import fire
import yaml

from intraday_analytics.configuration import AnalyticsConfig, BMLLJobConfig
from intraday_analytics.bmll_jobs import submit_instance_job
from intraday_analytics.execution import run_multiday_pipeline


def _get_universe_from_spec(date, spec: str):
    module_name, value = (
        (spec.split("=", 1) + [None])[:2] if "=" in spec else (spec, None)
    )
    module = importlib.import_module(f"intraday_analytics.universes.{module_name}")
    if not hasattr(module, "get_universe"):
        raise ValueError(
            f"Universe module {module_name} must define get_universe(date, value)."
        )
    return module.get_universe(date, value)


def _load_universe_override(spec: str) -> Callable:
    """
    Load a universe selector from intraday_analytics/universes/<name>.py.

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


def _load_bmll_job_config(config_file: Optional[str]) -> BMLLJobConfig:
    if not config_file:
        return BMLLJobConfig()
    yaml_data = _load_yaml_config(config_file)
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


def _write_beaf_run_script(job_config: BMLLJobConfig, args: list[str]) -> str:
    jobs_dir = Path(job_config.jobs_dir)
    jobs_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    script_path = jobs_dir / f"beaf_run_{stamp}.sh"
    cmd = " ".join(["python", "-m", "intraday_analytics.beaf", "run", *args])
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


def bmll_job_run(
    *,
    config_file: Optional[str] = None,
    name: Optional[str] = None,
    instance_size: Optional[int] = None,
    conda_env: Optional[str] = None,
    delete_after: Optional[bool] = None,
    cron: Optional[str] = None,
    cron_timezone: Optional[str] = None,
    **kwargs,
):
    """
    Submit a remote BMLL job that runs the standard CLI (`beaf run`).
    """
    job_config = _load_bmll_job_config(config_file)
    if not job_config.enabled:
        logging.warning("BMLL_JOBS.enabled is False; job will still be submitted.")
    cli_args = {}
    if config_file:
        cli_args["config_file"] = config_file
    cli_args.update(kwargs)
    allowed = set(inspect.signature(run_cli).parameters.keys())
    cli_args = {k: v for k, v in cli_args.items() if k in allowed}
    script_path = _write_beaf_run_script(job_config, _format_cli_args(cli_args))
    return submit_instance_job(
        script_path,
        name=name,
        instance_size=instance_size,
        conda_env=conda_env,
        cron=cron,
        cron_timezone=cron_timezone,
        delete_after=delete_after,
        config=job_config,
    )


def bmll_job_install(
    *,
    config_file: Optional[str] = None,
    name: Optional[str] = None,
    instance_size: Optional[int] = None,
    conda_env: Optional[str] = None,
    cron: Optional[str] = None,
    cron_timezone: Optional[str] = None,
    **kwargs,
):
    """
    Install a cron-triggered BMLL job that runs the standard CLI (`beaf run`).
    """
    return bmll_job_run(
        config_file=config_file,
        name=name,
        instance_size=instance_size,
        conda_env=conda_env,
        delete_after=False,
        cron=cron,
        cron_timezone=cron_timezone,
        **kwargs,
    )


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
) -> None:
    """
    Run a standard CLI flow using a base USER_CONFIG and a default universe.
    """
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
    if len(sys.argv) > 1 and sys.argv[1] == "bmll_job_run":
        sys.argv.pop(1)
        fire.Fire(bmll_job_run)
    elif len(sys.argv) > 1 and sys.argv[1] == "bmll_job_install":
        sys.argv.pop(1)
        fire.Fire(bmll_job_install)
    else:
        fire.Fire(run_cli)
