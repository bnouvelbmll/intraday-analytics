from __future__ import annotations

import inspect
import logging
from pathlib import Path
from typing import Optional

from basalt.cli import (
    _format_cli_args,
    _load_bmll_job_config,
    _default_basalt_job_name,
    _write_basalt_run_script,
    run_cli,
)
from .backend import submit_instance_job


def ec2_run(
    *,
    config_file: Optional[str] = None,
    name: Optional[str] = None,
    instance_size: Optional[int] = None,
    conda_env: Optional[str] = None,
    delete_after: Optional[bool] = None,
    cron: Optional[str] = None,
    cron_timezone: Optional[str] = None,
    overwrite_existing: bool = False,
    pipeline: Optional[str] = None,
    **kwargs,
):
    """
    Submit an AWS EC2-backed BMLL job that runs the standard basalt CLI.
    """
    if pipeline and not config_file:
        config_file = pipeline
    if not config_file:
        raise SystemExit("Provide --config_file (or --pipeline).")

    job_config = _load_bmll_job_config(config_file)
    if not job_config.enabled:
        logging.warning("BMLL_JOBS.enabled is False; job will still be submitted.")

    cli_args = {"config_file": config_file, **kwargs}
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


def ec2_install(
    *,
    config_file: Optional[str] = None,
    pipeline: Optional[str] = None,
    name: Optional[str] = None,
    instance_size: Optional[int] = None,
    conda_env: Optional[str] = None,
    cron: Optional[str] = None,
    cron_timezone: Optional[str] = None,
    overwrite_existing: bool = False,
    **kwargs,
):
    """
    Install a cron-triggered AWS EC2-backed BMLL job.
    """
    return ec2_run(
        config_file=config_file,
        pipeline=pipeline,
        name=name,
        instance_size=instance_size,
        conda_env=conda_env,
        delete_after=False,
        cron=cron,
        cron_timezone=cron_timezone,
        overwrite_existing=overwrite_existing,
        **kwargs,
    )


class EC2CLI:
    @staticmethod
    def run(*args, **kwargs):
        return ec2_run(*args, **kwargs)

    @staticmethod
    def install(*args, **kwargs):
        return ec2_install(*args, **kwargs)


def get_cli_extension():
    return {"ec2": EC2CLI}

