from __future__ import annotations

from .backend import (
    bmll_job,
    submit_function_job,
    submit_instance_job,
    convert_dagster_cron_to_bmll,
)

from .cli_ext import (
    ec2_run,
    ec2_install,
    get_cli_extension,
)

__all__ = [
    "bmll_job",
    "submit_function_job",
    "submit_instance_job",
    "convert_dagster_cron_to_bmll",
    "ec2_run",
    "ec2_install",
    "get_cli_extension",
]


def get_basalt_plugin():
    return {
        "name": "aws_ec2",
        "provides": ["aws ec2 executor"],
        "cli_extensions": ["ec2"],
    }
