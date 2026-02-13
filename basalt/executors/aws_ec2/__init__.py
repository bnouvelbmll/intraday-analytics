from __future__ import annotations

from .backend import EC2RunConfig, ec2_install, ec2_run

from .cli_ext import (
    get_cli_extension,
)

__all__ = [
    "EC2RunConfig",
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
