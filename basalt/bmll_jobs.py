from __future__ import annotations

# Backward-compatible import path. Prefer `basalt.executors.aws_ec2`.
try:
    from basalt.executors.aws_ec2 import *  # noqa: F401,F403
except Exception as exc:  # pragma: no cover - only used when executor package is absent
    _IMPORT_ERROR = exc

    def _raise_missing(*args, **kwargs):
        raise RuntimeError(
            "AWS EC2 executor is not installed. Install bmll-basalt-aws-ec2."
        ) from _IMPORT_ERROR

    submit_instance_job = _raise_missing
    submit_function_job = _raise_missing
    convert_dagster_cron_to_bmll = _raise_missing

    def bmll_job(*args, **kwargs):
        _raise_missing()
