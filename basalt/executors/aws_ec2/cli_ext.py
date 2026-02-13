from __future__ import annotations

from typing import Optional

from .backend import ec2_install as _ec2_install_backend
from .backend import ec2_run as _ec2_run_backend


def ec2_run(
    *,
    pipeline: Optional[str] = None,
    config_file: Optional[str] = None,
    name: Optional[str] = None,
    instance_size: Optional[int] = None,
    conda_env: Optional[str] = None,
    user_config_json: Optional[str] = None,
    ami_id: Optional[str] = None,
    instance_type: Optional[str] = None,
    subnet_id: Optional[str] = None,
    security_group_ids: Optional[str | list[str]] = None,
    key_name: Optional[str] = None,
    iam_instance_profile: Optional[str] = None,
    region: Optional[str] = None,
    assign_public_ip: Optional[bool] = None,
    dry_run: Optional[bool] = None,
    root_volume_size_gb: Optional[int] = None,
    user_data: Optional[str] = None,
    **kwargs
):
    """
    Launch a standalone EC2 instance using boto3 and an AMI image.
    """
    return _ec2_run_backend(
        pipeline=pipeline,
        config_file=config_file,
        name=name,
        instance_size=instance_size,
        conda_env=conda_env,
        user_config_json=user_config_json,
        ami_id=ami_id,
        instance_type=instance_type,
        subnet_id=subnet_id,
        security_group_ids=security_group_ids,
        key_name=key_name,
        iam_instance_profile=iam_instance_profile,
        region=region,
        assign_public_ip=assign_public_ip,
        dry_run=dry_run,
        root_volume_size_gb=root_volume_size_gb,
        user_data=user_data,
        **kwargs,
    )


def ec2_install(
    *,
    pipeline: Optional[str] = None,
    config_file: Optional[str] = None,
    name: Optional[str] = None,
    instance_size: Optional[int] = None,
    conda_env: Optional[str] = None,
    user_config_json: Optional[str] = None,
    ami_id: Optional[str] = None,
    instance_type: Optional[str] = None,
    subnet_id: Optional[str] = None,
    security_group_ids: Optional[str | list[str]] = None,
    key_name: Optional[str] = None,
    iam_instance_profile: Optional[str] = None,
    region: Optional[str] = None,
    assign_public_ip: Optional[bool] = None,
    dry_run: Optional[bool] = None,
    root_volume_size_gb: Optional[int] = None,
    user_data: Optional[str] = None,
    **kwargs,
):
    """
    Install semantics for EC2 map to launching a run.
    """
    return _ec2_install_backend(
        pipeline=pipeline,
        config_file=config_file,
        name=name,
        instance_size=instance_size,
        conda_env=conda_env,
        user_config_json=user_config_json,
        ami_id=ami_id,
        instance_type=instance_type,
        subnet_id=subnet_id,
        security_group_ids=security_group_ids,
        key_name=key_name,
        iam_instance_profile=iam_instance_profile,
        region=region,
        assign_public_ip=assign_public_ip,
        dry_run=dry_run,
        root_volume_size_gb=root_volume_size_gb,
        user_data=user_data,
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
