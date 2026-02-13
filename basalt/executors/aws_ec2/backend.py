from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
import json
import os
import shlex
import subprocess
import sys


@dataclass
class EC2RunConfig:
    region: str = "us-east-1"
    ami_id: str | None = None
    instance_type: str = "m7i.2xlarge"
    subnet_id: str | None = None
    security_group_ids: list[str] | None = None
    key_name: str | None = None
    iam_instance_profile: str | None = None
    assign_public_ip: bool = True
    root_volume_size_gb: int = 120
    user_data: str | None = None
    dry_run: bool = False
    instance_name_prefix: str = "basalt-ec2"

    @classmethod
    def from_env(cls) -> "EC2RunConfig":
        sgs = os.environ.get("BASALT_EC2_SECURITY_GROUP_IDS")
        return cls(
            region=os.environ.get("BASALT_EC2_REGION", "us-east-1"),
            ami_id=os.environ.get("BASALT_EC2_AMI_ID"),
            instance_type=os.environ.get("BASALT_EC2_INSTANCE_TYPE", "m7i.2xlarge"),
            subnet_id=os.environ.get("BASALT_EC2_SUBNET_ID"),
            security_group_ids=[x.strip() for x in sgs.split(",")] if sgs else None,
            key_name=os.environ.get("BASALT_EC2_KEY_NAME"),
            iam_instance_profile=os.environ.get("BASALT_EC2_IAM_INSTANCE_PROFILE"),
            assign_public_ip=str(os.environ.get("BASALT_EC2_ASSIGN_PUBLIC_IP", "1")).lower() in {"1", "true", "yes", "on"},
            root_volume_size_gb=int(os.environ.get("BASALT_EC2_ROOT_VOLUME_GB", "120")),
            dry_run=str(os.environ.get("BASALT_EC2_DRY_RUN", "0")).lower() in {"1", "true", "yes", "on"},
            instance_name_prefix=os.environ.get("BASALT_EC2_NAME_PREFIX", "basalt-ec2"),
        )


def _require_boto3():
    try:
        import boto3  # type: ignore
    except Exception as exc:
        raise RuntimeError("boto3 is required for aws_ec2 executor.") from exc
    return boto3


def _map_instance_size(instance_size: int | None, default_instance_type: str) -> str:
    if instance_size is None:
        return default_instance_type
    if instance_size <= 16:
        return "m7i.large"
    if instance_size <= 32:
        return "m7i.xlarge"
    if instance_size <= 64:
        return "m7i.2xlarge"
    if instance_size <= 128:
        return "m7i.4xlarge"
    return "m7i.8xlarge"


def _build_user_data(
    *,
    pipeline: str,
    user_config_json: str | None,
    conda_env: str | None,
) -> str:
    cli_bits = [
        "python",
        "-m",
        "basalt.basalt",
        "pipeline",
        "run",
        "--pipeline",
        pipeline,
    ]
    if user_config_json:
        cli_bits.extend(["--user_config_json", user_config_json])
    cmd = " ".join(shlex.quote(x) for x in cli_bits)
    conda_line = ""
    if conda_env:
        conda_line = (
            "if [ -f \"$HOME/miniconda3/etc/profile.d/conda.sh\" ]; then "
            "source \"$HOME/miniconda3/etc/profile.d/conda.sh\"; "
            f"conda activate {shlex.quote(conda_env)}; "
            "fi"
        )
    return "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "cd /opt/basalt || true",
            conda_line,
            cmd,
        ]
    )


def ec2_run(
    *,
    pipeline: str | None = None,
    config_file: str | None = None,
    name: str | None = None,
    instance_size: int | None = None,
    conda_env: str | None = None,
    user_config_json: str | None = None,
    ami_id: str | None = None,
    instance_type: str | None = None,
    subnet_id: str | None = None,
    security_group_ids: list[str] | str | None = None,
    key_name: str | None = None,
    iam_instance_profile: str | None = None,
    region: str | None = None,
    assign_public_ip: bool | None = None,
    dry_run: bool | None = None,
    root_volume_size_gb: int | None = None,
    user_data: str | None = None,
    **_kwargs,
) -> dict[str, Any]:
    """
    Launch a standalone EC2 instance using boto3 and an AMI with pre-installed runtime.

    This executor is independent from BMLL jobs.
    """
    target = pipeline or config_file
    if not target:
        raise SystemExit("Provide --pipeline (or --config_file).")

    defaults = EC2RunConfig.from_env()
    cfg = EC2RunConfig(
        region=region or defaults.region,
        ami_id=ami_id or defaults.ami_id,
        instance_type=instance_type or _map_instance_size(instance_size, defaults.instance_type),
        subnet_id=subnet_id or defaults.subnet_id,
        security_group_ids=defaults.security_group_ids if security_group_ids is None else (
            [x.strip() for x in security_group_ids.split(",") if x.strip()]
            if isinstance(security_group_ids, str)
            else list(security_group_ids)
        ),
        key_name=key_name or defaults.key_name,
        iam_instance_profile=iam_instance_profile or defaults.iam_instance_profile,
        assign_public_ip=defaults.assign_public_ip if assign_public_ip is None else bool(assign_public_ip),
        root_volume_size_gb=defaults.root_volume_size_gb if root_volume_size_gb is None else int(root_volume_size_gb),
        user_data=user_data,
        dry_run=defaults.dry_run if dry_run is None else bool(dry_run),
        instance_name_prefix=defaults.instance_name_prefix,
    )
    if not cfg.ami_id:
        raise RuntimeError("Missing AMI id. Set --ami_id or BASALT_EC2_AMI_ID.")

    user_data_payload = cfg.user_data or _build_user_data(
        pipeline=target,
        user_config_json=user_config_json,
        conda_env=conda_env,
    )

    boto3 = _require_boto3()
    client = boto3.client("ec2", region_name=cfg.region)
    launch_args: dict[str, Any] = {
        "ImageId": cfg.ami_id,
        "InstanceType": cfg.instance_type,
        "MinCount": 1,
        "MaxCount": 1,
        "UserData": user_data_payload,
        "DryRun": bool(cfg.dry_run),
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": name or f"{cfg.instance_name_prefix}-{os.getpid()}"},
                    {"Key": "ManagedBy", "Value": "basalt"},
                    {"Key": "Executor", "Value": "aws_ec2"},
                ],
            }
        ],
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "VolumeSize": int(cfg.root_volume_size_gb),
                    "VolumeType": "gp3",
                    "DeleteOnTermination": True,
                },
            }
        ],
    }
    if cfg.subnet_id:
        nic = {
            "SubnetId": cfg.subnet_id,
            "AssociatePublicIpAddress": bool(cfg.assign_public_ip),
            "DeviceIndex": 0,
        }
        if cfg.security_group_ids:
            nic["Groups"] = list(cfg.security_group_ids)
        launch_args["NetworkInterfaces"] = [nic]
    elif cfg.security_group_ids:
        launch_args["SecurityGroupIds"] = list(cfg.security_group_ids)
    if cfg.key_name:
        launch_args["KeyName"] = cfg.key_name
    if cfg.iam_instance_profile:
        launch_args["IamInstanceProfile"] = {"Name": cfg.iam_instance_profile}

    resp = client.run_instances(**launch_args)
    instance = (resp.get("Instances") or [{}])[0]
    return {
        "executor": "ec2",
        "region": cfg.region,
        "ami_id": cfg.ami_id,
        "instance_type": cfg.instance_type,
        "instance_id": instance.get("InstanceId"),
        "state": (instance.get("State") or {}).get("Name"),
        "pipeline": target,
    }


def ec2_install(**kwargs) -> dict[str, Any]:
    """
    For parity with other executors, `install` maps to `run`.
    """
    return ec2_run(**kwargs)

