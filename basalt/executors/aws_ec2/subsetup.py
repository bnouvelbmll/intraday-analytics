"""Subpackage setup metadata for AWS EC2 executor."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "aws_ec2",
        "name": "bmll-basalt-aws_ec2",
        "packages": (
            "basalt.executors",
            "basalt.executors.aws_ec2",
            "basalt.executors.aws_ec2.*",
        ),
        "package_roots": ("basalt.executors.aws_ec2",),
        "requirements": [],
        "entry_points": {
            "basalt.plugins": ["aws_ec2=basalt.executors.aws_ec2:get_basalt_plugin"],
            "basalt.cli": ["ec2=basalt.executors.aws_ec2:get_cli_extension"],
        },
    }
