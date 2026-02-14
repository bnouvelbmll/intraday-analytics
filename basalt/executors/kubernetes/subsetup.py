"""Subpackage setup metadata for Kubernetes executor."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "kubernetes",
        "name": "bmll-basalt-kubernetes",
        "packages": (
            "basalt.executors",
            "basalt.executors.kubernetes",
            "basalt.executors.kubernetes.*",
        ),
        "package_roots": ("basalt.executors.kubernetes",),
        "requirements": [],
        "entry_points": {
            "basalt.plugins": [
                "kubernetes=basalt.executors.kubernetes:get_basalt_plugin"
            ],
            "basalt.cli": [
                "k8s=basalt.executors.kubernetes:get_cli_extension"
            ],
        },
    }
