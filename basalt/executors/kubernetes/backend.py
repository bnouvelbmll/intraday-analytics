from __future__ import annotations


def k8s_run(*args, **kwargs):
    raise NotImplementedError(
        "Kubernetes executor is not implemented yet. Use `basalt ec2 run` for now."
    )


def k8s_install(*args, **kwargs):
    raise NotImplementedError(
        "Kubernetes executor install is not implemented yet."
    )

