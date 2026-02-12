from __future__ import annotations

from .backend import k8s_run, k8s_install


class K8SCLI:
    @staticmethod
    def run(*args, **kwargs):
        return k8s_run(*args, **kwargs)

    @staticmethod
    def install(*args, **kwargs):
        return k8s_install(*args, **kwargs)


def get_cli_extension():
    return {"k8s": K8SCLI}

