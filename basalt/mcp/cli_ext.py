from __future__ import annotations

from . import service


class MCPCLI:
    @staticmethod
    def capabilities():
        return service.list_capabilities()

    @staticmethod
    def configure(*args, **kwargs):
        return service.configure_job(*args, **kwargs)

    @staticmethod
    def run(*args, **kwargs):
        return service.run_job(*args, **kwargs)

    @staticmethod
    def recent_runs(*args, **kwargs):
        return service.recent_runs(*args, **kwargs)

    @staticmethod
    def success_rate(*args, **kwargs):
        return service.success_rate(*args, **kwargs)

    @staticmethod
    def materialized_partitions(*args, **kwargs):
        return service.materialized_partitions(*args, **kwargs)

    @staticmethod
    def serve(*args, **kwargs):
        from .server import run_server

        return run_server(*args, **kwargs)


def get_cli_extension():
    return {"mcp": MCPCLI}
