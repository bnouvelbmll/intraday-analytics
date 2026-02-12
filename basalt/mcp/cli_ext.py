from __future__ import annotations

class MCPCLI:
    @staticmethod
    def serve(*args, **kwargs):
        from .server import run_server

        return run_server(*args, **kwargs)


def get_cli_extension():
    return {"mcp": MCPCLI}
