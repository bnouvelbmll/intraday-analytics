"""Subpackage setup metadata for Dagster integration."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "dagster",
        "name": "bmll-basalt-dagster",
        "packages": ("basalt.dagster", "basalt.dagster.*"),
        "package_roots": ("basalt.dagster",),
        "requirements": ["dagster", "dagster-webserver"],
        "entry_points": {
            "basalt.plugins": ["dagster=basalt.dagster:get_basalt_plugin"],
            "basalt.cli": ["dagster=basalt.dagster.cli_ext:get_cli_extension"],
        },
    }
