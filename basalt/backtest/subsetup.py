"""Subpackage setup metadata for backtest."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "backtest",
        "name": "bmll-basalt-backtest",
        "packages": ("basalt.backtest", "basalt.backtest.*"),
        "package_roots": ("basalt.backtest",),
        "requirements": [],
        "entry_points": {
            "basalt.plugins": ["backtest=basalt.backtest:get_basalt_plugin"],
            "basalt.cli": ["backtest=basalt.backtest.cli_ext:get_cli_extension"],
        },
    }
