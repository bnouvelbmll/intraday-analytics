"""Subpackage setup metadata for talib analytics."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "talib",
        "name": "bmll-basalt-talib",
        "packages": ("basalt.analytics.talib", "basalt.analytics.talib.*"),
        "package_roots": ("basalt.analytics.talib",),
        "requirements": ["TA-Lib"],
        "entry_points": {
            "basalt.plugins": ["talib=basalt.analytics.talib:get_basalt_plugin"],
        },
    }
