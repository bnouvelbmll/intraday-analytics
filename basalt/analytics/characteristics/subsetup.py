"""Subpackage setup metadata for analytics characteristics."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "characteristics",
        "name": "bmll-basalt-characteristics",
        "packages": (
            "basalt.analytics.characteristics",
            "basalt.analytics.characteristics.*",
        ),
        "package_roots": ("basalt.analytics.characteristics",),
        "requirements": [],
        "entry_points": {
            "basalt.plugins": [
                "characteristics=basalt.analytics.characteristics:get_basalt_plugin"
            ],
        },
    }
