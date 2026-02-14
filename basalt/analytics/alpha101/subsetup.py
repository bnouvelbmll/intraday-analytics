"""Subpackage setup metadata for analytics alpha101."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "alpha101",
        "name": "bmll-basalt-alpha101",
        "packages": (
            "basalt.analytics.alpha101",
            "basalt.analytics.alpha101.*",
        ),
        "package_roots": ("basalt.analytics.alpha101",),
        "requirements": [],
        "entry_points": {
            "basalt.plugins": ["alpha101=basalt.analytics.alpha101:get_basalt_plugin"],
        },
    }
