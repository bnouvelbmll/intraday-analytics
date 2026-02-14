"""Subpackage setup metadata for preprocessors."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "preprocessors",
        "name": "bmll-basalt-preprocessors",
        "packages": ("basalt.preprocessors", "basalt.preprocessors.*"),
        "package_roots": ("basalt.preprocessors",),
        "requirements": [],
        "entry_points": {
            "basalt.plugins": ["preprocessors=basalt.preprocessors:get_basalt_plugin"],
        },
    }
