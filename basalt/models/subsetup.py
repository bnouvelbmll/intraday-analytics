"""Subpackage setup metadata for models."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "models",
        "name": "bmll-basalt-models",
        "packages": ("basalt.models", "basalt.models.*"),
        "package_roots": ("basalt.models",),
        "requirements": ["bmll-basalt-objective-functions>=0.1.0"],
        "entry_points": {
            "basalt.plugins": ["models=basalt.models:get_basalt_plugin"],
        },
    }
