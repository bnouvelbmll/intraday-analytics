"""Subpackage setup metadata for optimize."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "optimize",
        "name": "bmll-basalt-optimize",
        "packages": ("basalt.optimize", "basalt.optimize.*"),
        "package_roots": ("basalt.optimize",),
        "requirements": [],
        "entry_points": {
            "basalt.plugins": ["optimize=basalt.optimize:get_basalt_plugin"],
            "basalt.cli": ["optimize=basalt.optimize.cli_ext:get_cli_extension"],
        },
    }
