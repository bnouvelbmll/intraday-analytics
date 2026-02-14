"""Subpackage setup metadata for objective functions."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "objective_functions",
        "name": "bmll-basalt-objective-functions",
        "packages": (
            "basalt.objective_functions",
            "basalt.objective_functions.*",
        ),
        "package_roots": ("basalt.objective_functions",),
        "requirements": [],
        "entry_points": {
            "basalt.plugins": [
                "objective_functions=basalt.objective_functions:get_basalt_plugin"
            ],
        },
    }
