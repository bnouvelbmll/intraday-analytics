"""Subpackage setup metadata for visualization."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "visualization",
        "name": "bmll-basalt-visualization",
        "packages": ("basalt.visualization", "basalt.visualization.*"),
        "package_roots": ("basalt.visualization",),
        "requirements": ["streamlit", "plotly"],
        "entry_points": {
            "basalt.plugins": ["visualization=basalt.visualization:get_basalt_plugin"],
            "basalt.cli": ["viz=basalt.visualization.cli_ext:get_cli_extension"],
        },
    }
