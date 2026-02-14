"""Subpackage setup metadata for MCP integration."""


def get_subpackage_spec() -> dict:
    return {
        "dist": "mcp",
        "name": "bmll-basalt-mcp",
        "packages": ("basalt.mcp", "basalt.mcp.*"),
        "package_roots": ("basalt.mcp",),
        "requirements": [
            "fastmcp>=2.14,<2.15",
            "mcp>=1.22,<2",
        ],
        "entry_points": {
            "basalt.plugins": ["mcp=basalt.mcp:get_basalt_plugin"],
            "basalt.cli": ["mcp=basalt.mcp.cli_ext:get_cli_extension"],
        },
    }
