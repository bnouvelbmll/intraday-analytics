from pathlib import Path
from setuptools import find_packages, setup
import os
import sys
import subprocess
import shutil


def _load_requirements(path: str) -> list[str]:
    reqs: list[str] = []
    file_path = Path(path)
    if not file_path.exists():
        return reqs
    for line in file_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        reqs.append(line)
    return reqs


core_requirements = _load_requirements("requirements.txt")
_OPTIONAL_CORE_REQ_PREFIXES = ("ta-lib",)
core_requirements = [
    req
    for req in core_requirements
    if not req.lower().startswith(_OPTIONAL_CORE_REQ_PREFIXES)
]

VERSION = "0.1.0"


def _all_dist_names() -> list[str]:
    return [
        "core",
        "dagster",
        "preprocessors",
        "characteristics",
        "alpha101",
        "visualization",
        "optimize",
        "objective_functions",
        "models",
        "talib",
        "mcp",
        "aws_ec2",
        "kubernetes",
    ]


def _build_all_dists() -> None:
    for name in _all_dist_names():
        env = os.environ.copy()
        env["BASALT_DIST"] = name
        subprocess.check_call([sys.executable, __file__, *sys.argv[1:]], env=env)


if (
    os.environ.get("BASALT_DIST") is None
    and any(cmd in sys.argv for cmd in ("bdist_wheel", "bdist", "sdist"))
):
    _build_all_dists()
    raise SystemExit(0)


dist = os.environ.get("BASALT_DIST", "core")
if dist == "all" and any(cmd in sys.argv for cmd in ("bdist_wheel", "bdist", "sdist")):
    _build_all_dists()
    raise SystemExit(0)

shutil.rmtree("build", ignore_errors=True)
subpackage_requirements = {
    "dagster": ["dagster", "dagster-webserver"],
    "preprocessors": [],
    "characteristics": [],
    "alpha101": [],
    "visualization": ["streamlit", "plotly"],
    "optimize": [],
    "objective_functions": [],
    "models": [],
    "talib": ["TA-Lib"],
    "mcp": ["fastmcp", "mcp"],
    "aws_ec2": [],
    "kubernetes": [],
}
extras = {
    name: [f"bmll-basalt-{name}>={VERSION}"] for name in subpackage_requirements
}
extras["all"] = sorted({dep for deps in extras.values() for dep in deps})

if dist == "core":
    name = "bmll-basalt"
    packages = find_packages(
        exclude=(
            "intraday_analytics*",
            "basalt.dagster*",
            "basalt.preprocessors*",
            "basalt.analytics.characteristics*",
            "basalt.analytics.alpha101*",
            "basalt.visualization*",
            "basalt.optimize*",
            "basalt.objective_functions*",
            "basalt.models*",
            "basalt.executors.aws_ec2*",
            "basalt.executors.kubernetes*",
            "basalt.analytics.talib*",
            "basalt.mcp*",
            "basalt.tests*",
            "basalt.analytics.tests*",
            "basalt.dagster.tests*",
            "basalt.preprocessors.tests*",
        )
    )
    install_requires = core_requirements
    extras_require = extras
    entry_points = {
        "console_scripts": [
            "basalt=basalt.basalt:main",
        ],
        "basalt.plugins": [
            "bmll=basalt.executors.bmll:get_basalt_plugin",
        ],
        "basalt.cli": [
            "bmll=basalt.executors.bmll:get_cli_extension",
        ],
    }
elif dist == "dagster":
    name = "bmll-basalt-dagster"
    extras_require = {}
    entry_points = {
        "basalt.plugins": [
            "dagster=basalt.dagster:get_basalt_plugin",
        ],
        "basalt.cli": [
            "dagster=basalt.dagster.cli_ext:get_cli_extension",
        ],
    }
    packages = find_packages(
        include=(
            "basalt.dagster",
            "basalt.dagster.*",
        )
    )
    install_requires = ["bmll-basalt>=" + VERSION] + subpackage_requirements[dist]
elif dist == "preprocessors":
    name = "bmll-basalt-preprocessors"
    extras_require = {}
    entry_points = {
        "basalt.plugins": [
            "preprocessors=basalt.preprocessors:get_basalt_plugin",
        ],
    }
    packages = find_packages(
        include=(
            "basalt.preprocessors",
            "basalt.preprocessors.*",
        )
    )
    install_requires = ["bmll-basalt>=" + VERSION] + subpackage_requirements[dist]
elif dist == "characteristics":
    name = "bmll-basalt-characteristics"
    extras_require = {}
    entry_points = {
        "basalt.plugins": [
            "characteristics=basalt.analytics.characteristics:get_basalt_plugin",
        ],
    }
    packages = find_packages(
        include=(
            "basalt.analytics.characteristics",
            "basalt.analytics.characteristics.*",
        )
    )
    install_requires = ["bmll-basalt>=" + VERSION] + subpackage_requirements[dist]
elif dist == "alpha101":
    name = "bmll-basalt-alpha101"
    extras_require = {}
    entry_points = {
        "basalt.plugins": [
            "alpha101=basalt.analytics.alpha101:get_basalt_plugin",
        ],
    }
    packages = find_packages(
        include=(
            "basalt.analytics.alpha101",
            "basalt.analytics.alpha101.*",
        )
    )
    install_requires = ["bmll-basalt>=" + VERSION] + subpackage_requirements[dist]
elif dist == "visualization":
    name = "bmll-basalt-visualization"
    extras_require = {}
    entry_points = {
        "basalt.plugins": [
            "visualization=basalt.visualization:get_basalt_plugin",
        ],
        "basalt.cli": [
            "viz=basalt.visualization.cli_ext:get_cli_extension",
        ],
    }
    packages = find_packages(
        include=(
            "basalt.visualization",
            "basalt.visualization.*",
        )
    )
    install_requires = ["bmll-basalt>=" + VERSION] + subpackage_requirements[dist]
elif dist == "optimize":
    name = "bmll-basalt-optimize"
    extras_require = {}
    entry_points = {
        "basalt.plugins": [
            "optimize=basalt.optimize:get_basalt_plugin",
        ],
        "basalt.cli": [
            "optimize=basalt.optimize.cli_ext:get_cli_extension",
        ],
    }
    packages = find_packages(
        include=(
            "basalt.optimize",
            "basalt.optimize.*",
        )
    )
    install_requires = ["bmll-basalt>=" + VERSION] + subpackage_requirements[dist]
elif dist == "objective_functions":
    name = "bmll-basalt-objective-functions"
    extras_require = {}
    entry_points = {
        "basalt.plugins": [
            "objective_functions=basalt.objective_functions:get_basalt_plugin",
        ],
    }
    packages = find_packages(
        include=(
            "basalt.objective_functions",
            "basalt.objective_functions.*",
        )
    )
    install_requires = ["bmll-basalt>=" + VERSION] + subpackage_requirements[dist]
elif dist == "models":
    name = "bmll-basalt-models"
    extras_require = {}
    entry_points = {
        "basalt.plugins": [
            "models=basalt.models:get_basalt_plugin",
        ],
    }
    packages = find_packages(
        include=(
            "basalt.models",
            "basalt.models.*",
        )
    )
    install_requires = [
        "bmll-basalt>=" + VERSION,
        "bmll-basalt-objective-functions>=" + VERSION,
    ] + subpackage_requirements[dist]
elif dist in {"aws_ec2", "kubernetes"}:
    name = f"bmll-basalt-{dist}"
    extras_require = {}
    if dist == "aws_ec2":
        packages = find_packages(
            include=(
                "basalt.executors",
                "basalt.executors.aws_ec2",
                "basalt.executors.aws_ec2.*",
            )
        )
        entry_points = {
            "basalt.plugins": [
                "aws_ec2=basalt.executors.aws_ec2:get_basalt_plugin",
            ],
            "basalt.cli": [
                "ec2=basalt.executors.aws_ec2:get_cli_extension",
            ],
        }
    else:
        packages = find_packages(
            include=(
                "basalt.executors",
                "basalt.executors.kubernetes",
                "basalt.executors.kubernetes.*",
            )
        )
        entry_points = {
            "basalt.plugins": [
                "kubernetes=basalt.executors.kubernetes:get_basalt_plugin",
            ],
            "basalt.cli": [
                "k8s=basalt.executors.kubernetes:get_cli_extension",
            ],
        }
    install_requires = ["bmll-basalt>=" + VERSION] + subpackage_requirements[dist]
elif dist == "talib":
    name = "bmll-basalt-talib"
    extras_require = {}
    entry_points = {
        "basalt.plugins": [
            "talib=basalt.analytics.talib:get_basalt_plugin",
        ],
    }
    packages = find_packages(
        include=(
            "basalt.analytics.talib",
            "basalt.analytics.talib.*",
        )
    )
    install_requires = ["bmll-basalt>=" + VERSION] + subpackage_requirements[dist]
elif dist == "mcp":
    name = "bmll-basalt-mcp"
    extras_require = {}
    entry_points = {
        "basalt.plugins": [
            "mcp=basalt.mcp:get_basalt_plugin",
        ],
        "basalt.cli": [
            "mcp=basalt.mcp.cli_ext:get_cli_extension",
        ],
    }
    packages = find_packages(
        include=(
            "basalt.mcp",
            "basalt.mcp.*",
        )
    )
    install_requires = ["bmll-basalt>=" + VERSION] + subpackage_requirements[dist]
elif dist == "all":
    # Aggregate editable/install mode: a single distribution containing all
    # local subpackages and all plugin/cli entry points. This avoids pip editable
    # failures that expect a single .egg-info directory.
    name = "bmll-basalt-all"
    extras_require = {}
    entry_points = {
        "console_scripts": [
            "basalt=basalt.basalt:main",
        ],
        "basalt.plugins": [
            "bmll=basalt.executors.bmll:get_basalt_plugin",
            "dagster=basalt.dagster:get_basalt_plugin",
            "preprocessors=basalt.preprocessors:get_basalt_plugin",
            "characteristics=basalt.analytics.characteristics:get_basalt_plugin",
            "alpha101=basalt.analytics.alpha101:get_basalt_plugin",
            "talib=basalt.analytics.talib:get_basalt_plugin",
            "mcp=basalt.mcp:get_basalt_plugin",
            "optimize=basalt.optimize:get_basalt_plugin",
            "objective_functions=basalt.objective_functions:get_basalt_plugin",
            "models=basalt.models:get_basalt_plugin",
            "aws_ec2=basalt.executors.aws_ec2:get_basalt_plugin",
            "kubernetes=basalt.executors.kubernetes:get_basalt_plugin",
            "visualization=basalt.visualization:get_basalt_plugin",
        ],
        "basalt.cli": [
            "bmll=basalt.executors.bmll:get_cli_extension",
            "dagster=basalt.dagster.cli_ext:get_cli_extension",
            "mcp=basalt.mcp.cli_ext:get_cli_extension",
            "optimize=basalt.optimize.cli_ext:get_cli_extension",
            "ec2=basalt.executors.aws_ec2:get_cli_extension",
            "k8s=basalt.executors.kubernetes:get_cli_extension",
            "viz=basalt.visualization.cli_ext:get_cli_extension",
        ],
    }
    packages = find_packages(
        exclude=(
            "intraday_analytics*",
            "basalt.tests*",
            "basalt.analytics.tests*",
            "basalt.dagster.tests*",
            "basalt.preprocessors.tests*",
        )
    )
    all_optional = sorted(
        {
            dep
            for deps in subpackage_requirements.values()
            for dep in deps
        }
    )
    install_requires = core_requirements + all_optional
else:
    name = f"bmll-basalt-{dist}"
    extras_require = {}
    entry_points = {}
    packages = []
    install_requires = ["bmll-basalt>=" + VERSION] + extras.get(dist, [])


setup(
    name=name,
    version=VERSION,
    description=(
        "BASALT (BMLL Advanced Statistical Analytics & Layered "
        "Transformations) intraday analytics pipeline"
    ),
    packages=packages,
    include_package_data=False,
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points=entry_points,
)
