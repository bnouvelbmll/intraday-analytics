from pathlib import Path
from setuptools import find_packages, setup
import os
import sys
import subprocess


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

extras = {
    "dagster": ["dagster", "dagster-webserver"],
    "characteristics": [],
    "preprocessors": [],
    "alpha101": [],
    "aws_ec2": [],
    "kubernetes": [],
}
extras["all"] = sorted({dep for deps in extras.values() for dep in deps})

VERSION = "0.1.0"


def _build_all_dists() -> None:
    dist_names = [
        "core",
        "dagster",
        "preprocessors",
        "characteristics",
        "alpha101",
        "aws_ec2",
        "kubernetes",
    ]
    for name in dist_names:
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

if dist == "core":
    name = "bmll-basalt"
    packages = find_packages(
        exclude=(
            "intraday_analytics*",
            "basalt.executors*",
        )
    )
    install_requires = core_requirements
    extras_require = extras
    entry_points = {
        "console_scripts": [
            "basalt=basalt.basalt:main",
        ],
        "basalt.cli": [
            "dagster=basalt.dagster.cli_ext:get_cli_extension",
        ],
    }
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
            "basalt.cli": [
                "k8s=basalt.executors.kubernetes:get_cli_extension",
            ],
        }
    install_requires = ["bmll-basalt>=" + VERSION] + extras.get(dist, [])
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
    include_package_data=True,
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points=entry_points,
)
