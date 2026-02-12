from pathlib import Path
from setuptools import find_packages, setup


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
}
extras["all"] = sorted({dep for deps in extras.values() for dep in deps})


setup(
    name="bmll-basalt",
    version="0.1.0",
    description="Basalt intraday analytics pipeline",
    packages=find_packages(exclude=("intraday_analytics*",)),
    include_package_data=True,
    install_requires=core_requirements,
    extras_require=extras,
    entry_points={
        "console_scripts": [
            "basalt=basalt.basalt:main",
        ],
        "basalt.cli": [
            "dagster=basalt.dagster.cli_ext:get_cli_extension",
        ],
    },
)
