from pathlib import Path
from setuptools import find_packages, setup
import importlib.util
import os
import sys
import subprocess
import shutil


VERSION = "0.1.0"


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


def _load_subsetup(path: Path) -> dict:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load subpackage setup: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    if not hasattr(module, "get_subpackage_spec"):
        raise RuntimeError(f"Missing get_subpackage_spec in {path}")
    data = module.get_subpackage_spec()
    if not isinstance(data, dict):
        raise RuntimeError(f"Subpackage spec in {path} must be a dict")
    data["_path"] = str(path)
    return data


def _discover_subpackages(root: str = "basalt") -> list[dict]:
    specs: list[dict] = []
    for path in sorted(Path(root).rglob("subsetup.py")):
        specs.append(_load_subsetup(path))
    return specs


def _all_dist_names(specs: list[dict]) -> list[str]:
    names = ["core"]
    names.extend(sorted({spec["dist"] for spec in specs}))
    return names


def _merge_entry_points(*groups: dict) -> dict:
    merged: dict[str, list[str]] = {}
    for group in groups:
        for key, values in (group or {}).items():
            merged.setdefault(key, [])
            for value in values:
                if value not in merged[key]:
                    merged[key].append(value)
    return merged


def _build_all_dists(specs: list[dict]) -> None:
    for name in _all_dist_names(specs):
        env = os.environ.copy()
        env["BASALT_DIST"] = name
        subprocess.check_call([sys.executable, __file__, *sys.argv[1:]], env=env)


subpackages = _discover_subpackages()
subpackages_by_dist = {spec["dist"]: spec for spec in subpackages}

if (
    os.environ.get("BASALT_DIST") is None
    and any(cmd in sys.argv for cmd in ("bdist_wheel", "bdist", "sdist"))
):
    _build_all_dists(subpackages)
    raise SystemExit(0)


dist = os.environ.get("BASALT_DIST", "core")
if dist == "all" and any(cmd in sys.argv for cmd in ("bdist_wheel", "bdist", "sdist")):
    _build_all_dists(subpackages)
    raise SystemExit(0)

shutil.rmtree("build", ignore_errors=True)

subpackage_requirements = {
    spec["dist"]: list(spec.get("requirements") or []) for spec in subpackages
}

extras = {
    name: [f"bmll-basalt-{name}>={VERSION}"] for name in subpackage_requirements
}
extras["all"] = sorted({dep for deps in extras.values() for dep in deps})

core_entry_points = {
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

if dist == "core":
    name = "bmll-basalt"
    core_excludes = [
        "intraday_analytics*",
        "basalt.tests*",
        "basalt.analytics.tests*",
        "basalt.dagster.tests*",
        "basalt.preprocessors.tests*",
    ]
    for spec in subpackages:
        for pkg_root in spec.get("package_roots", []):
            core_excludes.append(f"{pkg_root}*")
    packages = find_packages(exclude=tuple(core_excludes))
    install_requires = core_requirements
    extras_require = extras
    entry_points = core_entry_points
elif dist == "all":
    name = "bmll-basalt-all"
    extras_require = {}
    entry_points = _merge_entry_points(
        core_entry_points,
        *(spec.get("entry_points") or {} for spec in subpackages),
    )
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
        {dep for deps in subpackage_requirements.values() for dep in deps}
    )
    install_requires = core_requirements + all_optional
else:
    spec = subpackages_by_dist.get(dist)
    if spec is None:
        name = f"bmll-basalt-{dist}"
        extras_require = {}
        entry_points = {}
        packages = []
        install_requires = ["bmll-basalt>=" + VERSION] + extras.get(dist, [])
    else:
        name = spec.get("name") or f"bmll-basalt-{dist}"
        extras_require = {}
        entry_points = spec.get("entry_points") or {}
        packages = find_packages(include=tuple(spec.get("packages") or ()))
        install_requires = ["bmll-basalt>=" + VERSION] + list(
            spec.get("requirements") or []
        )


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
