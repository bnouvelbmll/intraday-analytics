from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
import os


ROOT = Path(__file__).resolve().parents[2]
DIST = ROOT / "dist"
SUBPACKAGES = [
    "dagster",
    "preprocessors",
    "characteristics",
    "alpha101",
    "optimize",
    "objective_functions",
    "models",
]


def run(cmd: list[str], *, cwd: Path | None = None) -> None:
    print("$", " ".join(cmd))
    subprocess.check_call(cmd, cwd=cwd or ROOT)


def find_wheel(prefix: str) -> Path:
    wheels = sorted(DIST.glob(f"{prefix.replace('-', '_')}-*.whl"))
    if not wheels:
        raise RuntimeError(f"Wheel not found: {prefix}")
    return wheels[-1]


def main() -> int:
    for dist in ["core", *SUBPACKAGES]:
        env = os.environ.copy()
        env["BASALT_DIST"] = dist
        print("$", " ".join([sys.executable, "setup.py", "bdist_wheel"]))
        subprocess.check_call([sys.executable, "setup.py", "bdist_wheel"], cwd=ROOT, env=env)

    core_wheel = find_wheel("bmll-basalt")
    sub_wheels = [find_wheel(f"bmll-basalt-{name}") for name in SUBPACKAGES]

    target_dir = Path(tempfile.mkdtemp(prefix="basalt_with_subpkgs_target_"))
    try:
        run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--no-deps",
                "--target",
                str(target_dir),
                str(core_wheel),
                *[str(w) for w in sub_wheels],
            ]
        )

        run(
            [
                sys.executable,
                "-S",
                "-c",
                (
                    f"import sys; sys.path.insert(0, {str(target_dir)!r}); "
                    "sys.path.append('/home/bnouvel/lib/py312/lib/python3.12/site-packages'); "
                    "import basalt; "
                    "import basalt.dagster; "
                    "import basalt.preprocessors; "
                    "import basalt.analytics.characteristics; "
                    "import basalt.analytics.alpha101; "
                    "import basalt.optimize; "
                    "import basalt.objective_functions; "
                    "import basalt.models; "
                    "print('ok')"
                ),
            ],
            cwd=Path("/tmp"),
        )
        run(
            [
                sys.executable,
                "-S",
                "-c",
                (
                    f"import sys; sys.path.insert(0, {str(target_dir)!r}); "
                    "sys.path.append('/home/bnouvel/lib/py312/lib/python3.12/site-packages'); "
                    "from basalt.basalt import _load_cli_extensions; "
                    "ext = _load_cli_extensions(); "
                    "print(sorted(ext.keys()))"
                ),
            ],
            cwd=Path("/tmp"),
        )
        print("core+subpackages integration check passed")
        return 0
    finally:
        shutil.rmtree(target_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
