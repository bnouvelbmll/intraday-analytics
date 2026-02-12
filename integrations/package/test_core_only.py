from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DIST = ROOT / "dist"
OPTIONAL_DISTS = [
    "bmll-basalt-dagster",
    "bmll-basalt-preprocessors",
    "bmll-basalt-characteristics",
    "bmll-basalt-alpha101",
    "bmll-basalt-talib",
    "bmll-basalt-aws-ec2",
    "bmll-basalt-kubernetes",
]


def run(cmd: list[str], *, env: dict[str, str] | None = None, cwd: Path | None = None) -> None:
    print("$", " ".join(cmd))
    subprocess.check_call(cmd, env=env, cwd=cwd or ROOT)


def run_out(cmd: list[str], *, env: dict[str, str] | None = None, cwd: Path | None = None) -> str:
    print("$", " ".join(cmd))
    return subprocess.check_output(
        cmd, env=env, cwd=cwd or ROOT, text=True, stderr=subprocess.STDOUT
    )


def main() -> int:
    shutil.rmtree(ROOT / "build", ignore_errors=True)
    env = os.environ.copy()
    env["BASALT_DIST"] = "core"
    run([sys.executable, "setup.py", "bdist_wheel"], env=env)

    wheels = sorted(DIST.glob("bmll_basalt-*.whl"))
    if not wheels:
        raise RuntimeError("Core wheel not found in dist/")
    wheel = wheels[-1]

    target_dir = Path(tempfile.mkdtemp(prefix="basalt_core_only_target_"))
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
                str(wheel),
            ]
        )
        with zipfile.ZipFile(wheel) as zf:
            names = zf.namelist()
        forbidden_prefixes = (
            "basalt/dagster/",
            "basalt/preprocessors/",
            "basalt/analytics/characteristics/",
            "basalt/analytics/alpha101/",
            "basalt/analytics/talib/",
        )
        for pref in forbidden_prefixes:
            if any(name.startswith(pref) for name in names):
                raise RuntimeError(f"Core wheel unexpectedly contains {pref}")

        installed_meta = {p.name for p in target_dir.glob("*.dist-info")}
        for pkg in OPTIONAL_DISTS:
            normalized = pkg.replace("-", "_")
            if any(name.startswith(normalized) for name in installed_meta):
                raise RuntimeError(f"Unexpected optional package installed: {pkg}")

        out = run_out(
            [
                sys.executable,
                "-S",
                "-c",
                (
                    f"import sys; sys.path.insert(0, {str(target_dir)!r}); "
                    "sys.path.append('/home/bnouvel/lib/py312/lib/python3.12/site-packages'); "
                    "import basalt, pathlib; "
                    "p = pathlib.Path(basalt.__file__); "
                    "print(str(p)); "
                    "assert '/basalt/' in str(p); "
                    "print('import smoke ok')"
                ),
            ],
            cwd=Path("/tmp"),
        )
        print(out.strip())
        print("core-only integration check passed")
        return 0
    finally:
        shutil.rmtree(target_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
