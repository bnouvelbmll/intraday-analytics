#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


GROUPS = [
    ("basalt.analytics.characteristics", "basalt/analytics/characteristics/", []),
    ("basalt.analytics.alpha101", "basalt/analytics/alpha101/", []),
    ("basalt.analytics", "basalt/analytics/", ["basalt/analytics/characteristics/", "basalt/analytics/alpha101/"]),
    ("basalt.preprocessors", "basalt/preprocessors/", []),
    ("basalt.time", "basalt/time/", []),
    ("basalt.dagster", "basalt/dagster/", []),
    ("basalt.executors", "basalt/executors/", []),
    (
        "basalt",
        "basalt/",
        [
            "basalt/analytics/",
            "basalt/preprocessors/",
            "basalt/time/",
            "basalt/dagster/",
            "basalt/executors/",
        ],
    ),
    (
        "basalt.core",
        "basalt/",
        [
            "basalt/analytics/",
            "basalt/preprocessors/",
            "basalt/time/",
            "basalt/dagster/",
            "basalt/executors/",
            "basalt/tests/",
            "basalt/cli.py",
            "basalt/config_ui.py",
            "basalt/schema_utils.py",
            "basalt/analytics_explain.py",
        ],
    ),
    ("demo", "demo/", []),
    ("main.py", "main.py", []),
]


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def _normalize(path: str) -> str:
    return path.replace("\\", "/")


def _matches(path: str, prefix: str) -> bool:
    if prefix.endswith("/"):
        return path.startswith(prefix)
    return path == prefix


def _in_group(path: str, prefix: str, excludes: list[str]) -> bool:
    if not _matches(path, prefix):
        return False
    return not any(_matches(path, ex) for ex in excludes)


def _group_summary(files: dict, prefix: str, excludes: list[str]) -> tuple[int, int]:
    covered = 0
    total = 0
    for filename, payload in files.items():
        f = _normalize(filename)
        if not _in_group(f, prefix, excludes):
            continue
        summary = payload.get("summary", {})
        covered += int(summary.get("covered_lines", 0))
        total += int(summary.get("num_statements", 0))
    return covered, total


def _file_offenders(
    files: dict,
    *,
    prefix: str,
    excludes: list[str],
    limit: int,
    min_statements: int,
) -> list[tuple[str, int, int, float]]:
    offenders: list[tuple[str, int, int, float]] = []
    for filename, payload in files.items():
        path = _normalize(filename)
        summary = payload.get("summary", {})
        stmts = int(summary.get("num_statements", 0))
        covered = int(summary.get("covered_lines", 0))
        missed = max(stmts - covered, 0)
        if stmts < min_statements:
            continue
        if not _in_group(path, prefix, excludes):
            continue
        pct = (covered / stmts * 100.0) if stmts else 0.0
        offenders.append((path, stmts, missed, pct))
    offenders.sort(key=lambda x: (x[3], -x[2], -x[1], x[0]))
    return offenders[:limit]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run pytest coverage and print coverage by package/subpackage."
    )
    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Do not run tests; use existing .coverage data.",
    )
    parser.add_argument(
        "--offenders",
        type=int,
        default=15,
        help="Number of lowest-coverage files to display.",
    )
    parser.add_argument(
        "--min-statements",
        type=int,
        default=20,
        help="Minimum statements for a file to be included in offenders list.",
    )
    parser.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Extra args forwarded to pytest when tests are run.",
    )
    args = parser.parse_args()

    if not args.skip_tests:
        pytest_args = args.pytest_args
        if pytest_args and pytest_args[0] == "--":
            pytest_args = pytest_args[1:]
        _run(
            [
                sys.executable,
                "-m",
                "coverage",
                "run",
                "--source=basalt,demo,main",
                "-m",
                "pytest",
                *pytest_args,
            ]
        )

    json_path = Path(".coverage.json")
    _run(
        [
            sys.executable,
            "-m",
            "coverage",
            "json",
            "--ignore-errors",
            "-o",
            str(json_path),
        ]
    )
    data = json.loads(json_path.read_text(encoding="utf-8"))
    files = data.get("files", {})

    print("Coverage by package/subpackage")
    print("-" * 72)
    print(f"{'Group':40} {'Stmts':>10} {'Cover':>9}")
    print("-" * 72)
    for name, prefix, excludes in GROUPS:
        covered, total = _group_summary(files, prefix, excludes)
        pct = (covered / total * 100.0) if total else 0.0
        print(f"{name:40} {total:10d} {pct:8.2f}%")
    print("-" * 72)

    limit = max(args.offenders, 0)
    min_statements = max(args.min_statements, 0)
    if limit > 0:
        print("Worst offenders by package/subpackage")
        print("-" * 120)
        print(f"{'Package':35} {'File':55} {'Stmts':>8} {'Miss':>8} {'Cover':>9}")
        print("-" * 120)
        printed = False
        for group_name, prefix, excludes in GROUPS:
            offenders = _file_offenders(
                files,
                prefix=prefix,
                excludes=excludes,
                limit=limit,
                min_statements=min_statements,
            )
            for path, stmts, missed, pct in offenders:
                shown = path if len(path) <= 55 else "..." + path[-52:]
                print(f"{group_name:35} {shown:55} {stmts:8d} {missed:8d} {pct:8.2f}%")
                printed = True
        if not printed:
            print("(no files matched filters)")
        print("-" * 120)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
