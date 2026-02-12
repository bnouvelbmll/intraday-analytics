#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _latest_file(directory: Path, prefix: str) -> Path | None:
    files = sorted(directory.glob(f"{prefix}_*.csv"))
    return files[-1] if files else None


def _to_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _bar(value: float | None, max_value: float, width: int = 40) -> str:
    if value is None or max_value <= 0:
        return ""
    n = int(round((value / max_value) * width))
    return "#" * max(0, min(width, n))


def _print_summary(rows: list[dict[str, str]]) -> None:
    if not rows:
        print("No summary rows.")
        return
    p95_values = [_to_float(r.get("p95_duration_s")) for r in rows]
    p95_values = [v for v in p95_values if v is not None]
    max_p95 = max(p95_values) if p95_values else 0.0

    print("Benchmark Summary")
    print("-" * 80)
    for row in rows:
        exe = row.get("executor", "")
        size = row.get("instance_size", "") or "-"
        runs = row.get("runs", "0")
        sr = row.get("success_rate", "")
        mean_s = _to_float(row.get("mean_duration_s"))
        p95_s = _to_float(row.get("p95_duration_s"))
        jph = row.get("jobs_per_hour", "")
        bar = _bar(p95_s, max_p95)
        print(
            f"{exe:8} size={size:>4} runs={runs:>3} success={sr:>6} "
            f"mean_s={mean_s!s:>8} p95_s={p95_s!s:>8} jph={jph:>8}  {bar}"
        )


def _write_html(
    summary_rows: list[dict[str, str]],
    out_path: Path,
) -> None:
    try:
        import plotly.express as px
        from plotly.offline import plot
    except Exception as exc:
        raise RuntimeError("plotly is required for --html output") from exc

    cleaned: list[dict[str, Any]] = []
    for row in summary_rows:
        cleaned.append(
            {
                "executor": row.get("executor"),
                "instance_size": row.get("instance_size") or "-",
                "runs": _to_float(row.get("runs")),
                "success_rate": _to_float(row.get("success_rate")),
                "mean_duration_s": _to_float(row.get("mean_duration_s")),
                "p95_duration_s": _to_float(row.get("p95_duration_s")),
                "jobs_per_hour": _to_float(row.get("jobs_per_hour")),
            }
        )

    fig = px.bar(
        cleaned,
        x="instance_size",
        y="p95_duration_s",
        color="executor",
        barmode="group",
        hover_data=["mean_duration_s", "success_rate", "jobs_per_hour", "runs"],
        title="Benchmark P95 Duration by Executor/Size",
    )
    html = plot(fig, include_plotlyjs=True, output_type="div")
    out_path.write_text(
        "<html><head><meta charset='utf-8'><title>Benchmark Visualization</title></head>"
        f"<body><h1>Benchmark Visualization</h1>{html}</body></html>",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Visualize benchmark_results output files."
    )
    parser.add_argument("--results-dir", default="benchmark_results")
    parser.add_argument("--summary-csv", default=None)
    parser.add_argument(
        "--html",
        default=None,
        help="Optional HTML output path (requires plotly).",
    )
    args = parser.parse_args()

    results_dir = Path(args.results_dir).resolve()
    if args.summary_csv:
        summary_path = Path(args.summary_csv).resolve()
    else:
        summary_path = _latest_file(results_dir, "perf_summary")
        if summary_path is None:
            raise SystemExit(f"No perf_summary_*.csv in {results_dir}")

    summary_rows = _load_csv(summary_path)
    print(f"Using summary: {summary_path}")
    _print_summary(summary_rows)

    if args.html:
        out_path = Path(args.html).resolve()
        _write_html(summary_rows, out_path)
        print(f"\nWrote HTML visualization: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
