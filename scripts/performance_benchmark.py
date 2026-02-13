#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import inspect
import json
import math
from pathlib import Path
import platform
import statistics
import time
from typing import Any


SUCCESS_STATES = {"success", "succeeded", "completed", "ok", "finished"}
FAILURE_STATES = {"failed", "failure", "error", "cancelled", "canceled", "killed"}
TERMINAL_STATES = SUCCESS_STATES | FAILURE_STATES


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _parse_csv_list(text: str) -> list[str]:
    return [x.strip() for x in str(text).split(",") if x.strip()]


def _normalize_executor(name: str) -> str:
    low = str(name).strip().lower()
    allowed = {"direct", "dagster", "bmll", "ec2", "kubernetes"}
    if low not in allowed:
        raise ValueError(
            f"Unsupported executor '{name}'. Use one of: {', '.join(sorted(allowed))}."
        )
    return low


def _parse_single_executor(text: str) -> str:
    values = _parse_csv_list(text)
    if len(values) != 1:
        raise ValueError(
            "Use exactly one executor per benchmark run "
            "(for example `--executors bmll`)."
        )
    return _normalize_executor(values[0])


def _parse_instance_sizes(text: str) -> list[int]:
    out: list[int] = []
    for part in _parse_csv_list(text):
        out.append(int(part))
    if not out:
        raise ValueError("At least one instance size is required.")
    return out


def _coerce_scalar(value: str) -> Any:
    low = str(value).strip().lower()
    if low in {"true", "false"}:
        return low == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except Exception:
        return value


def _parse_extra_args(items: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid --extra-arg '{item}'. Expected KEY=VALUE.")
        key, value = item.split("=", 1)
        out[key.strip()] = _coerce_scalar(value.strip())
    return out


def _extract_ids(job_run: Any) -> tuple[str | None, str | None]:
    job_id = None
    run_id = None
    if isinstance(job_run, tuple) and len(job_run) == 2:
        job, run = job_run
        job_id = getattr(job, "id", None)
        run_id = getattr(run, "id", None) or getattr(run, "run_id", None)
    return (
        str(job_id) if job_id is not None else None,
        str(run_id) if run_id is not None else None,
    )


def _run_bmll_case(
    *,
    pipeline: str,
    instance_size: int,
    name: str,
    delete_after: bool,
    extra_args: dict[str, Any],
) -> tuple[Any, str | None, str | None]:
    from basalt.executors.bmll.backend import submit_instance_job
    from basalt.cli import (
        _format_cli_args,
        _load_bmll_job_config,
        _write_basalt_run_script,
        run_cli,
    )

    job_config = _load_bmll_job_config(pipeline)
    cli_args = {"config_file": pipeline, **extra_args}
    allowed = set(inspect.signature(run_cli).parameters.keys())
    cli_args = {k: v for k, v in cli_args.items() if k in allowed}
    script_path = _write_basalt_run_script(job_config, _format_cli_args(cli_args))
    result = submit_instance_job(
        script_path,
        name=name,
        instance_size=instance_size,
        delete_after=delete_after,
        config=job_config,
    )
    job_id, run_id = _extract_ids(result)
    return result, job_id, run_id


def _run_ec2_case(
    *,
    pipeline: str,
    instance_size: int,
    name: str,
    delete_after: bool,
    extra_args: dict[str, Any],
) -> tuple[Any, str | None, str | None]:
    from basalt.executors.aws_ec2 import ec2_run

    result = ec2_run(
        pipeline=pipeline,
        instance_size=instance_size,
        name=name,
        delete_after=delete_after,
        **extra_args,
    )
    job_id, run_id = _extract_ids(result)
    return result, job_id, run_id


def _poll_bmll_run_status(
    *,
    run_id: str,
    timeout_seconds: int,
    poll_interval_seconds: int,
) -> tuple[str, str | None]:
    from bmll import compute

    deadline = time.time() + timeout_seconds
    last_state = "unknown"
    while time.time() < deadline:
        runs = compute.get_job_runs()
        found = None
        for run in runs:
            rid = str(getattr(run, "id", None) or getattr(run, "run_id", None) or "")
            if rid == run_id:
                found = run
                break
        if found is None:
            time.sleep(poll_interval_seconds)
            continue
        state = str(
            getattr(found, "state", None) or getattr(found, "status", "unknown")
        ).lower()
        last_state = state
        if state in TERMINAL_STATES:
            return state, str(
                getattr(found, "id", None) or getattr(found, "run_id", None) or run_id
            )
        time.sleep(poll_interval_seconds)
    return f"timeout:{last_state}", run_id


def _run_dagster_case(
    *,
    pipeline: str,
    job: str | None,
    partition: str | None,
) -> tuple[int, str]:
    from basalt.dagster.cli_ext import _dagster_run

    rc = _dagster_run(pipeline=pipeline, job=job, partition=partition)
    code = int(rc) if isinstance(rc, int) else 0
    return code, ("success" if code == 0 else f"error:{code}")


def _run_direct_case(*, pipeline: str, extra_args: dict[str, Any]) -> tuple[int, str]:
    from basalt.basalt import _pipeline_run

    _pipeline_run(pipeline=pipeline, **extra_args)
    return 0, "success"


def _run_k8s_case(*, pipeline: str, extra_args: dict[str, Any]) -> tuple[int, str]:
    from basalt.executors.kubernetes import k8s_run

    rc = k8s_run(pipeline=pipeline, **extra_args)
    code = int(rc) if isinstance(rc, int) else 0
    return code, ("success" if code == 0 else f"error:{code}")


def _run_case(
    *,
    benchmark_id: str,
    executor: str,
    pipeline: str,
    instance_size: int | None,
    repeat_index: int,
    name_prefix: str,
    timeout_seconds: int,
    poll_interval_seconds: int,
    delete_after: bool,
    extra_args: dict[str, Any],
    dagster_job: str | None,
    dagster_partition: str | None,
    dry_run: bool,
) -> dict[str, Any]:
    submitted_at = _now_iso()
    started = time.perf_counter()
    record: dict[str, Any] = {
        "benchmark_id": benchmark_id,
        "submitted_at": submitted_at,
        "executor": executor,
        "pipeline": pipeline,
        "instance_size": instance_size,
        "repeat_index": repeat_index,
        "host_machine": platform.machine(),
        "host_platform": platform.platform(),
        "job_id": None,
        "run_id": None,
        "status": "unknown",
        "success": False,
        "duration_s": None,
        "error": None,
    }
    if dry_run:
        record["status"] = "dry_run"
        record["duration_s"] = 0.0
        return record
    try:
        if executor in {"bmll", "ec2"}:
            name = f"{name_prefix}[{executor}][{instance_size}gb][r{repeat_index}]"
            runner = _run_bmll_case if executor == "bmll" else _run_ec2_case
            _result, job_id, run_id = runner(
                pipeline=pipeline,
                instance_size=int(instance_size or 16),
                name=name,
                delete_after=delete_after,
                extra_args=extra_args,
            )
            record["job_id"] = job_id
            record["run_id"] = run_id
            if run_id:
                state, resolved_run_id = _poll_bmll_run_status(
                    run_id=run_id,
                    timeout_seconds=timeout_seconds,
                    poll_interval_seconds=poll_interval_seconds,
                )
                record["run_id"] = resolved_run_id
                record["status"] = state
                record["success"] = any(s in state for s in SUCCESS_STATES)
            else:
                record["status"] = "submitted_no_run_id"
                record["success"] = False
        elif executor == "dagster":
            _rc, state = _run_dagster_case(
                pipeline=pipeline,
                job=dagster_job,
                partition=dagster_partition,
            )
            record["status"] = state
            record["success"] = state == "success"
        elif executor == "direct":
            _rc, state = _run_direct_case(pipeline=pipeline, extra_args=extra_args)
            record["status"] = state
            record["success"] = state == "success"
        elif executor == "kubernetes":
            _rc, state = _run_k8s_case(pipeline=pipeline, extra_args=extra_args)
            record["status"] = state
            record["success"] = state == "success"
        else:
            raise ValueError(f"Unsupported executor: {executor}")
    except Exception as exc:
        record["status"] = "error"
        record["error_type"] = type(exc).__name__
        record["error"] = str(exc)
        record["success"] = False
    finally:
        record["duration_s"] = round(time.perf_counter() - started, 3)
        record["finished_at"] = _now_iso()
    return record


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    k = (len(values) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return values[int(k)]
    return values[f] * (c - k) + values[c] * (k - f)


def _summarize(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in records:
        key = (row["executor"], str(row.get("instance_size")))
        groups.setdefault(key, []).append(row)
    out: list[dict[str, Any]] = []
    for (executor, size), rows in sorted(groups.items()):
        durations = sorted(
            float(r["duration_s"]) for r in rows if r.get("duration_s") is not None
        )
        successes = sum(1 for r in rows if r.get("success"))
        total = len(rows)
        row = {
            "executor": executor,
            "instance_size": size if size != "None" else "",
            "runs": total,
            "successes": successes,
            "success_rate": round(successes / total, 4) if total else 0.0,
            "mean_duration_s": round(statistics.mean(durations), 3)
            if durations
            else None,
            "p50_duration_s": round(_percentile(durations, 0.5), 3)
            if durations
            else None,
            "p95_duration_s": round(_percentile(durations, 0.95), 3)
            if durations
            else None,
            "min_duration_s": min(durations) if durations else None,
            "max_duration_s": max(durations) if durations else None,
            "jobs_per_hour": round(3600.0 / statistics.mean(durations), 3)
            if durations and statistics.mean(durations) > 0
            else None,
        }
        out.append(row)
    return out


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark BASALT runs for one executor."
    )
    parser.add_argument("--pipeline", required=True, help="Pipeline module/file path.")
    parser.add_argument(
        "--executors",
        default="bmll",
        help="Single executor: direct, dagster, bmll, ec2, kubernetes.",
    )
    parser.add_argument(
        "--instance-sizes",
        default="16,32,64",
        help="Comma-separated instance sizes for bmll/ec2 runs.",
    )
    parser.add_argument("--repeats", type=int, default=1, help="Repeats per entry.")
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    parser.add_argument("--poll-interval-seconds", type=int, default=30)
    parser.add_argument("--delete-after", action="store_true")
    parser.add_argument("--output-dir", default="benchmark_results")
    parser.add_argument("--tag", default="")
    parser.add_argument("--dagster-job", default=None)
    parser.add_argument("--dagster-partition", default=None)
    parser.add_argument(
        "--run-per-size",
        action="store_true",
        help="For non bmll/ec2 executors, run once per size entry.",
    )
    parser.add_argument(
        "--extra-arg",
        action="append",
        default=[],
        help="Extra KEY=VALUE forwarded to selected executor call.",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    executor = _parse_single_executor(args.executors)
    sizes = _parse_instance_sizes(args.instance_sizes)
    extra_args = _parse_extra_args(args.extra_arg)
    benchmark_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    name_prefix = f"[basalt][perf]{'[' + args.tag + ']' if args.tag else ''}"

    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    use_sizes = executor in {"bmll", "ec2"} or args.run_per_size
    size_values = sizes if use_sizes else [None]

    records: list[dict[str, Any]] = []
    for size in size_values:
        for i in range(1, int(args.repeats) + 1):
            record = _run_case(
                benchmark_id=benchmark_id,
                executor=executor,
                pipeline=args.pipeline,
                instance_size=size,
                repeat_index=i,
                name_prefix=name_prefix,
                timeout_seconds=int(args.timeout_seconds),
                poll_interval_seconds=int(args.poll_interval_seconds),
                delete_after=bool(args.delete_after),
                extra_args=extra_args,
                dagster_job=args.dagster_job,
                dagster_partition=args.dagster_partition,
                dry_run=bool(args.dry_run),
            )
            records.append(record)
            print(
                f"[{executor}] size={size} repeat={i} "
                f"status={record['status']} duration_s={record['duration_s']}"
            )
            if record.get("error"):
                print(f"  error: {record['error_type']}: {record['error']}")

    summary = _summarize(records)

    runs_jsonl = out_dir / f"perf_runs_{benchmark_id}.jsonl"
    runs_csv = out_dir / f"perf_runs_{benchmark_id}.csv"
    summary_csv = out_dir / f"perf_summary_{benchmark_id}.csv"

    _write_jsonl(runs_jsonl, records)
    _write_csv(runs_csv, records)
    _write_csv(summary_csv, summary)

    print("\nSummary:")
    for row in summary:
        print(
            f"- executor={row['executor']} size={row['instance_size'] or '-'} "
            f"runs={row['runs']} success_rate={row['success_rate']} "
            f"mean_s={row['mean_duration_s']} p95_s={row['p95_duration_s']} "
            f"jobs_per_hour={row['jobs_per_hour']}"
        )

    print("\nArtifacts:")
    print(f"- {runs_jsonl}")
    print(f"- {runs_csv}")
    print(f"- {summary_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
