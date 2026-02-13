from __future__ import annotations

from pathlib import Path
import inspect
import importlib
import importlib.util
import json
import pkgutil
from typing import Any


def _to_plain(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): _to_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_plain(v) for v in value]
    if hasattr(value, "model_dump") and callable(value.model_dump):
        return _to_plain(value.model_dump())
    out: dict[str, Any] = {}
    for key in ("id", "name", "status", "state", "run_id", "job_name", "created_at"):
        if hasattr(value, key):
            out[key] = _to_plain(getattr(value, key))
    return out or repr(value)


def _ensure_analytics_loaded() -> None:
    import basalt.analytics as analytics_pkg

    for mod in pkgutil.iter_modules(
        analytics_pkg.__path__, analytics_pkg.__name__ + "."
    ):
        importlib.import_module(mod.name)
    importlib.import_module("basalt.time.dense")
    importlib.import_module("basalt.time.external_events")


def list_capabilities() -> dict[str, Any]:
    available = {
        "pipeline": True,
        "bmll": importlib.util.find_spec("basalt.executors.bmll") is not None,
        "ec2": importlib.util.find_spec("basalt.executors.aws_ec2") is not None,
        "k8s": importlib.util.find_spec("basalt.executors.kubernetes") is not None,
        "dagster": (
            importlib.util.find_spec("basalt.dagster") is not None
            and importlib.util.find_spec("dagster") is not None
        ),
        "optimize": importlib.util.find_spec("basalt.optimize") is not None,
        "objective_functions": importlib.util.find_spec("basalt.objective_functions") is not None,
        "models": importlib.util.find_spec("basalt.models") is not None,
        "mlflow": importlib.util.find_spec("mlflow") is not None,
        "wandb": importlib.util.find_spec("wandb") is not None,
        "fastmcp": importlib.util.find_spec("fastmcp") is not None,
    }
    return {"capabilities": available}


def list_plugins() -> dict[str, Any]:
    from basalt.plugins import list_plugins_payload

    return {"plugins": list_plugins_payload()}


def list_metrics(
    *,
    module: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    from basalt.analytics_base import ANALYTIC_DOCS

    _ensure_analytics_loaded()
    target = (module or "").strip().lower()
    rows: list[dict[str, Any]] = []
    for doc in ANALYTIC_DOCS:
        mod = str(doc.get("module") or "")
        if target and mod.lower() != target:
            continue
        rows.append(
            {
                "module": mod,
                "pattern": str(doc.get("pattern") or ""),
                "unit": str(doc.get("unit") or ""),
                "definition": str(doc.get("template") or ""),
                "description": str(doc.get("description") or ""),
            }
        )
    rows.sort(key=lambda r: (r["module"], r["pattern"]))
    if limit is not None:
        rows = rows[: max(0, int(limit))]
    return {"metrics": rows, "count": len(rows)}


def inspect_metric_source(
    *,
    metric: str,
    module: str | None = None,
    context_lines: int = 20,
) -> dict[str, Any]:
    from basalt.analytics_registry import get_registered_entries

    _ensure_analytics_loaded()
    query = str(metric or "").strip()
    if not query:
        raise ValueError("metric is required")
    module_hint = (module or "").strip().lower()
    entries = get_registered_entries()
    source_files: list[Path] = []
    seen: set[Path] = set()

    def _add_file_for_module(mod_name: str) -> None:
        entry = entries.get(mod_name)
        if entry is None:
            return
        src = inspect.getsourcefile(entry.cls)
        if not src:
            return
        path = Path(src)
        if path.exists() and path not in seen:
            seen.add(path)
            source_files.append(path)

    if module_hint:
        _add_file_for_module(module_hint)
    else:
        for mod_name in entries.keys():
            _add_file_for_module(mod_name)

    # Fallback to known analytic/time package files if registry lookups are empty.
    if not source_files:
        candidates = list(Path("basalt/analytics").glob("*.py")) + list(
            Path("basalt/time").glob("*.py")
        )
        for path in candidates:
            if path.exists() and path not in seen:
                seen.add(path)
                source_files.append(path)

    needle = query.lower()
    hits: list[dict[str, Any]] = []
    max_hits = 25
    span = max(1, int(context_lines))
    for path in source_files:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue
        for idx, line in enumerate(lines):
            if needle not in line.lower():
                continue
            start = max(0, idx - span)
            end = min(len(lines), idx + span + 1)
            snippet = "\n".join(lines[start:end])
            hits.append(
                {
                    "file": str(path),
                    "line": idx + 1,
                    "match": line.strip(),
                    "snippet": snippet,
                }
            )
            if len(hits) >= max_hits:
                return {"metric": query, "module": module, "matches": hits}
    return {"metric": query, "module": module, "matches": hits}


def _pipeline_yaml_path(pipeline: str) -> Path:
    return Path(pipeline).with_suffix(".yaml")


def configure_job(
    *,
    pipeline: str,
    executor: str = "bmll",
    instance_size: int | None = None,
    conda_env: str | None = None,
    max_runtime_hours: int | None = None,
    cron: str | None = None,
    cron_timezone: str | None = None,
    schedule_name: str | None = None,
) -> dict[str, Any]:
    from basalt.cli import _load_yaml_config, _extract_user_config
    from basalt.config_ui import _save_yaml_user_config

    executor = str(executor).strip().lower()
    yaml_path = _pipeline_yaml_path(pipeline)
    data = _load_yaml_config(str(yaml_path))
    user_cfg = _extract_user_config(data) if isinstance(data, dict) else {}

    if executor in {"bmll", "bmll_jobs", "job"}:
        cfg = dict(user_cfg.get("BMLL_JOBS") or {})
        cfg["enabled"] = True
        if instance_size is not None:
            cfg["default_instance_size"] = int(instance_size)
        if conda_env:
            cfg["default_conda_env"] = str(conda_env)
        if max_runtime_hours is not None:
            cfg["max_runtime_hours"] = int(max_runtime_hours)
        user_cfg["BMLL_JOBS"] = cfg
        _save_yaml_user_config(yaml_path, user_cfg)
        return {
            "pipeline": str(pipeline),
            "yaml_path": str(yaml_path),
            "executor": "bmll",
            "updated": {"BMLL_JOBS": cfg},
        }

    if executor == "dagster":
        from basalt.dagster.cli_ext import _dagster_install

        _dagster_install(
            pipeline=str(pipeline),
            cron=str(cron or "0 6 * * MON-SAT"),
            timezone=str(cron_timezone or "UTC"),
            schedule_name=schedule_name,
        )
        reloaded = _load_yaml_config(str(yaml_path))
        new_user_cfg = _extract_user_config(reloaded) if isinstance(reloaded, dict) else {}
        return {
            "pipeline": str(pipeline),
            "yaml_path": str(yaml_path),
            "executor": "dagster",
            "updated": {
                "SCHEDULES": new_user_cfg.get("SCHEDULES", []),
                "AUTO_MATERIALIZE_ENABLED": new_user_cfg.get("AUTO_MATERIALIZE_ENABLED"),
            },
        }

    raise ValueError(f"Unsupported executor for configure_job: {executor}")


def run_job(
    *,
    pipeline: str,
    executor: str = "bmll",
    name: str | None = None,
    instance_size: int | None = None,
    conda_env: str | None = None,
    cron: str | None = None,
    cron_timezone: str | None = None,
    overwrite_existing: bool = False,
    job: str | None = None,
    partition: str | None = None,
    run_id: str | None = None,
    instance_ref_file: str | None = None,
    **kwargs,
) -> dict[str, Any]:
    executor = str(executor).strip().lower()
    if executor in {"pipeline", "local"}:
        from basalt.basalt import _pipeline_run

        result = _pipeline_run(pipeline=str(pipeline), **kwargs)
        return {"executor": "pipeline", "result": _to_plain(result)}

    if executor in {"bmll", "bmll_jobs", "job"}:
        from basalt.executors.bmll import bmll_run

        result = bmll_run(
            pipeline=str(pipeline),
            name=name,
            instance_size=instance_size,
            conda_env=conda_env,
            cron=cron,
            cron_timezone=cron_timezone,
            overwrite_existing=overwrite_existing,
            **kwargs,
        )
        return {"executor": "bmll", "result": _to_plain(result)}

    if executor in {"ec2", "aws", "aws_ec2"}:
        from basalt.executors.aws_ec2 import ec2_run

        result = ec2_run(
            pipeline=str(pipeline),
            name=name,
            instance_size=instance_size,
            conda_env=conda_env,
            cron=cron,
            cron_timezone=cron_timezone,
            overwrite_existing=overwrite_existing,
            **kwargs,
        )
        return {"executor": "ec2", "result": _to_plain(result)}

    if executor in {"k8s", "kubernetes"}:
        from basalt.executors.kubernetes import k8s_run

        result = k8s_run(pipeline=str(pipeline), **kwargs)
        return {"executor": "k8s", "result": _to_plain(result)}

    if executor == "dagster":
        from basalt.dagster.cli_ext import _dagster_run

        result = _dagster_run(
            pipeline=str(pipeline) if pipeline else None,
            job=job,
            partition=partition,
            run_id=run_id,
            instance_ref_file=instance_ref_file,
        )
        return {"executor": "dagster", "result": _to_plain(result)}

    raise ValueError(f"Unsupported executor for run_job: {executor}")


def recent_runs(
    *,
    executor: str = "bmll",
    limit: int = 20,
    status: str | None = None,
    dagster_home: str | None = None,
) -> dict[str, Any]:
    executor = str(executor).strip().lower()
    rows: list[dict[str, Any]] = []

    if executor in {"bmll", "bmll_jobs", "job", "ec2", "aws", "aws_ec2"}:
        from bmll import compute

        runs = compute.get_job_runs(state=status) if status else compute.get_job_runs()
        for run in list(runs)[: int(limit)]:
            rows.append(
                {
                    "id": getattr(run, "id", None),
                    "name": getattr(run, "name", None),
                    "status": getattr(run, "state", None) or getattr(run, "status", None),
                    "started_at": getattr(run, "start_time", None),
                    "ended_at": getattr(run, "end_time", None),
                }
            )
        normalized = "bmll" if executor in {"bmll", "bmll_jobs", "job"} else "ec2"
        return {"executor": normalized, "runs": rows}

    if executor == "dagster":
        import os
        from dagster import DagsterInstance

        if dagster_home:
            os.environ["DAGSTER_HOME"] = dagster_home
        instance = DagsterInstance.get()
        runs = instance.get_runs(limit=int(limit))
        for run in runs:
            run_status = str(getattr(run, "status", ""))
            if status and status.lower() not in run_status.lower():
                continue
            rows.append(
                {
                    "run_id": getattr(run, "run_id", None),
                    "job_name": getattr(run, "job_name", None),
                    "status": run_status,
                    "start_time": getattr(run, "start_time", None),
                    "end_time": getattr(run, "end_time", None),
                }
            )
        return {"executor": "dagster", "runs": rows}

    if executor in {"pipeline", "local"}:
        return {
            "executor": "pipeline",
            "runs": [],
            "note": "Local pipeline execution history is not persisted by default.",
        }

    raise ValueError(f"Unsupported executor for recent_runs: {executor}")


def materialized_partitions(
    *,
    asset_key: str,
    limit: int = 200,
    dagster_home: str | None = None,
) -> dict[str, Any]:
    import os
    from dagster import DagsterEventType, DagsterInstance, AssetKey, EventRecordsFilter

    if dagster_home:
        os.environ["DAGSTER_HOME"] = dagster_home

    parts = tuple(p for p in str(asset_key).split("/") if p)
    key = AssetKey(list(parts))
    instance = DagsterInstance.get()
    records = instance.get_event_records(
        EventRecordsFilter(DagsterEventType.ASSET_MATERIALIZATION, asset_key=key),
        limit=int(limit),
    )

    by_partition: dict[str, dict[str, Any]] = {}
    for record in records:
        event = record.event_log_entry
        dagster_event = getattr(event, "dagster_event", None)
        if not dagster_event:
            continue
        mat = dagster_event.step_materialization_data.materialization
        partition = mat.partition or "<unpartitioned>"
        row = by_partition.setdefault(
            partition,
            {"partition": partition, "count": 0, "latest_timestamp": None},
        )
        row["count"] += 1
        ts = getattr(event, "timestamp", None)
        if ts is not None and (row["latest_timestamp"] is None or ts > row["latest_timestamp"]):
            row["latest_timestamp"] = ts

    return {
        "asset_key": "/".join(parts),
        "partitions": sorted(
            by_partition.values(),
            key=lambda r: (r["latest_timestamp"] or 0),
            reverse=True,
        ),
    }


def success_rate(
    *,
    executor: str = "bmll",
    limit: int = 100,
    status: str | None = None,
    dagster_home: str | None = None,
) -> dict[str, Any]:
    payload = recent_runs(
        executor=executor,
        limit=limit,
        status=status,
        dagster_home=dagster_home,
    )
    runs = payload.get("runs", [])
    success_states = {"success", "succeeded", "completed", "ok"}
    failure_states = {"failed", "failure", "canceled", "cancelled", "error"}
    successes = 0
    failures = 0
    for run in runs:
        value = str(run.get("status", "")).lower()
        if any(s in value for s in success_states):
            successes += 1
        elif any(s in value for s in failure_states):
            failures += 1
    total = len(runs)
    return {
        "executor": payload.get("executor"),
        "lookback": total,
        "successes": successes,
        "failures": failures,
        "success_rate": (successes / total) if total else None,
    }


def optimize_run(
    *,
    pipeline: str,
    search_space: dict[str, Any] | None = None,
    search_space_file: str | None = None,
    trials: int = 20,
    executor: str = "direct",
    score_fn: str | None = None,
    maximize: bool = True,
    seed: int = 0,
    output_dir: str = "optimize_results",
    tracker: str = "none",
    tracker_project: str | None = None,
    tracker_experiment: str | None = None,
    tracker_run_name: str | None = None,
    tracker_tags: dict[str, Any] | str | None = None,
    tracker_uri: str | None = None,
    tracker_mode: str | None = None,
    model_factory: str | None = None,
    dataset_builder: str | None = None,
    objectives: str | None = None,
    objective: str | None = None,
    use_aggregate: bool = False,
    search_generator: str | None = None,
    instance_size: int | None = None,
    delete_after: bool | None = None,
) -> dict[str, Any]:
    from basalt.optimize.core import optimize_pipeline
    from basalt.optimize.cli_ext import _load_search_space

    resolved_space = _load_search_space(
        search_space=search_space,
        search_space_file=search_space_file,
    )
    result = optimize_pipeline(
        pipeline=pipeline,
        search_space=resolved_space,
        trials=trials,
        score_fn=score_fn,
        maximize=maximize,
        seed=seed,
        output_dir=output_dir,
        executor=executor,
        instance_size=instance_size,
        delete_after=delete_after,
        tracker=tracker,
        tracker_project=tracker_project,
        tracker_experiment=tracker_experiment,
        tracker_run_name=tracker_run_name,
        tracker_tags=tracker_tags,
        tracker_uri=tracker_uri,
        tracker_mode=tracker_mode,
        model_factory=model_factory,
        dataset_builder=dataset_builder,
        objectives=objectives,
        objective=objective,
        use_aggregate=use_aggregate,
        search_generator=search_generator,
    )
    return {"optimize": _to_plain(result)}


def optimize_summary(
    *,
    output_dir: str = "optimize_results",
) -> dict[str, Any]:
    out = Path(output_dir)
    summary_file = out / "summary.json"
    trials_file = out / "trials.jsonl"
    if not summary_file.exists():
        return {
            "output_dir": str(out),
            "exists": False,
            "message": f"Missing summary file: {summary_file}",
        }
    summary = json.loads(summary_file.read_text(encoding="utf-8"))
    total_trials = 0
    if trials_file.exists():
        with trials_file.open("r", encoding="utf-8") as fh:
            for _ in fh:
                total_trials += 1
    return {
        "output_dir": str(out),
        "exists": True,
        "summary": summary,
        "trial_rows": total_trials,
    }
