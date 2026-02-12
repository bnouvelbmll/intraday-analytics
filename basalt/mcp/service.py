from __future__ import annotations

from pathlib import Path
import importlib.util
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


def list_capabilities() -> dict[str, Any]:
    available = {
        "pipeline": True,
        "ec2": importlib.util.find_spec("basalt.executors.aws_ec2") is not None,
        "k8s": importlib.util.find_spec("basalt.executors.kubernetes") is not None,
        "dagster": (
            importlib.util.find_spec("basalt.dagster") is not None
            and importlib.util.find_spec("dagster") is not None
        ),
        "fastmcp": importlib.util.find_spec("fastmcp") is not None,
    }
    return {"capabilities": available}


def _pipeline_yaml_path(pipeline: str) -> Path:
    return Path(pipeline).with_suffix(".yaml")


def configure_job(
    *,
    pipeline: str,
    executor: str = "ec2",
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

    if executor in {"ec2", "aws", "aws_ec2"}:
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
            "executor": "ec2",
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
    executor: str = "ec2",
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
    executor: str = "ec2",
    limit: int = 20,
    status: str | None = None,
    dagster_home: str | None = None,
) -> dict[str, Any]:
    executor = str(executor).strip().lower()
    rows: list[dict[str, Any]] = []

    if executor in {"ec2", "aws", "aws_ec2"}:
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
        return {"executor": "ec2", "runs": rows}

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
    executor: str = "ec2",
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
