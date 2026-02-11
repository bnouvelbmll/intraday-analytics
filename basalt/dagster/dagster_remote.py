from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Optional

from dagster import DagsterInstance
from dagster import AssetSelection, MultiPartitionKey, define_asset_job
from dagster._core.storage.tags import (
    PARTITION_NAME_TAG,
    get_multidimensional_partition_tag,
)
from dagster._core.instance.ref import InstanceRef
from dagster._grpc.types import ExecuteRunArgs


def run_dagster_remote(
    run_id: str,
    instance_ref_file: str,
    set_exit_code_on_failure: bool = True,
) -> int:
    instance_ref = InstanceRef.from_dict(
        json.loads(Path(instance_ref_file).read_text(encoding="utf-8"))
    )
    with DagsterInstance.from_ref(instance_ref) as instance:
        run = instance.get_run_by_id(run_id)
        if run is None:
            raise RuntimeError(f"Run not found: {run_id}")
        job_origin = getattr(run, "job_code_origin", None)
        if job_origin is None:
            remote_origin = getattr(run, "remote_job_origin", None)
            if remote_origin is not None and hasattr(remote_origin, "job_origin"):
                job_origin = remote_origin.job_origin
        if job_origin is None:
            raise RuntimeError("Run does not have a job origin; cannot execute remotely.")

        args = ExecuteRunArgs(
            job_origin=job_origin,
            run_id=run_id,
            instance_ref=instance_ref,
            set_exit_code_on_failure=set_exit_code_on_failure,
        )
        cmd = list(args.get_command_args())
    result = subprocess.run(cmd, check=False)
    return result.returncode


def _load_module_from_file(path: str):
    import importlib.util

    file_path = Path(path).resolve()
    spec = importlib.util.spec_from_file_location(file_path.stem, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


def _parse_partition_spec(partition: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    if not partition:
        return None, None
    if "|" in partition:
        items = partition.split("|")
        return items[0], items[1] if len(items) > 1 else None
    parts = {}
    for item in partition.split(","):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        parts[key.strip()] = value.strip()
    return parts.get("date"), parts.get("universe")


def run_dagster_pipeline(
    pipeline_file: str,
    job_name: Optional[str] = None,
    partition: Optional[str] = None,
    tags: Optional[dict[str, str]] = None,
) -> int:
    from dagster import DailyPartitionsDefinition, StaticPartitionsDefinition

    from basalt.dagster.dagster_compat import (
        build_assets_for_module,
        default_universes,
        _first_universe_spec,
    )

    module = _load_module_from_file(pipeline_file)
    base_config = getattr(module, "USER_CONFIG", {}) if hasattr(module, "USER_CONFIG") else {}
    base_name = Path(pipeline_file).stem
    pkg_name = module.__name__.split(".")[0]
    asset_base = f"{pkg_name}{base_name}" if base_name and base_name[0].isdigit() else base_name

    date_value, universe_value = _parse_partition_spec(partition)
    if not (date_value and universe_value):
        default_universe = _first_universe_spec(
            getattr(module, "UNIVERSES", None) or default_universes()
        )
        if date_value is None:
            date_value = (
                base_config.get("START_DATE")
                if base_config.get("START_DATE") == base_config.get("END_DATE")
                else base_config.get("START_DATE")
            )
        if universe_value is None:
            universe_value = default_universe
    partitions_def = None
    if date_value and universe_value:
        date_partitions = StaticPartitionsDefinition([date_value])
        universe_partitions = StaticPartitionsDefinition([universe_value])
        from dagster import MultiPartitionsDefinition

        partitions_def = MultiPartitionsDefinition(
            {"date": date_partitions, "universe": universe_partitions}
        )

    assets = build_assets_for_module(
        module=module,
        partitions_def=partitions_def,
        universe_dim="universe",
        date_dim="date",
    )
    selection = AssetSelection.groups(asset_base)
    job_def = define_asset_job(
        name=job_name or f"{asset_base}__job",
        selection=selection,
        partitions_def=partitions_def,
    )

    instance = DagsterInstance.get()
    run_tags = dict(tags or {})
    partition_key = None
    if date_value and universe_value:
        partition_key = MultiPartitionKey({"date": date_value, "universe": universe_value})
        run_tags[PARTITION_NAME_TAG] = partition_key.to_string()
        run_tags[get_multidimensional_partition_tag("date")] = date_value
        run_tags[get_multidimensional_partition_tag("universe")] = universe_value

    result = job_def.execute_in_process(
        instance=instance,
        partition_key=partition_key,
        tags=run_tags,
        raise_on_error=True,
    )
    return 0 if result.success else 1
