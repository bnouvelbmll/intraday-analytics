#!/usr/bin/env python3
"""
Integration test for direct DB injection of external asset materializations.

Creates an isolated DAGSTER_HOME, injects multipartition materializations via
the DB fast-path, and verifies Dagster can see the partitions via the high-level
instance API. If DB injection fails, it emits the same events via the proper
API path and prints a comparison of stored events.
"""
from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Iterable

from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    DailyPartitionsDefinition,
    MultiPartitionKey,
    StaticPartitionsDefinition,
)
from dagster._core.instance.utils import RUNLESS_JOB_NAME, RUNLESS_RUN_ID
from dagster._core.storage.event_log.schema import SqlEventLogStorageTable
from dagster._core.storage.event_log.sql_event_log import SqlEventLogStorage
from dagster._serdes import deserialize_value
from sqlalchemy import select

# Allow importing from repo root when run from scripts/
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.s3_bulk_sync_db import (  # noqa: E402
    _build_event,
    _insert_events,
    _update_asset_indexes,
)
from intraday_analytics.dagster_compat import (  # noqa: E402
    build_input_source_assets,
)


def _build_partitions(
    *,
    date_dim: str,
    mic_dim: str,
    dates: Iterable[str],
    mics: Iterable[str],
) -> list[str]:
    partitions = []
    for date_key in dates:
        for mic in mics:
            partitions.append(
                str(MultiPartitionKey({date_dim: date_key, mic_dim: mic}))
            )
    return partitions


def _event_summary(entry) -> dict:
    de = entry.dagster_event
    mat = None
    if de and getattr(de, "event_specific_data", None):
        mat = getattr(de.event_specific_data, "materialization", None)
    return {
        "run_id": entry.run_id,
        "job_name": entry.job_name,
        "event_type": de.event_type_value if de else None,
        "step_key": de.step_key if de else None,
        "asset_key": de.asset_key.to_string() if de and de.asset_key else None,
        "partition": de.partition if de else None,
        "metadata_keys": (
            sorted(list(mat.metadata.keys())) if mat and mat.metadata else []
        ),
        "metadata_source": (
            mat.metadata.get("source").value
            if mat
            and mat.metadata
            and "source" in mat.metadata
            and getattr(mat.metadata.get("source"), "value", None) is not None
            else None
        ),
    }


def _load_events(storage: SqlEventLogStorage, asset_key: AssetKey):
    with storage.run_connection(RUNLESS_RUN_ID) as conn:
        rows = conn.execute(
            SqlEventLogStorageTable.select().where(
                SqlEventLogStorageTable.c.asset_key == asset_key.to_string()
            )
        ).fetchall()
    return [deserialize_value(row.event) for row in rows]


def _filter_by_source(events, source: str):
    filtered = []
    for entry in events:
        de = entry.dagster_event
        if not de or not getattr(de, "event_specific_data", None):
            continue
        mat = getattr(de.event_specific_data, "materialization", None)
        if not mat or not mat.metadata or "source" not in mat.metadata:
            continue
        meta_value = mat.metadata.get("source")
        meta_str = getattr(meta_value, "value", None) or str(meta_value)
        if meta_str == source:
            filtered.append(entry)
    return filtered


def main() -> int:
    tmp_dir = tempfile.mkdtemp(prefix="dagster_bulk_sync_test_")
    os.environ["DAGSTER_HOME"] = tmp_dir
    print(f"DAGSTER_HOME={tmp_dir}")

    instance = DagsterInstance.get()
    storage = instance.event_log_storage
    if not isinstance(storage, SqlEventLogStorage):
        print("ERROR: event log storage is not SQL-backed")
        return 2

    asset_key = AssetKey(["BMLL", "l2"])
    partitions = _build_partitions(
        date_dim="date",
        mic_dim="mic",
        dates=["2020-01-01", "2020-01-02"],
        mics=["XNAS", "XLON"],
    )

    # Asset-graph check: ensure the asset exists and partition keys are valid
    # DailyPartitionsDefinition uses an exclusive end_date; set to 2020-01-03 to cover 1st + 2nd.
    date_parts = DailyPartitionsDefinition(
        start_date="2020-01-01", end_date="2020-01-03"
    )
    mic_parts = StaticPartitionsDefinition(["XNAS", "XLON"])
    assets, _ = build_input_source_assets(
        tables=["l2"],
        date_partitions_def=date_parts,
        mic_partitions_def=mic_parts,
        asset_key_prefix=["BMLL"],
        date_dim="date",
        mic_dim="mic",
        group_name="BMLL",
    )
    asset_map = {a.key: a for a in assets}
    if asset_key not in asset_map:
        print("ERROR: asset key not found in asset graph:", asset_key.to_string())
        return 5
    partitions_def = asset_map[asset_key].partitions_def
    if partitions_def is None:
        print("ERROR: asset has no partitions_def but partitions were injected")
        return 6
    invalid = [p for p in partitions if not partitions_def.has_partition_key(p)]
    if invalid:
        print("ERROR: injected partitions not valid for partitions_def:")
        for part in invalid:
            print(f"  {part}")
        return 7

    # DB injection path
    events = []
    ts = time.time()
    for partition in partitions:
        mat = AssetMaterialization(
            asset_key=asset_key,
            partition=partition,
            metadata={"source": "db_test"},
        )
        events.append(_build_event(mat, ts, RUNLESS_RUN_ID, RUNLESS_JOB_NAME))

    event_ids = _insert_events(storage, events, RUNLESS_RUN_ID)
    _update_asset_indexes(storage, events, event_ids)

    materialized = instance.get_materialized_partitions(asset_key)
    expected = set(partitions)
    missing = expected - set(materialized)
    if not missing:
        print("PASS: DB-injected partitions visible via get_materialized_partitions")
        return 0

    print("FAIL: DB-injected partitions missing:")
    for part in sorted(missing):
        print(f"  {part}")

    # Proper API path for comparison
    for partition in partitions:
        mat = AssetMaterialization(
            asset_key=asset_key,
            partition=partition,
            metadata={"source": "api_test"},
        )
        instance.report_runless_asset_event(mat)

    materialized_api = instance.get_materialized_partitions(asset_key)
    missing_after_api = expected - set(materialized_api)
    if missing_after_api:
        print("ERROR: even API-injected partitions missing")
        for part in sorted(missing_after_api):
            print(f"  {part}")
        return 3

    # Compare stored events
    all_events = _load_events(storage, asset_key)
    db_events = _filter_by_source(all_events, "db_test")
    api_events = _filter_by_source(all_events, "api_test")

    if not db_events or not api_events:
        print("ERROR: could not locate db_test or api_test events for comparison")
        return 4

    print("Comparison (first DB vs first API event):")
    print("DB:", _event_summary(db_events[0]))
    print("API:", _event_summary(api_events[0]))
    print("Next step: align DB injection fields to match the API event summary.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
