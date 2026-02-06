#!/usr/bin/env python3
"""
Inspect Dagster event log storage (SQLite or Postgres) for runless asset events.
"""
from __future__ import annotations

import os
from typing import Iterable

import fire
from sqlalchemy import func, select

from dagster import DagsterInstance, AssetKey
from dagster._core.instance.utils import RUNLESS_RUN_ID
from dagster._core.storage.event_log.schema import (
    SqlEventLogStorageTable,
    AssetKeyTable,
    AssetEventTagsTable,
)
from dagster._core.storage.event_log.sql_event_log import SqlEventLogStorage


def _ensure_dagster_home(dagster_home: str | None) -> str:
    resolved = dagster_home or os.getenv("DAGSTER_HOME", "/tmp/dagster_test")
    os.environ.setdefault("DAGSTER_HOME", resolved)
    os.makedirs(resolved, exist_ok=True)
    return resolved


def _print_storage_debug(storage: SqlEventLogStorage) -> None:
    print(f"storage_type: {type(storage).__name__}")
    for attr in (
        "_conn_string",
        "_index_conn_string",
        "_db_path",
        "_index_db_path",
        "_base_dir",
        "_db_dir",
    ):
        if hasattr(storage, attr):
            print(f"{attr}: {getattr(storage, attr)}")


def inspect_db(
    asset_key: str | None = None,
    limit: int = 10,
    run_id: str = RUNLESS_RUN_ID,
    dagster_home: str | None = None,
):
    """
    Inspect event log and asset index tables for a given run_id (default: RUNLESS).

    Args:
        asset_key: asset key string (use AssetKey.to_string()) to filter.
        limit: number of recent events to display for asset_key.
        run_id: event log shard/run_id (SQLite uses run_id shards).
        dagster_home: override DAGSTER_HOME.
    """
    resolved_home = _ensure_dagster_home(dagster_home)
    print(f"DAGSTER_HOME: {resolved_home}")

    instance = DagsterInstance.get()
    storage = instance.event_log_storage
    if not isinstance(storage, SqlEventLogStorage):
        raise RuntimeError("Event log storage is not SQL-backed.")

    _print_storage_debug(storage)

    if asset_key and not asset_key.strip().startswith("["):
        asset_key = AssetKey.from_user_string(asset_key).to_string()

    with storage.run_connection(run_id) as conn:
        total = conn.execute(
            select(func.count()).select_from(SqlEventLogStorageTable)
        ).scalar_one()
        print(f"event_log_count[{run_id}]: {total}")

        by_type = conn.execute(
            select(
                SqlEventLogStorageTable.c.dagster_event_type,
                func.count(),
            ).group_by(SqlEventLogStorageTable.c.dagster_event_type)
        ).fetchall()
        if by_type:
            print("event_log_by_type:")
            for event_type, count in by_type:
                print(f"  {event_type}: {count}")

        if asset_key:
            rows = conn.execute(
                select(
                    SqlEventLogStorageTable.c.id,
                    SqlEventLogStorageTable.c.dagster_event_type,
                    SqlEventLogStorageTable.c.timestamp,
                    SqlEventLogStorageTable.c.partition,
                )
                .where(SqlEventLogStorageTable.c.asset_key == asset_key)
                .order_by(SqlEventLogStorageTable.c.timestamp.desc())
                .limit(limit)
            ).fetchall()
            print(f"recent_events_for_asset_key[{asset_key}]: {len(rows)}")
            for row in rows:
                print(
                    f"  id={row.id} type={row.dagster_event_type} "
                    f"ts={row.timestamp} partition={row.partition}"
                )

    with storage.index_connection() as conn:
        asset_count = conn.execute(
            select(func.count()).select_from(AssetKeyTable)
        ).scalar_one()
        print(f"asset_key_count: {asset_count}")

        if asset_key:
            row = conn.execute(
                select(AssetKeyTable).where(AssetKeyTable.c.asset_key == asset_key)
            ).fetchone()
            if row is None:
                print(f"asset_key_row[{asset_key}]: not found")
            else:
                print(
                    "asset_key_row[{}]: last_materialization_timestamp={} last_run_id={}".format(
                        asset_key,
                        row.last_materialization_timestamp,
                        row.last_run_id,
                    )
                )

        if storage.has_table(AssetEventTagsTable.name):
            tag_count = conn.execute(
                select(func.count()).select_from(AssetEventTagsTable)
            ).scalar_one()
            print(f"asset_event_tags_count: {tag_count}")


if __name__ == "__main__":
    fire.Fire(inspect_db)
