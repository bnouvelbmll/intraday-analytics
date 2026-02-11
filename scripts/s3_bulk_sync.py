#!/usr/bin/env python3
import datetime as dt
import os
from typing import Iterable

import fire
from tqdm import tqdm

from dagster import DagsterInstance, AssetKey, AssetMaterialization, AssetObservation

from basalt.tables import ALL_TABLES
from basalt.dagster.dagster_compat import (
    _parse_table_s3_path,
    _s3_list_all_objects,
    _s3_table_root_prefix,
    _iter_objects_newest_first,
)


def _disable_fire_pager() -> None:
    if "PAGER" not in os.environ:
        os.environ["PAGER"] = "cat"


def _date_range_default():
    today = dt.date.today()
    end = today - dt.timedelta(days=1)
    start = end - dt.timedelta(days=30)
    return start.isoformat(), end.isoformat()


def _date_in_range(date_key: str, start: str, end: str) -> bool:
    try:
        value = dt.date.fromisoformat(date_key)
    except Exception:
        return False
    return dt.date.fromisoformat(start) <= value <= dt.date.fromisoformat(end)


def _parse_tables(tables: str | None) -> list[str]:
    if not tables or tables.lower() == "all":
        return list(ALL_TABLES.keys())
    return [t.strip() for t in tables.split(",") if t.strip()]


def _emit_events(
    instance: DagsterInstance,
    *,
    asset_key: str,
    partition: str,
    metadata: dict,
    emit_observations: bool,
    emit_materializations: bool,
):
    if emit_observations:
        instance.report_runless_asset_event(
            AssetObservation(
                asset_key=asset_key, partition=partition, metadata=metadata
            )
        )
    if emit_materializations:
        instance.report_runless_asset_event(
            AssetMaterialization(
                asset_key=asset_key, partition=partition, metadata=metadata
            )
        )


def sync(
    tables: str | None = "all",
    start_date: str | None = None,
    end_date: str | None = None,
    emit_observations: bool = True,
    emit_materializations: bool = True,
    force_refresh: bool = False,
):
    """
    Bulk sync S3 objects into Dagster event log as observations/materializations.

    Args:
        tables: comma-separated list (e.g. "l2,l3,trades"), or "all".
        start_date: inclusive YYYY-MM-DD. Defaults to last 30 days.
        end_date: inclusive YYYY-MM-DD. Defaults to yesterday.
        emit_observations: emit AssetObservation events.
        emit_materializations: emit AssetMaterialization events.
        force_refresh: bypass cache for S3 listings.
    """
    if not start_date or not end_date:
        start_date, end_date = _date_range_default()

    dagster_home = os.getenv("DAGSTER_HOME", "/tmp/dagster_test")
    os.environ.setdefault("DAGSTER_HOME", dagster_home)
    os.makedirs(dagster_home, exist_ok=True)
    instance = DagsterInstance.get()

    table_list = _parse_tables(tables)
    total_emitted = 0

    for table_name in table_list:
        table = ALL_TABLES.get(table_name)
        if not table:
            continue
        prefix = _s3_table_root_prefix(table)
        if not prefix:
            continue
        objects = _s3_list_all_objects(prefix, force_refresh=force_refresh)

        filtered: list[tuple[str, dict, str, str]] = []
        for path, meta in _iter_objects_newest_first(objects):
            parsed = _parse_table_s3_path(table, path)
            if not parsed:
                continue
            mic, date_key = parsed
            if _date_in_range(date_key, start_date, end_date):
                filtered.append((path, meta, mic, date_key))

        asset_key = AssetKey(["BMLL", table_name])
        for path, meta, mic, date_key in tqdm(
            filtered,
            desc=f"sync {table_name}",
        ):
            partition = f"{date_key}|{mic}"
            metadata = {
                "s3_path": path,
                "size_bytes": meta.get("size_bytes"),
                "last_modified": meta.get("last_modified"),
                "source": "s3_bulk_sync",
            }
            _emit_events(
                instance,
                asset_key=asset_key,
                partition=partition,
                metadata=metadata,
                emit_observations=emit_observations,
                emit_materializations=emit_materializations,
            )
            total_emitted += 1

    print(f"emitted events for {total_emitted} partitions")


if __name__ == "__main__":
    _disable_fire_pager()
    fire.Fire(sync)
