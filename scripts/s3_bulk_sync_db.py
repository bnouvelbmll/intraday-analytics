#!/usr/bin/env python3
"""
Unsafe fast-path bulk writer for Dagster event log storage.
Directly inserts event rows + asset index rows using SQLAlchemy.

Pinned to Dagster 1.12.x schema. Use at your own risk.
"""
import datetime as dt
import logging
import os
import time
from typing import Iterable

import fire
from tqdm import tqdm

from dagster import DagsterInstance, AssetKey, AssetMaterialization, AssetObservation
from dagster._core.events import (
    DagsterEvent,
    DagsterEventType,
    StepMaterializationData,
    AssetObservationData,
)
from dagster._serdes import deserialize_value
from dagster._core.events.log import EventLogEntry
from dagster._core.instance.utils import RUNLESS_JOB_NAME, RUNLESS_RUN_ID
from dagster._core.storage.event_log.schema import (
    SqlEventLogStorageTable,
    AssetKeyTable,
    AssetEventTagsTable,
)
from dagster._core.storage.event_log.sql_event_log import SqlEventLogStorage

from intraday_analytics.tables import ALL_TABLES
from intraday_analytics.dagster_compat import (
    _parse_table_s3_path,
    _s3_list_all_objects,
    _s3_table_root_prefix,
    _iter_objects_newest_first,
)


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


def _asset_step_key(asset_event) -> str | None:
    if getattr(asset_event, "asset_key", None):
        step_key = f"asset:{asset_event.asset_key.to_user_string()}"
        if getattr(asset_event, "partition", None):
            step_key = f"{step_key}:{asset_event.partition}"
        return step_key
    return None


def _build_dagster_event(asset_event, job_name: str, step_key: str | None) -> DagsterEvent:
    if isinstance(asset_event, AssetMaterialization):
        event_type = DagsterEventType.ASSET_MATERIALIZATION.value
        data_payload = StepMaterializationData(asset_event)
    elif isinstance(asset_event, AssetObservation):
        event_type = DagsterEventType.ASSET_OBSERVATION.value
        data_payload = AssetObservationData(asset_event)
    else:
        raise ValueError(f"Unsupported event type {type(asset_event)}")

    return DagsterEvent(
        event_type_value=event_type,
        event_specific_data=data_payload,
        job_name=job_name,
        step_key=step_key,
    )


def _build_event(asset_event, ts: float, run_id: str, job_name: str) -> EventLogEntry:
    if isinstance(asset_event, AssetMaterialization):
        event_type = DagsterEventType.ASSET_MATERIALIZATION.value
        data_payload = StepMaterializationData(asset_event)
    elif isinstance(asset_event, AssetObservation):
        event_type = DagsterEventType.ASSET_OBSERVATION.value
        data_payload = AssetObservationData(asset_event)
    else:
        raise ValueError(f"Unsupported event type {type(asset_event)}")

    step_key = _asset_step_key(asset_event)
    dagster_event = _build_dagster_event(asset_event, job_name, step_key)
    return EventLogEntry(
        user_message="",
        level=logging.INFO,
        job_name=job_name,
        run_id=run_id,
        error_info=None,
        timestamp=ts,
        step_key=dagster_event.step_key,
        dagster_event=dagster_event,
    )


def _build_event_from_seed(
    seed: EventLogEntry,
    asset_event,
    ts: float,
    run_id: str,
    job_name: str,
) -> EventLogEntry:
    step_key = _asset_step_key(asset_event)
    dagster_event = seed.dagster_event._replace(
        event_type_value=seed.dagster_event.event_type_value,
        event_specific_data=seed.dagster_event.event_specific_data,
        job_name=job_name,
        step_key=step_key,
    )
    if isinstance(asset_event, AssetMaterialization):
        dagster_event = dagster_event._replace(
            event_type_value=DagsterEventType.ASSET_MATERIALIZATION.value,
            event_specific_data=StepMaterializationData(asset_event),
        )
    elif isinstance(asset_event, AssetObservation):
        dagster_event = dagster_event._replace(
            event_type_value=DagsterEventType.ASSET_OBSERVATION.value,
            event_specific_data=AssetObservationData(asset_event),
        )
    else:
        raise ValueError(f"Unsupported event type {type(asset_event)}")

    return seed._replace(
        run_id=run_id,
        job_name=job_name,
        timestamp=ts,
        step_key=step_key,
        dagster_event=dagster_event,
    )


def _fetch_latest_event_entry(
    storage: SqlEventLogStorage,
    *,
    run_id: str,
    asset_key_str: str,
    event_type: str,
) -> EventLogEntry | None:
    with storage.run_connection(run_id) as conn:
        row = conn.execute(
            SqlEventLogStorageTable.select()
            .where(
                SqlEventLogStorageTable.c.asset_key == asset_key_str,
                SqlEventLogStorageTable.c.dagster_event_type == event_type,
            )
            .order_by(SqlEventLogStorageTable.c.id.desc())
            .limit(1)
        ).fetchone()
    if not row:
        return None
    return deserialize_value(row.event)


def _seed_with_api(
    instance: DagsterInstance,
    storage: SqlEventLogStorage,
    asset_event,
    run_id: str,
    job_name: str,
    ts: float,
) -> EventLogEntry | None:
    if run_id == RUNLESS_RUN_ID:
        instance.report_runless_asset_event(asset_event)
    else:
        dagster_event = _build_dagster_event(
            asset_event, job_name, _asset_step_key(asset_event)
        )
        instance.report_dagster_event(
            dagster_event=dagster_event,
            run_id=run_id,
            timestamp=ts,
        )
    return _fetch_latest_event_entry(
        storage,
        run_id=run_id,
        asset_key_str=asset_event.asset_key.to_string(),
        event_type=(
            DagsterEventType.ASSET_MATERIALIZATION.value
            if isinstance(asset_event, AssetMaterialization)
            else DagsterEventType.ASSET_OBSERVATION.value
        ),
    )


def _insert_events(
    storage: SqlEventLogStorage,
    events: list[EventLogEntry],
    run_id: str,
):
    if not events:
        return []

    rows = [storage._event_to_row(evt) for evt in events]

    with storage.run_connection(run_id) as conn:
        dialect = conn.engine.dialect.name
        if dialect == "postgresql":
            insert_stmt = SqlEventLogStorageTable.insert().returning(
                SqlEventLogStorageTable.c.id
            )
            result = conn.execute(insert_stmt, rows)
            event_ids = [row[0] for row in result.fetchall()]
        else:
            # SQLite: insert one-by-one to retrieve ids, but in a single transaction
            event_ids = []
            for row in rows:
                result = conn.execute(SqlEventLogStorageTable.insert().values(**row))
                event_ids.append(result.inserted_primary_key[0])

    return event_ids


def _update_asset_indexes(
    storage: SqlEventLogStorage,
    events: list[EventLogEntry],
    event_ids: list[int],
):
    if not events:
        return
    has_index_cols = storage.has_asset_key_index_cols()
    tag_rows = []

    from sqlalchemy import or_

    with storage.index_connection() as conn:
        for event, event_id in zip(events, event_ids):
            if not (event.is_dagster_event and event.dagster_event.asset_key):
                continue
            values = storage._get_asset_entry_values(event, event_id, has_index_cols)
            if values:
                insert_stmt = AssetKeyTable.insert().values(
                    asset_key=event.dagster_event.asset_key.to_string(), **values
                )
                
                # Only update if the new event is more recent than what's in the DB
                update_condition = (
                    AssetKeyTable.c.asset_key == event.dagster_event.asset_key.to_string()
                )
                
                if "last_materialization_timestamp" in values:
                    new_ts = values["last_materialization_timestamp"]
                    update_condition = update_condition & or_(
                        AssetKeyTable.c.last_materialization_timestamp.is_(None),
                        AssetKeyTable.c.last_materialization_timestamp < new_ts
                    )
                
                if "last_observation_timestamp" in values:
                    new_ts = values["last_observation_timestamp"]
                    update_condition = update_condition & or_(
                        AssetKeyTable.c.last_observation_timestamp.is_(None),
                        AssetKeyTable.c.last_observation_timestamp < new_ts
                    )

                update_stmt = (
                    AssetKeyTable.update()
                    .values(**values)
                    .where(update_condition)
                )
                try:
                    conn.execute(insert_stmt)
                except Exception:
                    conn.execute(update_stmt)

            for key, value in storage._tags_for_asset_event(event).items():
                tag_rows.append(
                    {
                        "event_id": event_id,
                        "asset_key": event.dagster_event.asset_key.to_string(),
                        "key": key,
                        "value": value,
                        "event_timestamp": storage._event_insert_timestamp(event),
                    }
                )

        if tag_rows and storage.has_table(AssetEventTagsTable.name):
            conn.execute(AssetEventTagsTable.insert(), tag_rows)


def sync(
    tables: str | None = "all",
    start_date: str | None = None,
    end_date: str | None = None,
    emit_observations: bool = True,
    emit_materializations: bool = True,
    force_refresh: bool = False,
    batch_size: int = 500,
    run_id: str | None = None,
    job_name: str | None = None,
    seed_with_api: bool = False,
):
    """
    Bulk sync S3 objects into Dagster event log via direct DB writes.

    Args:
        tables: comma-separated list (e.g. "l2,l3,trades"), or "all".
        start_date: inclusive YYYY-MM-DD. Defaults to last 30 days.
        end_date: inclusive YYYY-MM-DD. Defaults to yesterday.
        emit_observations: emit AssetObservation events.
        emit_materializations: emit AssetMaterialization events.
        force_refresh: bypass cache for S3 listings.
        batch_size: number of event log entries per insert batch.
        run_id: optional Dagster run id to associate events with.
        job_name: optional job name for inserted events.
        seed_with_api: use Dagster API once per event type to seed templates.
    """
    if not start_date or not end_date:
        start_date, end_date = _date_range_default()

    dagster_home = os.getenv("DAGSTER_HOME", "/tmp/dagster_test")
    os.environ.setdefault("DAGSTER_HOME", dagster_home)
    os.makedirs(dagster_home, exist_ok=True)
    instance = DagsterInstance.get()
    storage = instance.event_log_storage
    if not isinstance(storage, SqlEventLogStorage):
        raise RuntimeError("Event log storage is not SQL-backed.")

    table_list = _parse_tables(tables)
    total_emitted = 0
    target_run_id = run_id if run_id is not None else RUNLESS_RUN_ID
    target_job_name = job_name if job_name is not None else RUNLESS_JOB_NAME

    if target_run_id != RUNLESS_RUN_ID:
        if instance.get_run_by_id(target_run_id) is None:
            print(f"warning: run_id {target_run_id} not found in run storage")

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

        batch: list[EventLogEntry] = []
        seed_templates: dict[str, EventLogEntry] = {}
        for path, meta, mic, date_key in tqdm(
            filtered,
            desc=f"sync {table_name}",
        ):
            partition = f"{date_key}|{mic}"
            metadata = {
                "s3_path": path,
                "size_bytes": meta.get("size_bytes"),
                "last_modified": meta.get("last_modified"),
                "source": "s3_bulk_sync_db",
            }
            ts = time.time()
            if emit_observations:
                obs = AssetObservation(
                    asset_key=asset_key, partition=partition, metadata=metadata
                )
                if seed_with_api and DagsterEventType.ASSET_OBSERVATION.value not in seed_templates:
                    seed = _seed_with_api(
                        instance, storage, obs, target_run_id, target_job_name, ts
                    )
                    if seed:
                        seed_templates[DagsterEventType.ASSET_OBSERVATION.value] = seed
                        total_emitted += 1
                        obs = None
                if obs is not None:
                    if DagsterEventType.ASSET_OBSERVATION.value in seed_templates:
                        batch.append(
                            _build_event_from_seed(
                                seed_templates[DagsterEventType.ASSET_OBSERVATION.value],
                                obs,
                                ts,
                                target_run_id,
                                target_job_name,
                            )
                        )
                    else:
                        batch.append(_build_event(obs, ts, target_run_id, target_job_name))
            if emit_materializations:
                mat = AssetMaterialization(
                    asset_key=asset_key, partition=partition, metadata=metadata
                )
                if (
                    seed_with_api
                    and DagsterEventType.ASSET_MATERIALIZATION.value not in seed_templates
                ):
                    seed = _seed_with_api(
                        instance, storage, mat, target_run_id, target_job_name, ts
                    )
                    if seed:
                        seed_templates[DagsterEventType.ASSET_MATERIALIZATION.value] = seed
                        total_emitted += 1
                        mat = None
                if mat is not None:
                    if DagsterEventType.ASSET_MATERIALIZATION.value in seed_templates:
                        batch.append(
                            _build_event_from_seed(
                                seed_templates[
                                    DagsterEventType.ASSET_MATERIALIZATION.value
                                ],
                                mat,
                                ts,
                                target_run_id,
                                target_job_name,
                            )
                        )
                    else:
                        batch.append(_build_event(mat, ts, target_run_id, target_job_name))

            if len(batch) >= batch_size:
                event_ids = _insert_events(storage, batch, target_run_id)
                _update_asset_indexes(storage, batch, event_ids)
                total_emitted += len(batch)
                batch = []

        if batch:
            event_ids = _insert_events(storage, batch, target_run_id)
            _update_asset_indexes(storage, batch, event_ids)
            total_emitted += len(batch)

    print(f"emitted events: {total_emitted}")


if __name__ == "__main__":
    fire.Fire(sync)
