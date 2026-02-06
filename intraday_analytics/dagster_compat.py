from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import json
import logging
import os
import time
from hashlib import sha1
from typing import Callable, Optional, Sequence

from intraday_analytics.tables import ALL_TABLES
from intraday_analytics.utils import filter_existing_s3_files

from intraday_analytics.configuration import AnalyticsConfig
from intraday_analytics.execution import run_multiday_pipeline
from intraday_analytics.cli import _load_universe_override


@dataclass(frozen=True)
class UniversePartition:
    """
    A universe partition backed by a CLI-style universe selector.
    """

    name: str
    value: str | None = None

    @property
    def spec(self) -> str:
        return f"{self.name}={self.value}" if self.value is not None else self.name


def parse_universe_spec(spec: str) -> UniversePartition:
    if "=" in spec:
        name, value = spec.split("=", 1)
        return UniversePartition(name=name, value=value)
    return UniversePartition(name=spec, value=None)


def parse_date_key(key: str) -> DatePartition:
    if "_" in key:
        start, end = key.split("_", 1)
        return DatePartition(start_date=start, end_date=end)
    return DatePartition(start_date=key, end_date=key)


@dataclass(frozen=True)
class DatePartition:
    """
    A temporal partition (typically a day or range).
    """

    start_date: str
    end_date: str

    @property
    def key(self) -> str:
        if self.start_date == self.end_date:
            return self.start_date
        return f"{self.start_date}_{self.end_date}"


@dataclass(frozen=True)
class PartitionRun:
    universe: UniversePartition
    dates: DatePartition

    @property
    def key(self) -> str:
        return f"{self.universe.spec}:{self.dates.key}"


def build_partition_runs(
    universes: Sequence[UniversePartition],
    dates: Sequence[DatePartition],
) -> list[PartitionRun]:
    return [PartitionRun(u, d) for u in universes for d in dates]


def build_demo_assets(
    demo_pkg: str = "demo",
    partitions_def=None,
    universe_dim: str = "universe",
    date_dim: str = "date",
    split_passes: bool = False,
    input_asset_keys: Sequence | None = None,
):
    """
    Discover demo modules and return Dagster assets for each demo.
    """
    try:
        import importlib
        import pkgutil
        from dagster import asset
    except Exception as exc:
        raise ImportError("Dagster is required to build demo assets.") from exc

    demos = _discover_demo_modules(demo_pkg)

    assets = []
    for module in demos:
        name = module.__name__.split(".")[-1]
        asset_base = f"demo{name}" if name[0].isdigit() else name
        package_root = module.__name__.split(".")[0]
        base_config = module.USER_CONFIG
        passes = base_config.get("PASSES", []) if isinstance(base_config, dict) else []
        last_pass_key = None

        def _make_demo_asset(
            module, asset_name, asset_group, config_override, deps, key_prefix
        ):
            @asset(
                name=asset_name,
                partitions_def=partitions_def,
                group_name=asset_group,
                key_prefix=key_prefix,
                deps=deps,
            )
            def _demo_asset(context=None):
                base_config = config_override
                default_get_universe = module.get_universe
                get_pipeline = getattr(module, "get_pipeline", None)
                if context and getattr(context, "partition_key", None):
                    keys = getattr(context.partition_key, "keys_by_dimension", None)
                    if keys and universe_dim in keys and date_dim in keys:
                        partition = PartitionRun(
                            universe=parse_universe_spec(keys[universe_dim]),
                            dates=parse_date_key(keys[date_dim]),
                        )
                    else:
                        partition = PartitionRun(
                            universe=UniversePartition(name="default", value=None),
                            dates=DatePartition(
                                start_date=base_config["START_DATE"],
                                end_date=base_config["END_DATE"],
                            ),
                        )
                else:
                    partition = PartitionRun(
                        universe=UniversePartition(name="default", value=None),
                        dates=DatePartition(
                            start_date=base_config["START_DATE"],
                            end_date=base_config["END_DATE"],
                        ),
                    )

                run_partition(
                    base_config=base_config,
                    default_get_universe=default_get_universe,
                    partition=partition,
                    get_pipeline=get_pipeline,
                )

            return _demo_asset

        if split_passes and passes:
            for pass_config in passes:
                pass_name = _sanitize_name(pass_config.get("name", "pass"))
                asset_name = pass_name
                per_pass_config = {**base_config, "PASSES": [pass_config]}
                deps = _filter_input_deps(input_asset_keys, pass_config)
                if last_pass_key is not None:
                    deps.append(last_pass_key)
                assets.append(
                    _make_demo_asset(
                        module,
                        asset_name,
                        asset_base,
                        per_pass_config,
                        deps,
                        [package_root, asset_base],
                    )
                )
                last_pass_key = assets[-1].key
        else:
            deps = list(input_asset_keys or [])
            assets.append(
                _make_demo_asset(
                    module,
                    asset_base,
                    asset_base,
                    base_config,
                    deps,
                    [package_root],
                )
            )

    return assets


def build_demo_materialization_checks(
    demo_pkg: str = "demo",
    universe_dim: str = "universe",
    date_dim: str = "date",
    check_mode: str = "recursive",
    split_passes: bool = False,
):
    """
    Create Dagster asset checks that validate demo materializations by checking
    for output files in S3.
    """
    try:
        from dagster import AssetCheckResult, AssetKey, asset_check
    except Exception as exc:
        raise ImportError("Dagster is required to build demo asset checks.") from exc

    from intraday_analytics.process import get_final_s3_path

    demos = _discover_demo_modules(demo_pkg)
    checks = []

    for module in demos:
        name = module.__name__.split(".")[-1]
        asset_base = f"demo{name}" if name[0].isdigit() else name
        package_root = module.__name__.split(".")[0]
        base_config = module.USER_CONFIG
        passes = base_config.get("PASSES", []) if isinstance(base_config, dict) else []

        def _make_materialized_check(base_config, asset_key, pass_name: str | None):
            @asset_check(asset=asset_key, name="s3_materialized")
            def _materialized_check(context):
                logging.info(
                    "materialization check start asset=%s mode=%s",
                    asset_key,
                    check_mode,
                )
                keys = _safe_partition_keys(context)
                if keys and universe_dim in keys and date_dim in keys:
                    partition = PartitionRun(
                        universe=parse_universe_spec(keys[universe_dim]),
                        dates=parse_date_key(keys[date_dim]),
                    )
                else:
                    partition = PartitionRun(
                        universe=UniversePartition(name="default", value=None),
                        dates=DatePartition(
                            start_date=base_config["START_DATE"],
                            end_date=base_config["END_DATE"],
                        ),
                    )

                config = AnalyticsConfig(
                    **{
                        **base_config,
                        "START_DATE": partition.dates.start_date,
                        "END_DATE": partition.dates.end_date,
                    }
                )

                missing = []
                pass_list = config.PASSES
                if pass_name:
                    pass_list = [p for p in pass_list if p.name == pass_name]
                for pass_config in pass_list:
                    output_path = get_final_s3_path(
                        partition.dates.start_date,
                        partition.dates.end_date,
                        config,
                        pass_config.name,
                    )
                    if not _s3_path_exists(output_path, check_mode=check_mode):
                        missing.append(output_path)

                return AssetCheckResult(
                    passed=not missing,
                    metadata={
                        "missing": ", ".join(missing) if missing else "",
                        "check_mode": check_mode,
                    },
                )

            return _materialized_check

        if split_passes and passes:
            for pass_config in passes:
                pass_name = _sanitize_name(pass_config.get("name", "pass"))
                asset_key = AssetKey([package_root, asset_base, pass_name])
                checks.append(_make_materialized_check(base_config, asset_key, pass_name))
        else:
            checks.append(
                _make_materialized_check(
                    base_config, AssetKey([package_root, asset_base]), None
                )
            )

    return checks


def build_input_source_assets(
    *,
    tables: Sequence[str] | None = None,
    date_partitions_def=None,
    mic_partitions_def=None,
    cbbo_partitions_def=None,
    date_dim: str = "date",
    mic_dim: str = "mic",
    cbbo_dim: str = "cbbo",
    asset_key_prefix: Sequence[str] | None = None,
    group_name: str = "BMLL",
):
    """
    Build Dagster SourceAssets for raw input tables (l2, l3, trades, etc.).

    By default, tables are partitioned by (date x mic). The cbbo table can be
    partitioned by (date x cbbo) by providing cbbo_partitions_def.
    """
    try:
        from dagster import AssetKey, MultiPartitionsDefinition, SourceAsset
    except Exception as exc:
        raise ImportError("Dagster is required to build source assets.") from exc

    table_names = list(tables) if tables else list(ALL_TABLES.keys())
    assets = []
    asset_meta = {}

    for table_name in table_names:
        if table_name not in ALL_TABLES:
            continue
        table = ALL_TABLES[table_name]

        if table.name == "cbbo" and date_partitions_def and cbbo_partitions_def:
            partitions_def = MultiPartitionsDefinition(
                {date_dim: date_partitions_def, cbbo_dim: cbbo_partitions_def}
            )
            partitioning = "cbbo_date"
        elif date_partitions_def and mic_partitions_def:
            partitions_def = MultiPartitionsDefinition(
                {date_dim: date_partitions_def, mic_dim: mic_partitions_def}
            )
            partitioning = "mic_date"
        else:
            partitions_def = None
            partitioning = "none"

        key_path = [*asset_key_prefix, table.name] if asset_key_prefix else [table.name]
        asset_key = AssetKey(key_path)

        assets.append(
            SourceAsset(
                key=asset_key,
                partitions_def=partitions_def,
                description=f"External BMLL table: {table.name}",
                group_name=group_name,
                metadata={
                    "table": table.name,
                    "s3_folder": table.s3_folder_name,
                    "s3_prefix": table.s3_file_prefix,
                    "partitioning": partitioning,
                },
            )
        )
        asset_meta[asset_key] = table

    return assets, asset_meta


def build_s3_input_asset_checks(
    *,
    assets: Sequence,
    table_map: dict,
    date_dim: str = "date",
    mic_dim: str = "mic",
    cbbo_dim: str = "cbbo",
    check_mode: str = "recursive",
):
    """
    Create asset checks for external input assets by checking S3 for the
    expected file location.
    """
    try:
        from dagster import AssetCheckResult, asset_check
    except Exception as exc:
        raise ImportError("Dagster is required to build asset checks.") from exc

    checks = []

    for asset in assets:
        table = table_map.get(asset.key)
        if not table:
            continue

        def _make_input_check(table):
            @asset_check(asset=asset, name="s3_exists")
            def _input_check(context):
                logging.info(
                    "s3_exists check start asset=%s mode=%s",
                    asset.key,
                    check_mode,
                )
                keys = _safe_partition_keys(context)
                if not keys:
                    if check_mode == "recursive" and _full_check_on_unpartitioned():
                        prefix = _s3_table_root_prefix(table)
                        if not prefix:
                            return AssetCheckResult(
                                passed=False,
                                metadata={
                                    "reason": "partitioned asset check requires a partition key"
                                },
                            )
                        objects = _s3_list_all_objects(prefix)
                        logging.info(
                            "s3_exists unpartitioned recursive prefix=%s objects=%d",
                            prefix,
                            len(objects),
                        )
                        return AssetCheckResult(
                            passed=bool(objects),
                            metadata={
                                "reason": "checked table root recursively",
                                "prefix": prefix,
                                "object_count": len(objects),
                                "check_mode": check_mode,
                            },
                        )
                    logging.info(
                        "s3_exists skipped (no partition key) asset=%s",
                        asset.key,
                    )
                    return AssetCheckResult(
                        passed=True,
                        metadata={
                            "reason": "no partition key; check skipped",
                            "check_mode": check_mode,
                        },
                    )

                date_key = keys.get(date_dim)
                if not date_key or "_" in date_key:
                    return AssetCheckResult(
                        passed=False,
                        metadata={"reason": "input assets must use single-day partitions"},
                    )

                y, m, d = (int(part) for part in date_key.split("-"))

                if cbbo_dim in keys:
                    cbbo_value = keys[cbbo_dim]
                    s3_paths = table.get_s3_paths([cbbo_value], y, m, d)
                else:
                    mic = keys.get(mic_dim)
                    if not mic:
                        return AssetCheckResult(
                            passed=False,
                            metadata={"reason": "missing mic partition"},
                        )
                    s3_paths = table.get_s3_paths([mic], y, m, d)

                exists = all(_s3_path_exists(p, check_mode=check_mode) for p in s3_paths)
                logging.info(
                    "s3_exists partitioned asset=%s exists=%s paths=%s",
                    asset.key,
                    exists,
                    s3_paths,
                )
                return AssetCheckResult(
                    passed=exists,
                    metadata={
                        "paths": ", ".join(s3_paths),
                        "check_mode": check_mode,
                    },
                )

            return _input_check

        checks.append(_make_input_check(table))

    return checks


def build_s3_input_observation_sensor(
    *,
    name: str,
    assets: Sequence,
    table_map: dict,
    date_dim: str = "date",
    mic_dim: str = "mic",
    cbbo_dim: str = "cbbo",
    check_mode: str = "recursive",
    default_date: str | None = None,
    mics: Sequence[str] | None = None,
):
    """
    Create a lightweight sensor that observes external input assets in S3.
    The sensor checks a configured date (default: yesterday) and a limited set
    of MICs to avoid scanning large partitions.
    """
    try:
        from dagster import AssetObservation, MultiPartitionKey, SensorResult, sensor
    except Exception as exc:
        raise ImportError("Dagster is required to build sensors.") from exc

    @sensor(name=name, minimum_interval_seconds=300)
    def _sensor(context):
        date_key = (
            default_date
            or os.getenv("S3_SENSOR_DATE")
            or (dt.date.today() - dt.timedelta(days=1)).isoformat()
        )
        mic_env = os.getenv("S3_SENSOR_MICS", "")
        mic_list = list(mics) if mics is not None else [m for m in mic_env.split(",") if m]

        emit_all = os.getenv("S3_SENSOR_EMIT_ALL", "0") in {"1", "true", "True"}
        max_events = int(os.getenv("S3_SENSOR_MAX_OBSERVATIONS", "1000"))
        force_refresh = os.getenv("S3_SENSOR_FORCE_REFRESH", "0") in {"1", "true", "True"}

        events = []
        logging.info(
            "s3 sensor tick name=%s mode=%s emit_all=%s date=%s",
            name,
            check_mode,
            emit_all,
            date_key,
        )
        for asset in assets:
            table = table_map.get(asset.key)
            if not table:
                continue

            if emit_all and check_mode == "recursive":
                prefix = _s3_table_root_prefix(table)
                if not prefix:
                    continue
                objects = _s3_list_all_objects(prefix, force_refresh=force_refresh)
                logging.info(
                    "s3 sensor recursive prefix=%s objects=%d asset=%s",
                    prefix,
                    len(objects),
                    asset.key,
                )
                for path, meta in _iter_objects_newest_first(objects):
                    parsed = _parse_table_s3_path(table, path)
                    if not parsed:
                        continue
                    part_mic, part_date = parsed
                    if asset.metadata.get("partitioning") == "cbbo_date":
                        pk = MultiPartitionKey({date_dim: part_date, cbbo_dim: part_mic})
                    else:
                        pk = MultiPartitionKey({date_dim: part_date, mic_dim: part_mic})
                    events.append(
                        AssetObservation(
                            asset_key=asset.key,
                            partition=str(pk),
                            metadata={
                                "s3_path": path,
                                "size_bytes": meta.get("size_bytes"),
                                "last_modified": meta.get("last_modified"),
                                "source": "s3_listing",
                            },
                        )
                    )
                    if len(events) >= max_events:
                        break
                if len(events) >= max_events:
                    break
                continue

            if asset.metadata.get("partitioning") == "cbbo_date":
                cbbo_value = os.getenv("S3_SENSOR_CBBO", "cbbo")
                partition_key = MultiPartitionKey({date_dim: date_key, cbbo_dim: cbbo_value})
                y, m, d = (int(part) for part in date_key.split("-"))
                s3_paths = table.get_s3_paths([cbbo_value], y, m, d)
                if all(_s3_path_exists(p, check_mode=check_mode) for p in s3_paths):
                    events.append(
                        AssetObservation(
                            asset_key=asset.key,
                            partition=str(partition_key),
                            metadata={"s3_paths": s3_paths, "source": "s3_check"},
                        )
                    )
                continue

            if not mic_list:
                continue

            for mic in mic_list:
                partition_key = MultiPartitionKey({date_dim: date_key, mic_dim: mic})
                y, m, d = (int(part) for part in date_key.split("-"))
                s3_paths = table.get_s3_paths([mic], y, m, d)
                if all(_s3_path_exists(p, check_mode=check_mode) for p in s3_paths):
                    events.append(
                        AssetObservation(
                            asset_key=asset.key,
                            partition=str(partition_key),
                            metadata={"s3_paths": s3_paths, "source": "s3_check"},
                        )
                    )
                    if len(events) >= max_events:
                        break
            if len(events) >= max_events:
                break

        if not events:
            logging.info("s3 sensor no observations emitted")
            return SensorResult(skip_reason="No S3 inputs detected")
        logging.info("s3 sensor emitted observations=%d", len(events))
        return SensorResult(asset_events=events)

    return _sensor


def build_s3_input_sync_job(
    *,
    name: str,
    assets: Sequence,
    table_map: dict,
    date_dim: str = "date",
    mic_dim: str = "mic",
    cbbo_dim: str = "cbbo",
    check_mode: str = "recursive",
):
    """
    Build a manual job to bulk-emit AssetObservations from S3 listings.
    Intended for fast initial sync without relying on the sensor.
    """
    try:
        from dagster import AssetObservation, MultiPartitionKey, job, op
    except Exception as exc:
        raise ImportError("Dagster is required to build sync jobs.") from exc

    @op(name="sync_s3_input_assets")
    def _sync_op(context):
        force_refresh = os.getenv("S3_SYNC_FORCE_REFRESH", "0") in {"1", "true", "True"}
        reset = os.getenv("S3_SYNC_RESET", "0") in {"1", "true", "True"}
        max_events = int(os.getenv("S3_SYNC_MAX_OBSERVATIONS", "100000"))

        emitted = 0
        for asset in assets:
            table = table_map.get(asset.key)
            if not table:
                continue

            if check_mode != "recursive":
                context.log.warning(
                    "sync job requires recursive mode; skipping asset=%s", asset.key
                )
                continue

            prefix = _s3_table_root_prefix(table)
            if not prefix:
                context.log.warning("missing s3 prefix for asset=%s", asset.key)
                continue

            objects = _s3_list_all_objects(prefix, force_refresh=force_refresh)
            paths = [path for path, _ in _iter_objects_newest_first(objects)]

            cursor_path = _s3_sync_cursor_path(table)
            cursor = 0
            if not reset:
                cursor = _s3_sync_cursor_load(cursor_path)

            context.log.info(
                "sync asset=%s objects=%d cursor=%d",
                asset.key,
                len(paths),
                cursor,
            )

            for path in paths[cursor:]:
                parsed = _parse_table_s3_path(table, path)
                if not parsed:
                    cursor += 1
                    continue
                part_mic, part_date = parsed
                if asset.metadata.get("partitioning") == "cbbo_date":
                    pk = MultiPartitionKey({date_dim: part_date, cbbo_dim: part_mic})
                else:
                    pk = MultiPartitionKey({date_dim: part_date, mic_dim: part_mic})
                meta = objects.get(path, {})
                yield AssetObservation(
                    asset_key=asset.key,
                    partition=str(pk),
                    metadata={
                        "s3_path": path,
                        "size_bytes": meta.get("size_bytes"),
                        "last_modified": meta.get("last_modified"),
                        "source": "s3_sync_job",
                    },
                )
                emitted += 1
                cursor += 1
                if emitted >= max_events:
                    break

            _s3_sync_cursor_store(cursor_path, cursor)
            if emitted >= max_events:
                break

        context.log.info("sync emitted=%d", emitted)

    @job(name=name)
    def _sync_job():
        _sync_op()

    return _sync_job


def run_partition(
    base_config: dict,
    default_get_universe: Callable,
    partition: PartitionRun,
    get_pipeline: Optional[Callable] = None,
    on_result: Optional[Callable[[PartitionRun, AnalyticsConfig], None]] = None,
) -> None:
    """
    Execute a single partition. Intended to be wrapped by Dagster assets/jobs.
    """
    config = AnalyticsConfig(
        **{
            **base_config,
            "START_DATE": partition.dates.start_date,
            "END_DATE": partition.dates.end_date,
        }
    )

    get_universe = default_get_universe
    if partition.universe:
        get_universe = _load_universe_override(partition.universe.spec)

    run_multiday_pipeline(
        config=config,
        get_universe=get_universe,
        get_pipeline=get_pipeline,
    )

    if on_result:
        on_result(partition, config)


def _discover_demo_modules(demo_pkg: str):
    import importlib
    import pkgutil

    demos = []
    pkg = importlib.import_module(demo_pkg)
    for mod in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + "."):
        module = importlib.import_module(mod.name)
        if not hasattr(module, "USER_CONFIG") or not hasattr(module, "get_universe"):
            continue
        demos.append(module)
    return demos


_S3_LIST_CACHE: dict[str, dict[str, dict]] = {}
_S3_CACHE_TTL_SECONDS = 15 * 60
_S3_CACHE_DIR = os.getenv("S3_LIST_CACHE_DIR", "/tmp/dagster_s3_cache")


def _s3_path_exists(path: str, check_mode: str = "recursive") -> bool:
    if not path.startswith("s3://"):
        return os.path.exists(path)
    if check_mode == "list":
        return _s3_prefix_has_objects(_s3_dir_prefix(path))
    if check_mode == "recursive":
        return _s3_path_in_recursive_listing(path)
    return bool(filter_existing_s3_files([path]))


def _s3_dir_prefix(path: str) -> str:
    if not path.startswith("s3://"):
        return os.path.dirname(path)
    _, _, rest = path.partition("s3://")
    bucket, _, key = rest.partition("/")
    dir_key = key.rsplit("/", 1)[0] if "/" in key else ""
    return f"s3://{bucket}/{dir_key}"


def _s3_prefix_has_objects(prefix: str) -> bool:
    try:
        import boto3
    except Exception:
        return False

    if not prefix.startswith("s3://"):
        return False
    _, _, rest = prefix.partition("s3://")
    bucket, _, key = rest.partition("/")
    client = boto3.client("s3")
    response = client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    return bool(response.get("Contents"))


def _s3_path_in_recursive_listing(path: str) -> bool:
    prefix = _s3_dir_prefix(path)
    if prefix not in _S3_LIST_CACHE or _s3_cache_expired(prefix):
        _S3_LIST_CACHE[prefix] = _s3_list_all_objects(prefix)
    return path in _S3_LIST_CACHE[prefix]


def _s3_list_all_objects(prefix: str, *, force_refresh: bool = False) -> dict[str, dict]:
    try:
        import boto3
    except Exception:
        return {}

    if not prefix.startswith("s3://"):
        return {}

    cached = _s3_cache_load(prefix) if not force_refresh else None
    if cached is not None:
        logging.info(
            "s3 list cache hit prefix=%s objects=%d",
            prefix,
            len(cached),
        )
        return cached

    _, _, rest = prefix.partition("s3://")
    bucket, _, key_prefix = rest.partition("/")
    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    objects: dict[str, dict] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for item in page.get("Contents", []):
            path = f"s3://{bucket}/{item['Key']}"
            objects[path] = {
                "size_bytes": item.get("Size"),
                "last_modified": item.get("LastModified").isoformat()
                if item.get("LastModified")
                else None,
            }
    _s3_cache_store(prefix, objects)
    logging.info(
        "s3 list refresh prefix=%s objects=%d",
        prefix,
        len(objects),
    )
    return objects


def _iter_objects_newest_first(objects: dict[str, dict]):
    def sort_key(item):
        meta = item[1] or {}
        ts = meta.get("last_modified") or ""
        return ts

    return sorted(objects.items(), key=sort_key, reverse=True)


def _s3_cache_path(prefix: str) -> str:
    os.makedirs(_S3_CACHE_DIR, exist_ok=True)
    digest = sha1(prefix.encode("utf-8")).hexdigest()
    return os.path.join(_S3_CACHE_DIR, f"{digest}.json")


def _s3_cache_load(prefix: str) -> set[str] | None:
    path = _s3_cache_path(prefix)
    try:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        ts = payload.get("timestamp", 0)
        if time.time() - ts > _S3_CACHE_TTL_SECONDS:
            return None
        if "objects" in payload:
            return payload.get("objects", {})
        keys = payload.get("keys", [])
        return {key: {} for key in keys}
    except Exception:
        return None


def _s3_cache_store(prefix: str, objects: dict[str, dict]) -> None:
    path = _s3_cache_path(prefix)
    payload = {"timestamp": time.time(), "objects": objects}
    try:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
    except Exception:
        return


def _s3_cache_expired(prefix: str) -> bool:
    path = _s3_cache_path(prefix)
    try:
        if not os.path.exists(path):
            return True
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        ts = payload.get("timestamp", 0)
        return time.time() - ts > _S3_CACHE_TTL_SECONDS
    except Exception:
        return True


def _safe_partition_keys(context) -> dict | None:
    if not context:
        return None
    has_partition = getattr(context, "has_partition_key", None)
    if callable(has_partition) and not has_partition:
        return None
    try:
        partition_key = context.partition_key
    except Exception:
        return None
    return getattr(partition_key, "keys_by_dimension", None)


def _full_check_on_unpartitioned() -> bool:
    return os.getenv("S3_CHECK_FULL_ON_UNPARTITIONED", "1") not in {"0", "false", "False"}


def _s3_table_root_prefix(table) -> str | None:
    try:
        import bmll2
    except Exception:
        return None
    ap = bmll2._configure.L2_ACCESS_POINT_ALIAS
    return f"s3://{ap}/{table.s3_folder_name}/"


def list_cbbo_partitions_from_s3() -> list[str]:
    table = ALL_TABLES.get("cbbo")
    if not table:
        return ["cbbo"]
    prefix = _s3_table_root_prefix(table)
    if not prefix:
        return ["cbbo"]
    objects = _s3_list_all_objects(prefix)
    mics = set()
    for path in objects.keys():
        parsed = _parse_table_s3_path(table, path)
        if not parsed:
            continue
        mic, _ = parsed
        mics.add(mic)
    return sorted(mics) if mics else ["cbbo"]


def _parse_table_s3_path(table, path: str) -> tuple[str, str] | None:
    if not path.startswith("s3://"):
        return None
    _, _, rest = path.partition("s3://")
    parts = rest.split("/")
    if len(parts) < 6:
        return None
    try:
        folder_index = parts.index(table.s3_folder_name)
    except ValueError:
        return None
    if len(parts) <= folder_index + 4:
        return None
    mic = parts[folder_index + 1]
    yyyy = parts[folder_index + 2]
    mm = parts[folder_index + 3]
    dd = parts[folder_index + 4]
    if not (yyyy.isdigit() and mm.isdigit() and dd.isdigit()):
        return None
    date_key = f"{yyyy}-{mm}-{dd}"
    return mic, date_key


def _sanitize_name(name: str) -> str:
    if not name:
        return "pass"
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)


def _s3_sync_cursor_path(table) -> str:
    os.makedirs(_S3_CACHE_DIR, exist_ok=True)
    digest = sha1(f"sync:{table.name}".encode("utf-8")).hexdigest()
    return os.path.join(_S3_CACHE_DIR, f"{digest}.cursor.json")


def _s3_sync_cursor_load(path: str) -> int:
    try:
        if not os.path.exists(path):
            return 0
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return int(payload.get("cursor", 0))
    except Exception:
        return 0


def _s3_sync_cursor_store(path: str, cursor: int) -> None:
    payload = {"timestamp": time.time(), "cursor": cursor}
    try:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
    except Exception:
        return


def _filter_input_deps(input_asset_keys, pass_config) -> list:
    if not input_asset_keys or not isinstance(pass_config, dict):
        return list(input_asset_keys or [])

    required_tables = _required_tables_for_pass(pass_config)
    if not required_tables:
        return []

    deps = []
    for key in input_asset_keys:
        table = _table_from_asset_key(key)
        if not table or table in required_tables:
            deps.append(key)
    return deps


def _table_from_asset_key(asset_key) -> str | None:
    try:
        path = asset_key.path
    except Exception:
        return None
    if not path:
        return None
    if len(path) >= 2 and path[0] in {"input", "bmll"}:
        return path[1]
    if len(path) == 1:
        return path[0]
    return path[-1]


def _required_tables_for_pass(pass_config: dict) -> set[str]:
    modules = pass_config.get("modules", []) if isinstance(pass_config, dict) else []
    module_requires = _module_requires_map()
    required = set()
    for module in modules:
        for table in module_requires.get(module, []):
            required.add(table)
    return required


def _module_requires_map() -> dict[str, list[str]]:
    from .dense_analytics import DenseAnalytics
    from .analytics.trade import TradeAnalytics
    from .analytics.l2 import L2AnalyticsLast, L2AnalyticsTW
    from .analytics.l3 import L3Analytics
    from .analytics.execution import ExecutionAnalytics
    from .analytics.generic import GenericAnalytics
    from .analytics.l3_characteristics import L3CharacteristicsAnalytics
    from .analytics.trade_characteristics import TradeCharacteristicsAnalytics
    from .analytics.cbbo import CBBOAnalytics
    from .analytics.iceberg import IcebergAnalytics

    return {
        "dense": DenseAnalytics.REQUIRES,
        "trade": TradeAnalytics.REQUIRES,
        "l2": L2AnalyticsLast.REQUIRES,
        "l2tw": L2AnalyticsTW.REQUIRES,
        "l3": L3Analytics.REQUIRES,
        "execution": ExecutionAnalytics.REQUIRES,
        "generic": GenericAnalytics.REQUIRES,
        "l3_characteristics": L3CharacteristicsAnalytics.REQUIRES,
        "trade_characteristics": TradeCharacteristicsAnalytics.REQUIRES,
        "cbbo": CBBOAnalytics.REQUIRES,
        "iceberg": IcebergAnalytics.REQUIRES,
    }
