from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import json
import logging
import os
import time
from hashlib import sha1
from typing import Callable, Optional, Sequence
from pathlib import Path

from intraday_analytics.tables import ALL_TABLES
from intraday_analytics.utils import filter_existing_s3_files

from intraday_analytics.configuration import AnalyticsConfig, SchedulePartitionSelector
from intraday_analytics.execution import run_multiday_pipeline
from intraday_analytics.cli import _load_universe_override, resolve_user_config


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
    if "+" in spec:
        return UniversePartition(name=spec, value=None)
    if "=" in spec:
        name, value = spec.split("=", 1)
        return UniversePartition(name=name, value=value)
    return UniversePartition(name=spec, value=None)


_UNIVERSE_REGISTRY: dict[str, Callable] = {}


class MICUniverse:
    ALL = object()

    def __init__(
        self,
        whitelist: Sequence[str] | object = ALL,
        blacklist: Sequence[str] | None = None,
        name: str = "mic",
    ) -> None:
        self.whitelist = whitelist
        self.blacklist = set(blacklist or [])
        self.name = name

    def _available_mics(self) -> list[str]:
        try:
            import bmll.reference
        except Exception:
            return []
        return (
            bmll.reference.available_markets()
            .query("Schema=='Equity'")
            .query("IsAlive")["MIC"]
            .tolist()
        )

    def iter_partitions(self) -> list[UniversePartition]:
        mics = self._available_mics()
        if self.whitelist is not MICUniverse.ALL:
            allowed = set(self.whitelist or [])
            mics = [m for m in mics if m in allowed]
        if self.blacklist:
            mics = [m for m in mics if m not in self.blacklist]
        return [UniversePartition(name=self.name, value=mic) for mic in mics]

    def get_universe_for(self, _partition: UniversePartition) -> Optional[Callable]:
        return None


class IndexUniverse:
    def __init__(self, values: Sequence[str], name: str = "index") -> None:
        self.values = list(values)
        self.name = name

    def iter_partitions(self) -> list[UniversePartition]:
        return [UniversePartition(name=self.name, value=v) for v in self.values]

    def get_universe_for(self, _partition: UniversePartition) -> Optional[Callable]:
        return None


class OPOLUniverse:
    def __init__(self, values: Sequence[str], name: str = "opol") -> None:
        self.values = list(values)
        self.name = name

    def iter_partitions(self) -> list[UniversePartition]:
        return [UniversePartition(name=self.name, value=v) for v in self.values]

    def get_universe_for(self, _partition: UniversePartition) -> Optional[Callable]:
        return None


class CustomUniverse:
    def __init__(
        self,
        get_universe: Callable,
        name: str = "custom",
        value: str | None = None,
    ) -> None:
        self.get_universe = get_universe
        self.name = name
        self.value = value

    def iter_partitions(self) -> list[UniversePartition]:
        return [UniversePartition(name=self.name, value=self.value)]

    def get_universe_for(self, _partition: UniversePartition) -> Optional[Callable]:
        return self.get_universe


class CartProdUniverse:
    def __init__(self, left, right, separator: str = "+") -> None:
        self.left = left
        self.right = right
        self.separator = separator

    def iter_partitions(self) -> list[UniversePartition]:
        parts = []
        for left_part in self.left.iter_partitions():
            for right_part in self.right.iter_partitions():
                spec = f"{left_part.spec}{self.separator}{right_part.spec}"
                parts.append(UniversePartition(name=spec, value=None))
        return parts

    def get_universe_for(self, _partition: UniversePartition) -> Optional[Callable]:
        return None


def _ensure_polars_frame(frame):
    try:
        import polars as pl
    except Exception:
        pl = None
    try:
        import pandas as pd
    except Exception:
        pd = None

    if pl and isinstance(frame, pl.DataFrame):
        return frame
    if pd and isinstance(frame, pd.DataFrame) and pl:
        return pl.from_pandas(frame)
    return frame


def _intersect_universe_frames(frames: Sequence) -> object:
    frames = [_ensure_polars_frame(f) for f in frames if f is not None]
    if not frames:
        return None
    try:
        import polars as pl
    except Exception:
        return frames[0]

    result = frames[0]
    for frame in frames[1:]:
        if not isinstance(result, pl.DataFrame) or not isinstance(frame, pl.DataFrame):
            continue
        keys = [
            c
            for c in ("ListingId", "MIC")
            if c in result.columns and c in frame.columns
        ]
        if not keys:
            keys = [c for c in result.columns if c in frame.columns]
        if not keys:
            return result
        result = result.join(frame, on=keys, how="inner")
    return result


def _resolve_universe_callable(spec: str) -> Callable:
    if spec in _UNIVERSE_REGISTRY:
        return _UNIVERSE_REGISTRY[spec]
    return _load_universe_override(spec)


def build_universe_partitions(
    universes: Sequence,
) -> list[str]:
    _UNIVERSE_REGISTRY.clear()
    specs: list[str] = []

    def _register_custom(universe) -> None:
        if isinstance(universe, CustomUniverse):
            for part in universe.iter_partitions():
                _UNIVERSE_REGISTRY[part.spec] = universe.get_universe
        elif isinstance(universe, CartProdUniverse):
            _register_custom(universe.left)
            _register_custom(universe.right)

    for universe in universes:
        _register_custom(universe)

    for universe in universes:
        for part in universe.iter_partitions():
            specs.append(part.spec)
            fn = universe.get_universe_for(part)
            if fn:
                _UNIVERSE_REGISTRY[part.spec] = fn

    return specs


def default_universes() -> list:
    try:
        import main as main_script  # type: ignore

        if hasattr(main_script, "whitelist"):
            return [MICUniverse(whitelist=getattr(main_script, "whitelist"))]
    except Exception:
        pass
    return [MICUniverse()]


def parse_date_key(key: str) -> DatePartition:
    if "_" in key:
        start, end = key.split("_", 1)
        return DatePartition(start_date=start, end_date=end)

    # Support TimeWindowPartitionsDefinition keys (start date only).
    freq = os.getenv("BATCH_FREQ")
    if freq and freq != "D":
        start_date = dt.date.fromisoformat(key)
        if freq == "W":
            end_date = start_date + dt.timedelta(days=6)
        elif freq == "2W":
            end_date = start_date + dt.timedelta(days=13)
        elif freq == "M":
            import calendar

            end_date = dt.date(
                start_date.year,
                start_date.month,
                calendar.monthrange(start_date.year, start_date.month)[1],
            )
        elif freq == "A":
            end_date = dt.date(start_date.year, 12, 31)
        else:
            end_date = start_date
        return DatePartition(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )

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


def build_assets(
    pkg: str = "demo",
    partitions_def=None,
    universe_dim: str = "universe",
    date_dim: str = "date",
    split_passes: bool = False,
    input_asset_keys: Sequence | None = None,
):
    """
    Discover modules and return Dagster assets for each module.
    """
    try:
        import importlib
        import pkgutil
        from dagster import asset
    except Exception as exc:
        raise ImportError("Dagster is required to build datasets.") from exc

    pipelines = _discover_modules(pkg)

    assets = []
    for module in pipelines:
        name = module.__name__.split(".")[-1]
        asset_base = f"{pkg}{name}" if name[0].isdigit() else name
        package_root = module.__name__.split(".")[0]
        precedence = getattr(module, "CONFIG_YAML_PRECEDENCE", "yaml_overrides")
        base_config = resolve_user_config(
            module.USER_CONFIG, getattr(module, "__file__", None), precedence
        )
        config_model = (
            AnalyticsConfig(**base_config)
            if isinstance(base_config, dict)
            else AnalyticsConfig()
        )
        auto_kwargs = _auto_materialize_kwargs(config_model)
        output_target = config_model.OUTPUT_TARGET
        io_manager_key = getattr(output_target, "io_manager_key", None) or "output_target_io_manager"
        passes = base_config.get("PASSES", []) if isinstance(base_config, dict) else []
        last_pass_key = None

        universes = getattr(module, "UNIVERSES", None) or default_universes()
        default_universe_spec = _first_universe_spec(universes) or "default"

        def _make_asset(
            module, asset_name, asset_group, config_override, deps, key_prefix
        ):
            tags = _bmll_job_tags(config_model)
            @asset(
                name=asset_name,
                partitions_def=partitions_def,
                group_name=asset_group,
                key_prefix=key_prefix,
                deps=deps,
                io_manager_key=io_manager_key,
                tags=tags,
                **auto_kwargs,
            )
            def _new_asset(context=None):
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
                            universe=parse_universe_spec(default_universe_spec),
                            dates=DatePartition(
                                start_date=base_config["START_DATE"],
                                end_date=base_config["END_DATE"],
                            ),
                        )
                else:
                    partition = PartitionRun(
                        universe=parse_universe_spec(default_universe_spec),
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
                if io_manager_key:
                    try:
                        from intraday_analytics.process import get_final_output_path
                    except Exception:
                        return None
                    if split_passes and passes:
                        pass_names = [
                            p.get("name", "pass") for p in base_config.get("PASSES", [])
                        ]
                        outputs = {}
                        for name in pass_names:
                            outputs[name] = get_final_output_path(
                                dt.date.fromisoformat(partition.dates.start_date),
                                dt.date.fromisoformat(partition.dates.end_date),
                                AnalyticsConfig(**base_config),
                                name,
                                output_target,
                            )
                        return outputs
                    return get_final_output_path(
                        dt.date.fromisoformat(partition.dates.start_date),
                        dt.date.fromisoformat(partition.dates.end_date),
                        AnalyticsConfig(**base_config),
                        base_config.get("PASSES", [{}])[-1].get("name", asset_name),
                        output_target,
                    )

            return _new_asset

        if split_passes and passes:
            for pass_config in passes:
                pass_name = _sanitize_name(pass_config.get("name", "pass"))
                asset_name = pass_name
                per_pass_config = {**base_config, "PASSES": [pass_config]}
                deps = _filter_input_deps(input_asset_keys, pass_config)
                if last_pass_key is not None:
                    deps.append(last_pass_key)
                assets.append(
                    _make_asset(
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
                _make_asset(
                    module,
                    asset_base,
                    asset_base,
                    base_config,
                    deps,
                    [package_root],
                )
            )

    return assets


def build_assets_for_module(
    module,
    *,
    partitions_def=None,
    universe_dim: str = "universe",
    date_dim: str = "date",
    split_passes: bool = False,
    input_asset_keys: Sequence | None = None,
):
    """
    Build Dagster assets for a single module object.
    """
    try:
        from dagster import asset
    except Exception as exc:
        raise ImportError("Dagster is required to build datasets.") from exc

    name = Path(getattr(module, "__file__", "pipeline")).stem
    pkg_name = module.__name__.split(".")[0]
    asset_base = f"{pkg_name}{name}" if name and name[0].isdigit() else name
    package_root = module.__name__.split(".")[0]
    precedence = getattr(module, "CONFIG_YAML_PRECEDENCE", "yaml_overrides")
    base_config = resolve_user_config(
        module.USER_CONFIG, getattr(module, "__file__", None), precedence
    )
    config_model = (
        AnalyticsConfig(**base_config)
        if isinstance(base_config, dict)
        else AnalyticsConfig()
    )
    auto_kwargs = _auto_materialize_kwargs(config_model)
    output_target = config_model.OUTPUT_TARGET
    io_manager_key = getattr(output_target, "io_manager_key", None) or "output_target_io_manager"
    passes = base_config.get("PASSES", []) if isinstance(base_config, dict) else []
    last_pass_key = None

    universes = getattr(module, "UNIVERSES", None) or default_universes()
    default_universe_spec = _first_universe_spec(universes) or "default"

    def _make_asset(module, asset_name, asset_group, config_override, deps, key_prefix):
        tags = _bmll_job_tags(config_model)
        @asset(
            name=asset_name,
            partitions_def=partitions_def,
            group_name=asset_group,
            key_prefix=key_prefix,
            deps=deps,
            io_manager_key=io_manager_key,
            tags=tags,
            **auto_kwargs,
        )
        def _new_asset(context=None):
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
                        universe=parse_universe_spec(default_universe_spec),
                        dates=DatePartition(
                            start_date=base_config["START_DATE"],
                            end_date=base_config["END_DATE"],
                        ),
                    )
            else:
                partition = PartitionRun(
                    universe=parse_universe_spec(default_universe_spec),
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
            if io_manager_key:
                try:
                    from intraday_analytics.process import get_final_output_path
                except Exception:
                    return None
                if split_passes and passes:
                    pass_names = [
                        p.get("name", "pass") for p in base_config.get("PASSES", [])
                    ]
                    outputs = {}
                    for pass_name in pass_names:
                        outputs[pass_name] = get_final_output_path(
                            dt.date.fromisoformat(partition.dates.start_date),
                            dt.date.fromisoformat(partition.dates.end_date),
                            AnalyticsConfig(**base_config),
                            pass_name,
                            output_target,
                        )
                    return outputs
                return get_final_output_path(
                    dt.date.fromisoformat(partition.dates.start_date),
                    dt.date.fromisoformat(partition.dates.end_date),
                    AnalyticsConfig(**base_config),
                    base_config.get("PASSES", [{}])[-1].get("name", asset_name),
                    output_target,
                )

        return _new_asset

    assets = []
    if split_passes and passes:
        for pass_config in passes:
            pass_name = _sanitize_name(pass_config.get("name", "pass"))
            asset_name = pass_name
            per_pass_config = {**base_config, "PASSES": [pass_config]}
            deps = _filter_input_deps(input_asset_keys, pass_config)
            if last_pass_key is not None:
                deps.append(last_pass_key)
            assets.append(
                _make_asset(
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
            _make_asset(
                module,
                asset_base,
                asset_base,
                base_config,
                deps,
                [package_root],
            )
        )

    return assets


def _auto_materialize_kwargs(config: AnalyticsConfig) -> dict:
    if not config.AUTO_MATERIALIZE_ENABLED:
        return {}
    try:
        from dagster import AutomationCondition
        from datetime import timedelta

        condition = AutomationCondition.eager()
        if config.AUTO_MATERIALIZE_LATEST_DAYS:
            condition = condition.without(AutomationCondition.in_latest_time_window())
            condition = condition & AutomationCondition.in_latest_time_window(
                timedelta(days=int(config.AUTO_MATERIALIZE_LATEST_DAYS))
            )
        return {"automation_condition": condition}
    except Exception:
        try:
            from dagster import AutoMaterializePolicy

            if config.AUTO_MATERIALIZE_LATEST_DAYS:
                return {
                    "auto_materialize_policy": AutoMaterializePolicy.eager(
                        max_materializations_per_minute=None
                    )
                }
            return {"auto_materialize_policy": AutoMaterializePolicy.eager()}
        except Exception:
            return {}


def _bmll_job_tags(config: AnalyticsConfig) -> dict[str, str]:
    tags: dict[str, str] = {}
    job_cfg = getattr(config, "BMLL_JOBS", None)
    if not job_cfg:
        return tags
    if job_cfg.default_instance_size:
        tags["bmll/instance_size"] = str(job_cfg.default_instance_size)
    if job_cfg.default_conda_env:
        tags["bmll/conda_env"] = str(job_cfg.default_conda_env)
    if job_cfg.max_runtime_hours:
        tags["bmll/max_runtime_hours"] = str(job_cfg.max_runtime_hours)
    if job_cfg.max_concurrent_instances:
        tags["bmll/max_concurrent_instances"] = str(job_cfg.max_concurrent_instances)
    if job_cfg.log_path:
        tags["bmll/log_path"] = str(job_cfg.log_path)
    return tags


def _first_universe_spec(universes: Sequence) -> Optional[str]:
    for universe in universes:
        parts = universe.iter_partitions()
        if parts:
            return parts[0].spec
    return None


def _resolve_schedule_date(
    value: Optional[str], context, timezone: str
) -> Optional[str]:
    if value and value not in {"today", "yesterday"}:
        return value

    base = getattr(context, "scheduled_execution_time", None)
    if timezone:
        try:
            from zoneinfo import ZoneInfo

            tzinfo = ZoneInfo(timezone)
        except Exception:
            tzinfo = dt.timezone.utc
    else:
        tzinfo = dt.timezone.utc

    if base is None:
        base = dt.datetime.now(tzinfo)
    else:
        try:
            base = base.astimezone(tzinfo)
        except Exception:
            pass

    base_date = base.date()
    if value == "today":
        return base_date.isoformat()
    return (base_date - dt.timedelta(days=1)).isoformat()


def _schedule_partitions(
    schedule_cfg,
    *,
    default_universe: Optional[str],
    universe_dim: str,
    date_dim: str,
) -> list[dict[str, Optional[str]]]:
    entries = schedule_cfg.partitions or [SchedulePartitionSelector()]
    resolved = []
    for entry in entries:
        if isinstance(entry, dict):
            entry = SchedulePartitionSelector(**entry)
        universe = entry.universe or default_universe
        date_key = entry.date or "yesterday"
        resolved.append({universe_dim: universe, date_dim: date_key})
    return resolved


def build_schedules(
    pkg: str = "demo",
    partitions_def=None,
    universe_dim: str = "universe",
    date_dim: str = "date",
    split_passes: bool = False,
):
    """
    Build Dagster schedules for assets based on AnalyticsConfig.SCHEDULES.
    """
    try:
        from dagster import (
            AssetSelection,
            MultiPartitionKey,
            RunRequest,
            define_asset_job,
            schedule,
        )
    except Exception as exc:
        raise ImportError("Dagster is required to build schedules.") from exc

    pipelines = _discover_modules(pkg)
    schedules = []

    for module in pipelines:
        name = module.__name__.split(".")[-1]
        asset_base = f"{pkg}{name}" if name[0].isdigit() else name
        precedence = getattr(module, "CONFIG_YAML_PRECEDENCE", "yaml_overrides")
        base_config = resolve_user_config(
            module.USER_CONFIG, getattr(module, "__file__", None), precedence
        )
        config_model = (
            AnalyticsConfig(**base_config)
            if isinstance(base_config, dict)
            else AnalyticsConfig()
        )
        if not config_model.SCHEDULES:
            continue

        universes = getattr(module, "UNIVERSES", None) or default_universes()
        default_universe = _first_universe_spec(universes)

        selection = AssetSelection.groups(asset_base)
        job = define_asset_job(
            name=f"{asset_base}__job",
            selection=selection,
            partitions_def=partitions_def,
        )

        for schedule_cfg in config_model.SCHEDULES:
            if not schedule_cfg.enabled:
                continue

            schedule_name = f"{asset_base}__{schedule_cfg.name}"
            partition_specs = _schedule_partitions(
                schedule_cfg,
                default_universe=default_universe,
                universe_dim=universe_dim,
                date_dim=date_dim,
            )

            @schedule(
                name=schedule_name,
                cron_schedule=schedule_cfg.cron,
                job=job,
                execution_timezone=schedule_cfg.timezone,
            )
            def _schedule_fn(
                context,
                _specs=partition_specs,
                _timezone=schedule_cfg.timezone,
                _name=schedule_name,
            ):
                for spec in _specs:
                    date_value = _resolve_schedule_date(
                        spec.get(date_dim), context, _timezone
                    )
                    universe_value = spec.get(universe_dim)
                    if universe_value is None or date_value is None:
                        continue
                    key_map = {universe_dim: universe_value, date_dim: date_value}
                    partition_key = MultiPartitionKey(key_map)
                    yield RunRequest(
                        partition_key=partition_key,
                        run_key=f"{_name}:{partition_key}",
                    )

            schedules.append(_schedule_fn)

    return schedules


def build_schedules_for_module(
    module,
    *,
    partitions_def=None,
    universe_dim: str = "universe",
    date_dim: str = "date",
    split_passes: bool = False,
):
    """
    Build Dagster schedules for a single module based on AnalyticsConfig.SCHEDULES.
    """
    try:
        from dagster import (
            AssetSelection,
            MultiPartitionKey,
            RunRequest,
            define_asset_job,
            schedule,
        )
    except Exception as exc:
        raise ImportError("Dagster is required to build schedules.") from exc

    name = module.__name__.split(".")[-1]
    asset_base = f"{module.__name__.split('.')[0]}{name}" if name and name[0].isdigit() else name
    precedence = getattr(module, "CONFIG_YAML_PRECEDENCE", "yaml_overrides")
    base_config = resolve_user_config(
        module.USER_CONFIG, getattr(module, "__file__", None), precedence
    )
    config_model = (
        AnalyticsConfig(**base_config)
        if isinstance(base_config, dict)
        else AnalyticsConfig()
    )
    if not config_model.SCHEDULES:
        return []

    universes = getattr(module, "UNIVERSES", None) or default_universes()
    default_universe = _first_universe_spec(universes)

    selection = AssetSelection.groups(asset_base)
    job = define_asset_job(
        name=f"{asset_base}__job",
        selection=selection,
        partitions_def=partitions_def,
    )

    schedules = []
    for schedule_cfg in config_model.SCHEDULES:
        if not schedule_cfg.enabled:
            continue
        schedule_name = f"{asset_base}__{schedule_cfg.name}"
        partition_specs = _schedule_partitions(
            schedule_cfg,
            default_universe=default_universe,
            universe_dim=universe_dim,
            date_dim=date_dim,
        )

        @schedule(
            name=schedule_name,
            cron_schedule=schedule_cfg.cron,
            job=job,
            execution_timezone=schedule_cfg.timezone,
        )
        def _schedule_fn(
            context,
            _specs=partition_specs,
            _timezone=schedule_cfg.timezone,
            _name=schedule_name,
        ):
            for spec in _specs:
                date_value = _resolve_schedule_date(
                    spec.get(date_dim), context, _timezone
                )
                universe_value = spec.get(universe_dim)
                if universe_value is None or date_value is None:
                    continue
                key_map = {universe_dim: universe_value, date_dim: date_value}
                partition_key = MultiPartitionKey(key_map)
                yield RunRequest(
                    partition_key=partition_key,
                    run_key=f"{_name}:{partition_key}",
                )

        schedules.append(_schedule_fn)

    return schedules


def build_materialization_checks(
    pkg: str = "demo",
    universe_dim: str = "universe",
    date_dim: str = "date",
    check_mode: str = "head",
    split_passes: bool = False,
):
    """
    Create Dagster asset checks that validate materializations by checking
    for output files in S3.
    """
    try:
        from dagster import AssetCheckResult, AssetKey, asset_check
    except Exception as exc:
        raise ImportError("Dagster is required to build  asset checks.") from exc

    from intraday_analytics.process import get_final_s3_path

    modules = _discover_modules(pkg)
    checks = []

    for module in modules:
        name = module.__name__.split(".")[-1]
        asset_base = f"{pkg}{name}" if name[0].isdigit() else name
        package_root = module.__name__.split(".")[0]
        base_config = module.USER_CONFIG
        passes = base_config.get("PASSES", []) if isinstance(base_config, dict) else []
        universes = getattr(module, "UNIVERSES", None) or default_universes()
        default_universe_spec = _first_universe_spec(universes) or "default"

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
                        universe=parse_universe_spec(default_universe_spec),
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
                checks.append(
                    _make_materialized_check(base_config, asset_key, pass_name)
                )
        else:
            checks.append(
                _make_materialized_check(
                    base_config, AssetKey([package_root, asset_base]), None
                )
            )

    return checks


def build_materialization_checks_for_module(
    module,
    *,
    universe_dim: str = "universe",
    date_dim: str = "date",
    check_mode: str = "head",
    split_passes: bool = False,
):
    """
    Create Dagster asset checks for a single module.
    """
    try:
        from dagster import AssetCheckResult, AssetKey, asset_check
    except Exception as exc:
        raise ImportError("Dagster is required to build  asset checks.") from exc

    from intraday_analytics.process import get_final_s3_path

    name = module.__name__.split(".")[-1]
    asset_base = f"{module.__name__.split('.')[0]}{name}" if name and name[0].isdigit() else name
    precedence = getattr(module, "CONFIG_YAML_PRECEDENCE", "yaml_overrides")
    base_config = resolve_user_config(
        module.USER_CONFIG, getattr(module, "__file__", None), precedence
    )
    config_model = (
        AnalyticsConfig(**base_config)
        if isinstance(base_config, dict)
        else AnalyticsConfig()
    )
    passes = base_config.get("PASSES", []) if isinstance(base_config, dict) else []

    checks = []
    if split_passes and passes:
        for idx, pass_cfg in enumerate(passes):
            pass_label = pass_cfg.get("name", f"pass{idx+1}")
            asset_key = AssetKey([module.__name__.split(".")[0], asset_base, pass_label])

            @asset_check(
                name=f"{asset_base}__{pass_label}__check",
                asset=asset_key,
            )
            def _check(context, _pass_label=pass_label):
                path = get_final_s3_path(
                    parse_date_key(context.partition_key.keys_by_dimension[date_dim]).start_date,
                    parse_date_key(context.partition_key.keys_by_dimension[date_dim]).end_date,
                    config_model,
                    _pass_label,
                )
                exists = check_s3_url_exists(path, check_mode=check_mode)
                return AssetCheckResult(passed=exists, metadata={"path": path})

            checks.append(_check)
    else:
        asset_key = AssetKey([module.__name__.split(".")[0], asset_base])

        @asset_check(name=f"{asset_base}__check", asset=asset_key)
        def _check(context):
            path = get_final_s3_path(
                parse_date_key(context.partition_key.keys_by_dimension[date_dim]).start_date,
                parse_date_key(context.partition_key.keys_by_dimension[date_dim]).end_date,
                config_model,
                asset_base,
            )
            exists = check_s3_url_exists(path, check_mode=check_mode)
            return AssetCheckResult(passed=exists, metadata={"path": path})

        checks.append(_check)

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

        try:
            from dagster import AssetSpec
        except Exception:
            AssetSpec = None
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

        metadata = {
            "table": table.name,
            "s3_folder": table.s3_folder_name,
            "s3_prefix": table.s3_file_prefix,
            "partitioning": partitioning,
        }
        if AssetSpec is not None:
            assets.append(
                AssetSpec(
                    key=asset_key,
                    partitions_def=partitions_def,
                    description=f"External BMLL table: {table.name}",
                    group_name=group_name,
                    metadata=metadata,
                )
            )
        else:
            assets.append(
                SourceAsset(
                    key=asset_key,
                    partitions_def=partitions_def,
                    description=f"External BMLL table: {table.name}",
                    group_name=group_name,
                    metadata=metadata,
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
    check_mode: str = "head",
    include_stats: bool = False,
):
    """
    Create asset checks for external input assets by checking S3 for the
    expected file location.
    """
    try:
        from dagster import AssetCheckResult, asset_check
    except Exception as exc:
        raise ImportError("Dagster is required to build asset checks.") from exc
    try:
        from dagster import MetadataValue
    except Exception:
        MetadataValue = None

    include_stats = include_stats or os.getenv("S3_CHECK_INCLUDE_STATS", "0") not in {
        "0",
        "false",
        "False",
    }
    checks = []

    for asset in assets:
        table = table_map.get(asset.key)
        if not table:
            continue
        asset_key = asset.key if hasattr(asset, "key") else asset

        def _make_input_check(table):
            @asset_check(asset=asset_key, name="s3_exists")
            def _input_check(context):
                logging.info(
                    "s3_exists check start asset=%s mode=%s",
                    asset_key,
                    check_mode,
                )
                keys = _safe_partition_keys(context)
                if not keys:
                    logging.info(
                        "s3_exists skipped (no partition key) asset=%s",
                        asset_key,
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
                        metadata={
                            "reason": "input assets must use single-day partitions"
                        },
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

                exists = all(
                    _s3_path_exists(p, check_mode=check_mode) for p in s3_paths
                )
                logging.info(
                    "s3_exists partitioned asset=%s exists=%s paths=%s",
                    asset.key,
                    exists,
                    s3_paths,
                )
                metadata = {
                    "paths": ", ".join(s3_paths),
                    "check_mode": check_mode,
                }
                if include_stats:
                    metadata.update(
                        _build_s3_date_stats_metadata(
                            table=table,
                            date_key=date_key,
                            max_rows=int(os.getenv("S3_CHECK_STATS_LIMIT", "20")),
                            metadata_value_cls=MetadataValue,
                        )
                    )
                return AssetCheckResult(
                    passed=exists,
                    metadata=metadata,
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
        mic_list = (
            list(mics) if mics is not None else [m for m in mic_env.split(",") if m]
        )

        emit_all = os.getenv("S3_SENSOR_EMIT_ALL", "0") in {"1", "true", "True"}
        max_events = int(os.getenv("S3_SENSOR_MAX_OBSERVATIONS", "1000"))
        force_refresh = os.getenv("S3_SENSOR_FORCE_REFRESH", "0") in {
            "1",
            "true",
            "True",
        }

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
                        pk = MultiPartitionKey(
                            {date_dim: part_date, cbbo_dim: part_mic}
                        )
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
                partition_key = MultiPartitionKey(
                    {date_dim: date_key, cbbo_dim: cbbo_value}
                )
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
        from dagster import (
            AssetMaterialization,
            AssetObservation,
            MultiPartitionKey,
            job,
            op,
        )
    except Exception as exc:
        raise ImportError("Dagster is required to build sync jobs.") from exc

    @op(name="sync_s3_input_assets")
    def _sync_op(context):
        force_refresh = os.getenv("S3_SYNC_FORCE_REFRESH", "0") in {"1", "true", "True"}
        reset = os.getenv("S3_SYNC_RESET", "0") in {"1", "true", "True"}
        max_events = int(os.getenv("S3_SYNC_MAX_OBSERVATIONS", "100000"))
        date_start = os.getenv("S3_SYNC_START_DATE")
        date_end = os.getenv("S3_SYNC_END_DATE")
        shard_total = int(os.getenv("S3_SYNC_SHARD_TOTAL", "1"))
        shard_index = int(os.getenv("S3_SYNC_SHARD_INDEX", "0"))

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
                if not _date_in_range(part_date, date_start, date_end):
                    cursor += 1
                    continue
                if not _shard_accept(part_mic, shard_index, shard_total):
                    cursor += 1
                    continue
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
                yield AssetMaterialization(
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


def build_s3_input_sync_asset(
    *,
    name: str,
    assets: Sequence,
    table_map: dict,
    date_partitions_def,
    date_dim: str = "date",
    mic_dim: str = "mic",
    cbbo_dim: str = "cbbo",
    check_mode: str = "recursive",
    group_name: str = "BMLL",
    use_db_bulk: bool = False,
    repo_root: str | None = None,
    asset_key_prefix: Sequence[str] | None = None,
):
    """
    Build a partitioned asset for per-date sync of S3 input observations/materializations.
    """
    try:
        from dagster import AssetMaterialization, AssetObservation, asset
    except Exception as exc:
        raise ImportError("Dagster is required to build sync assets.") from exc

    @asset(
        name=name,
        partitions_def=date_partitions_def,
        group_name=group_name,
        key_prefix=[group_name],
    )
    def _sync_asset(context):
        if use_db_bulk:
            import subprocess
            import sys
            import os

            date_key = getattr(context, "partition_key", None)
            if not date_key:
                context.log.warning("sync asset requires a date partition")
                return

            root = repo_root or os.getenv("BMLL_REPO_ROOT")
            if not root:
                raise RuntimeError(
                    "Missing repo_root for db bulk sync (set BMLL_REPO_ROOT)."
                )
            script_path = os.path.join(root, "scripts", "s3_bulk_sync_db.py")
            if not os.path.exists(script_path):
                raise RuntimeError(f"Bulk sync script not found: {script_path}")

            tables = ",".join([t.name for t in table_map.values()])
            prefix = ",".join(asset_key_prefix) if asset_key_prefix else "BMLL"
            cmd = [
                sys.executable,
                script_path,
                "sync",
                "--tables",
                tables,
                "--start_date",
                date_key,
                "--end_date",
                date_key,
                "--emit_materializations",
                "True",
                "--emit_observations",
                "True",
                "--asset_key_prefix",
                prefix,
                "--refresh_status_cache",
                "True",
            ]
            subprocess.check_call(cmd, env=os.environ)
            return

        if check_mode != "recursive":
            context.log.warning("sync asset requires recursive mode")
            return

        date_key = getattr(context, "partition_key", None)
        if not date_key:
            context.log.warning("sync asset requires a date partition")
            return

        force_refresh = os.getenv("S3_SYNC_FORCE_REFRESH", "0") in {"1", "true", "True"}
        max_events = int(os.getenv("S3_SYNC_MAX_OBSERVATIONS", "100000"))
        shard_total = int(os.getenv("S3_SYNC_SHARD_TOTAL", "1"))
        shard_index = int(os.getenv("S3_SYNC_SHARD_INDEX", "0"))

        emitted = 0
        for asset in assets:
            table = table_map.get(asset.key)
            if not table:
                continue

            prefix = _s3_table_root_prefix(table)
            if not prefix:
                context.log.warning("missing s3 prefix for asset=%s", asset.key)
                continue

            objects = _s3_list_all_objects(prefix, force_refresh=force_refresh)
            for path, meta in _iter_objects_newest_first(objects):
                parsed = _parse_table_s3_path(table, path)
                if not parsed:
                    continue
                part_mic, part_date = parsed
                if part_date != date_key:
                    continue
                if not _shard_accept(part_mic, shard_index, shard_total):
                    continue
                if asset.metadata.get("partitioning") == "cbbo_date":
                    pk = f"{date_key}|{part_mic}"
                else:
                    pk = f"{date_key}|{part_mic}"
                yield AssetObservation(
                    asset_key=asset.key,
                    partition=pk,
                    metadata={
                        "s3_path": path,
                        "size_bytes": meta.get("size_bytes"),
                        "last_modified": meta.get("last_modified"),
                        "source": "s3_sync_asset",
                    },
                )
                yield AssetMaterialization(
                    asset_key=asset.key,
                    partition=pk,
                    metadata={
                        "s3_path": path,
                        "size_bytes": meta.get("size_bytes"),
                        "last_modified": meta.get("last_modified"),
                        "source": "s3_sync_asset",
                    },
                )
                emitted += 1
                if emitted >= max_events:
                    break
            if emitted >= max_events:
                break

        context.log.info("sync asset emitted=%d", emitted)

    return _sync_asset


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
            "UNIVERSE": partition.universe.spec if partition.universe else None,
        }
    )

    get_universe = default_get_universe
    if partition.universe:
        spec = partition.universe.spec
        if spec and spec != "default" and "+" in spec:
            parts = [s for s in spec.split("+") if s]
            resolvers = [_resolve_universe_callable(p) for p in parts]

            def _combined(date, _resolvers=resolvers):
                frames = [resolver(date) for resolver in _resolvers]
                return _intersect_universe_frames(frames)

            get_universe = _combined
        elif spec and spec != "default":
            get_universe = _resolve_universe_callable(spec)

    run_multiday_pipeline(
        config=config,
        get_universe=get_universe,
        get_pipeline=get_pipeline,
    )

    if on_result:
        on_result(partition, config)


def _discover_modules(pkg: str):
    import importlib
    import pkgutil

    modules = []
    pkg = importlib.import_module(pkg)
    for mod in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + "."):
        module = importlib.import_module(mod.name)
        if not hasattr(module, "USER_CONFIG") or not hasattr(module, "get_universe"):
            continue
        modules.append(module)
    return modules


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


def _s3_list_all_objects(
    prefix: str, *, force_refresh: bool = False
) -> dict[str, dict]:
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
                "last_modified": (
                    item.get("LastModified").isoformat()
                    if item.get("LastModified")
                    else None
                ),
            }
    _s3_cache_store(prefix, objects)
    logging.info(
        "s3 list refresh prefix=%s objects=%d",
        prefix,
        len(objects),
    )
    return objects


def _format_size_bytes(value: int | None) -> str:
    if value is None:
        return "n/a"
    size = float(value)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{int(value)} B"


def _build_s3_date_stats_metadata(
    *,
    table,
    date_key: str,
    max_rows: int = 20,
    metadata_value_cls=None,
) -> dict:
    prefix = _s3_table_root_prefix(table)
    if not prefix:
        return {"stats_reason": "missing_s3_prefix", "stats_date": date_key}

    objects = _s3_list_all_objects(prefix)
    if not objects:
        return {"stats_reason": "no_objects", "stats_date": date_key}

    size_by_mic: dict[str, int] = {}
    files_by_mic: dict[str, int] = {}
    total_files = 0
    total_size = 0

    for path, meta in objects.items():
        parsed = _parse_table_s3_path(table, path)
        if not parsed:
            continue
        mic, obj_date = parsed
        if obj_date != date_key:
            continue
        total_files += 1
        size = meta.get("size_bytes") or 0
        total_size += size
        size_by_mic[mic] = size_by_mic.get(mic, 0) + size
        files_by_mic[mic] = files_by_mic.get(mic, 0) + 1

    if total_files == 0:
        return {"stats_reason": "no_objects_for_date", "stats_date": date_key}

    rows = sorted(size_by_mic.items(), key=lambda item: item[1], reverse=True)
    if max_rows > 0:
        rows = rows[:max_rows]

    header = "| mic | files | size_bytes | size |"
    sep = "| --- | --- | --- | --- |"
    lines = [header, sep]
    for mic, size in rows:
        lines.append(
            f"| {mic} | {files_by_mic.get(mic, 0)} | {size} | {_format_size_bytes(size)} |"
        )
    table_md = "\n".join(lines)

    metadata: dict = {
        "stats_date": date_key,
        "stats_mic_count": len(size_by_mic),
        "stats_total_files": total_files,
        "stats_total_size_bytes": total_size,
    }

    if metadata_value_cls:
        metadata["stats_mic_table"] = metadata_value_cls.md(table_md)
    else:
        metadata["stats_mic_table"] = table_md

    return metadata


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
    return os.getenv("S3_CHECK_FULL_ON_UNPARTITIONED", "1") not in {
        "0",
        "false",
        "False",
    }


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


def _date_in_range(date_key: str, start: str | None, end: str | None) -> bool:
    if not start and not end:
        return True
    try:
        value = dt.date.fromisoformat(date_key)
    except Exception:
        return False
    if start:
        try:
            if value < dt.date.fromisoformat(start):
                return False
        except Exception:
            return False
    if end:
        try:
            if value > dt.date.fromisoformat(end):
                return False
        except Exception:
            return False
    return True


def _shard_accept(value: str, index: int, total: int) -> bool:
    if total <= 1:
        return True
    digest = sha1(value.encode("utf-8")).hexdigest()
    bucket = int(digest, 16) % total
    return bucket == index


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
        "cbbo_analytics": CBBOAnalytics.REQUIRES,
        "iceberg": IcebergAnalytics.REQUIRES,
    }
