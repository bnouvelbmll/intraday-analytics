from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, Sequence

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


def build_demo_assets(demo_pkg: str = "demo"):
    """
    Discover demo modules and return Dagster assets for each demo.
    """
    try:
        import importlib
        import pkgutil
        from dagster import asset
    except Exception as exc:
        raise ImportError("Dagster is required to build demo assets.") from exc

    demos = []
    pkg = importlib.import_module(demo_pkg)
    for mod in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + "."):
        module = importlib.import_module(mod.name)
        if not hasattr(module, "USER_CONFIG") or not hasattr(module, "get_universe"):
            continue
        demos.append(module)

    assets = []
    for module in demos:
        name = module.__name__.split(".")[-1]
        asset_name = f"demo{name}" if name[0].isdigit() else name

        @asset(name=asset_name)
        def _demo_asset(module=module):
            base_config = module.USER_CONFIG
            default_get_universe = module.get_universe
            get_pipeline = getattr(module, "get_pipeline", None)
            run_partition(
                base_config=base_config,
                default_get_universe=default_get_universe,
                partition=PartitionRun(
                    universe=UniversePartition(name="default", value=None),
                    dates=DatePartition(
                        start_date=base_config["START_DATE"],
                        end_date=base_config["END_DATE"],
                    ),
                ),
                get_pipeline=get_pipeline,
            )

        assets.append(_demo_asset)

    return assets


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
