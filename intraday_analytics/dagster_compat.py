from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, Iterable, Sequence

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
