from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from .core import optimize_pipeline


def _load_search_space(
    *,
    search_space: str | dict[str, Any] | None = None,
    search_space_file: str | None = None,
) -> dict[str, Any]:
    if search_space_file:
        path = Path(search_space_file)
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(payload, dict):
            raise ValueError("Search space file must define a dictionary payload.")
        return payload
    if isinstance(search_space, dict):
        return search_space
    if isinstance(search_space, str) and search_space.strip():
        payload = json.loads(search_space)
        if not isinstance(payload, dict):
            raise ValueError("Search space JSON must define an object payload.")
        return payload
    raise ValueError("Provide --search_space_file or --search_space JSON.")


class OptimizeCLI:
    @staticmethod
    def run(
        *,
        pipeline: str,
        score_fn: str | None = None,
        trials: int = 20,
        maximize: bool = True,
        seed: int = 0,
        output_dir: str = "optimize_results",
        executor: str = "direct",
        instance_size: int | None = None,
        delete_after: bool | None = None,
        dagster_job: str | None = None,
        dagster_partition: str | None = None,
        tracker: str = "none",
        tracker_project: str | None = None,
        tracker_experiment: str | None = None,
        tracker_run_name: str | None = None,
        tracker_tags: str | dict[str, Any] | None = None,
        tracker_uri: str | None = None,
        tracker_mode: str | None = None,
        model_factory: str | None = None,
        dataset_builder: str | None = None,
        objectives: str | None = None,
        objective: str | None = None,
        use_aggregate: bool = False,
        search_generator: str | None = None,
        search_space: str | dict[str, Any] | None = None,
        search_space_file: str | None = None,
    ):
        return optimize_pipeline(
            pipeline=pipeline,
            search_space=_load_search_space(
                search_space=search_space, search_space_file=search_space_file
            ),
            trials=trials,
            score_fn=score_fn,
            maximize=maximize,
            seed=seed,
            output_dir=output_dir,
            executor=executor,
            instance_size=instance_size,
            delete_after=delete_after,
            dagster_job=dagster_job,
            dagster_partition=dagster_partition,
            tracker=tracker,
            tracker_project=tracker_project,
            tracker_experiment=tracker_experiment,
            tracker_run_name=tracker_run_name,
            tracker_tags=tracker_tags,
            tracker_uri=tracker_uri,
            tracker_mode=tracker_mode,
            model_factory=model_factory,
            dataset_builder=dataset_builder,
            objectives=objectives,
            objective=objective,
            use_aggregate=use_aggregate,
            search_generator=search_generator,
        )


def get_cli_extension():
    return {"optimize": OptimizeCLI}
