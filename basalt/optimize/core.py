from __future__ import annotations

import copy
import importlib
import json
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from basalt.basalt import _load_pipeline_module
from basalt.cli import run_cli, resolve_user_config
from .tracking import create_tracker


@dataclass
class TrialResult:
    trial_id: int
    score: float | None
    params: dict[str, Any]
    status: str
    error: str | None = None
    executor_result: dict[str, Any] | None = None


def _parse_path(path: str) -> list[str | int]:
    tokens: list[str | int] = []
    cur = ""
    i = 0
    while i < len(path):
        ch = path[i]
        if ch == ".":
            if cur:
                tokens.append(cur)
                cur = ""
            i += 1
            continue
        if ch == "[":
            if cur:
                tokens.append(cur)
                cur = ""
            j = path.find("]", i)
            if j < 0:
                raise ValueError(f"Invalid override path: {path}")
            idx = path[i + 1 : j]
            tokens.append(int(idx))
            i = j + 1
            continue
        cur += ch
        i += 1
    if cur:
        tokens.append(cur)
    return tokens


def set_nested_value(cfg: dict[str, Any], path: str, value: Any) -> dict[str, Any]:
    out = copy.deepcopy(cfg)
    tokens = _parse_path(path)
    cur: Any = out
    for tok in tokens[:-1]:
        if isinstance(tok, int):
            while len(cur) <= tok:
                cur.append({})
            cur = cur[tok]
        else:
            if tok not in cur or cur[tok] is None:
                cur[tok] = {}
            cur = cur[tok]
    leaf = tokens[-1]
    if isinstance(leaf, int):
        while len(cur) <= leaf:
            cur.append(None)
        cur[leaf] = value
    else:
        cur[leaf] = value
    return out


def apply_overrides(cfg: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(cfg)
    for path, value in overrides.items():
        out = set_nested_value(out, path, value)
    return out


def sample_params(
    search_space: dict[str, Any],
    *,
    rng: random.Random,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    specs = search_space.get("params") or {}
    for path, spec in specs.items():
        if not isinstance(spec, dict):
            params[path] = spec
            continue
        kind = str(spec.get("type", "choice")).lower()
        if kind == "choice":
            values = list(spec.get("values") or [])
            if not values:
                raise ValueError(f"Empty choice values for {path}")
            params[path] = rng.choice(values)
        elif kind == "int":
            low = int(spec["low"])
            high = int(spec["high"])
            params[path] = rng.randint(low, high)
        elif kind == "float":
            low = float(spec["low"])
            high = float(spec["high"])
            params[path] = rng.uniform(low, high)
        elif kind == "bool":
            params[path] = bool(rng.choice([False, True]))
        else:
            raise ValueError(f"Unknown param type '{kind}' for {path}")
    return params


def _load_callable(target: str) -> Callable[..., Any]:
    if ":" not in target:
        raise ValueError("Callable must be in format module:function")
    module_name, attr = target.split(":", 1)
    module = importlib.import_module(module_name)
    fn = getattr(module, attr, None)
    if fn is None or not callable(fn):
        raise ValueError(f"Invalid callable target: {target}")
    return fn


def _build_scorer(
    *,
    score_fn: str | None,
    model_factory: str | None,
    dataset_builder: str | None,
    objectives: str | None,
    objective: str | None,
    use_aggregate: bool,
) -> Callable[..., Any] | None:
    if score_fn:
        return _load_callable(score_fn)
    if not (model_factory and dataset_builder and objectives):
        return None
    try:
        from basalt.models.optimization import make_model_objective_score_fn
    except Exception as exc:
        raise RuntimeError(
            "Model/objective scoring requires basalt models package. "
            "Install bmll-basalt-models."
        ) from exc

    model_factory_fn = _load_callable(model_factory)
    dataset_builder_fn = _load_callable(dataset_builder)
    if ":" in objectives:
        objectives_fn = _load_callable(objectives)
        objective_list = objectives_fn()
    else:
        from basalt.objective_functions import objectives_from_names

        objective_list = objectives_from_names(objectives)
    return make_model_objective_score_fn(
        dataset_builder=dataset_builder_fn,
        model_factory=model_factory_fn,
        objectives=objective_list,
        objective=objective,
        use_aggregate=use_aggregate,
    )


def _load_base_user_config(pipeline: str) -> tuple[dict[str, Any], Any]:
    module = _load_pipeline_module(pipeline)
    if not hasattr(module, "USER_CONFIG"):
        raise ValueError("Pipeline module must define USER_CONFIG")
    if not hasattr(module, "get_universe"):
        raise ValueError("Pipeline module must define get_universe(date)")
    config_file = getattr(module, "__file__", None) or pipeline
    return resolve_user_config(dict(module.USER_CONFIG), config_file), module


def _run_pipeline_with_config(module: Any, user_config: dict[str, Any]) -> Any:
    return run_cli(
        user_config,
        module.get_universe,
        get_pipeline=getattr(module, "get_pipeline", None),
        config_file=getattr(module, "__file__", None),
    )


def _extract_ids(job_run: Any) -> tuple[str | None, str | None]:
    job_id = None
    run_id = None
    if isinstance(job_run, tuple) and len(job_run) == 2:
        job, run = job_run
        job_id = getattr(job, "id", None)
        run_id = getattr(run, "id", None) or getattr(run, "run_id", None)
    return (
        str(job_id) if job_id is not None else None,
        str(run_id) if run_id is not None else None,
    )


def _dispatch_trial(
    *,
    executor: str,
    pipeline: str,
    user_config: dict[str, Any],
    trial_id: int,
    instance_size: int | None,
    delete_after: bool | None,
    dagster_job: str | None,
    dagster_partition: str | None,
) -> dict[str, Any]:
    payload = json.dumps(user_config, sort_keys=True)
    trial_name = f"[basalt][optimize][trial:{trial_id}]"
    if executor == "bmll":
        from basalt.cli import bmll_job_run

        result = bmll_job_run(
            config_file=pipeline,
            name=trial_name,
            instance_size=instance_size,
            delete_after=delete_after,
            user_config_json=payload,
        )
        job_id, run_id = _extract_ids(result)
        return {"status": "submitted", "job_id": job_id, "run_id": run_id}
    if executor == "ec2":
        from basalt.executors.aws_ec2 import ec2_run

        result = ec2_run(
            pipeline=pipeline,
            name=trial_name,
            instance_size=instance_size,
            delete_after=delete_after,
            user_config_json=payload,
        )
        job_id, run_id = _extract_ids(result)
        return {"status": "submitted", "job_id": job_id, "run_id": run_id}
    if executor == "dagster":
        raise ValueError(
            "Executor 'dagster' is not supported for optimize trials yet. "
            "Use direct/bmll/ec2 for now."
        )
    if executor == "kubernetes":
        raise ValueError(
            "Executor 'kubernetes' is not supported for optimize trials yet. "
            "Use direct/bmll/ec2 for now."
        )
    raise ValueError(f"Unsupported executor '{executor}'.")


def _generate_trial_params(
    *,
    trial_id: int,
    search_space: dict[str, Any],
    search_generator_fn: Callable[..., Any] | None,
    rng: random.Random,
    history: list[TrialResult],
    base_config: dict[str, Any],
) -> dict[str, Any]:
    if search_generator_fn is None:
        return sample_params(search_space, rng=rng)
    payload = search_generator_fn(
        trial_id=trial_id,
        search_space=search_space,
        rng=rng,
        history=history,
        base_config=base_config,
    )
    if not isinstance(payload, dict):
        raise ValueError("search_generator must return a dict[path, value].")
    return payload


def optimize_pipeline(
    *,
    pipeline: str,
    search_space: dict[str, Any],
    trials: int,
    score_fn: str | None = None,
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
    tracker_tags: dict[str, Any] | str | None = None,
    tracker_uri: str | None = None,
    tracker_mode: str | None = None,
    model_factory: str | None = None,
    dataset_builder: str | None = None,
    objectives: str | None = None,
    objective: str | None = None,
    use_aggregate: bool = False,
    search_generator: str | None = None,
) -> dict[str, Any]:
    out_dir = Path(output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    base_cfg, module = _load_base_user_config(pipeline)
    executor = str(executor).strip().lower()
    scorer = _build_scorer(
        score_fn=score_fn,
        model_factory=model_factory,
        dataset_builder=dataset_builder,
        objectives=objectives,
        objective=objective,
        use_aggregate=use_aggregate,
    )
    if executor == "direct" and scorer is None:
        raise ValueError(
            "For executor='direct', provide score_fn or "
            "(model_factory + dataset_builder + objectives)."
        )
    search_generator_fn = _load_callable(search_generator) if search_generator else None
    rng = random.Random(seed)
    tracker_impl = create_tracker(
        tracker=tracker,
        tracker_project=tracker_project,
        tracker_experiment=tracker_experiment,
        tracker_run_name=tracker_run_name,
        tracker_tags=tracker_tags,
        tracker_uri=tracker_uri,
        tracker_mode=tracker_mode,
    )
    tracker_impl.start(
        pipeline=pipeline,
        maximize=bool(maximize),
        metadata={"executor": executor, "trials": int(trials)},
    )

    all_results: list[TrialResult] = []
    for trial_id in range(1, int(trials) + 1):
        started = time.perf_counter()
        params = _generate_trial_params(
            trial_id=trial_id,
            search_space=search_space,
            search_generator_fn=search_generator_fn,
            rng=rng,
            history=all_results,
            base_config=base_cfg,
        )
        cfg = apply_overrides(base_cfg, params)
        status = "ok"
        error = None
        score: float | None = None
        executor_result: dict[str, Any] | None = None
        try:
            if executor == "direct":
                pipeline_result = _run_pipeline_with_config(module, cfg)
                raw_score = scorer(
                    pipeline_result=pipeline_result,
                    config=cfg,
                    params=params,
                    trial_id=trial_id,
                )
                score = float(raw_score)
            else:
                executor_result = _dispatch_trial(
                    executor=executor,
                    pipeline=pipeline,
                    user_config=cfg,
                    trial_id=trial_id,
                    instance_size=instance_size,
                    delete_after=delete_after,
                    dagster_job=dagster_job,
                    dagster_partition=dagster_partition,
                )
                status = str(executor_result.get("status", "submitted"))
        except Exception as exc:
            status = "error"
            error = str(exc)
            score = float("-inf") if maximize else float("inf")
        duration_seconds = time.perf_counter() - started

        result = TrialResult(
            trial_id=trial_id,
            score=score,
            params=params,
            status=status,
            error=error,
            executor_result=executor_result,
        )
        all_results.append(result)
        tracker_impl.log_trial(
            trial_id=trial_id,
            status=status,
            score=score,
            params=params,
            error=error,
            executor=executor,
            duration_seconds=duration_seconds,
            executor_result=executor_result,
        )

        with (out_dir / "trials.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    {
                        "trial_id": trial_id,
                        "score": score,
                        "status": status,
                        "error": error,
                        "params": params,
                        "executor_result": executor_result,
                    },
                    sort_keys=True,
                )
                + "\n"
            )

    scored = [r for r in all_results if r.status == "ok" and r.score is not None]
    ranked = sorted(scored, key=lambda r: float(r.score), reverse=maximize)
    best = ranked[0] if ranked else None
    payload = {
        "pipeline": pipeline,
        "trials": int(trials),
        "maximize": bool(maximize),
        "seed": int(seed),
        "executor": executor,
        "tracker": str(getattr(tracker_impl, "name", tracker)),
        "best": (
            {
                "trial_id": best.trial_id,
                "score": best.score,
                "params": best.params,
                "status": best.status,
                "error": best.error,
            }
            if best
            else None
        ),
        "successful_trials": sum(1 for r in all_results if r.status == "ok"),
        "submitted_trials": sum(1 for r in all_results if r.status == "submitted"),
        "failed_trials": sum(1 for r in all_results if r.status == "error"),
        "results_file": str(out_dir / "trials.jsonl"),
    }
    (out_dir / "summary.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    tracker_impl.finish(payload)
    return payload
