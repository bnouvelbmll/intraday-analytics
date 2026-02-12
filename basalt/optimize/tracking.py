from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


def _to_scalar(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return json.dumps(value, sort_keys=True)


def _normalize_tags(tags: dict[str, Any] | str | None) -> dict[str, Any]:
    if tags is None:
        return {}
    if isinstance(tags, dict):
        return {str(k): _to_scalar(v) for k, v in tags.items()}
    if isinstance(tags, str):
        text = tags.strip()
        if not text:
            return {}
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError("tracker_tags JSON must be an object.")
        return {str(k): _to_scalar(v) for k, v in parsed.items()}
    raise ValueError("tracker_tags must be a dict, JSON object string, or None.")


class BaseTracker:
    name = "none"

    def start(self, *, pipeline: str, maximize: bool, metadata: dict[str, Any]) -> None:
        return None

    def log_trial(
        self,
        *,
        trial_id: int,
        status: str,
        score: float | None,
        params: dict[str, Any],
        error: str | None,
        executor: str,
        duration_seconds: float,
        executor_result: dict[str, Any] | None,
    ) -> None:
        return None

    def finish(self, summary: dict[str, Any]) -> None:
        return None


class NoopTracker(BaseTracker):
    name = "none"


@dataclass
class MlflowTracker(BaseTracker):
    tracking_uri: str | None = None
    experiment: str | None = None
    run_name: str | None = None
    tags: dict[str, Any] | str | None = None
    _run: Any = None
    _mlflow: Any = None

    name = "mlflow"

    def start(self, *, pipeline: str, maximize: bool, metadata: dict[str, Any]) -> None:
        try:
            import mlflow  # type: ignore[import-not-found]
        except Exception as exc:
            raise RuntimeError("mlflow is not installed. Install mlflow to use tracker='mlflow'.") from exc
        self._mlflow = mlflow
        if self.tracking_uri:
            mlflow.set_tracking_uri(self.tracking_uri)
        if self.experiment:
            mlflow.set_experiment(self.experiment)
        run_name = self.run_name or f"basalt-optimize:{pipeline}"
        self._run = mlflow.start_run(run_name=run_name)
        tags = _normalize_tags(self.tags)
        if tags:
            mlflow.set_tags(tags)
        mlflow.log_params(
            {
                "pipeline": pipeline,
                "maximize": bool(maximize),
                **{f"meta.{k}": _to_scalar(v) for k, v in metadata.items()},
            }
        )

    def log_trial(
        self,
        *,
        trial_id: int,
        status: str,
        score: float | None,
        params: dict[str, Any],
        error: str | None,
        executor: str,
        duration_seconds: float,
        executor_result: dict[str, Any] | None,
    ) -> None:
        mlflow = self._mlflow
        with mlflow.start_run(run_name=f"trial-{trial_id}", nested=True):
            mlflow.log_params({f"param.{k}": _to_scalar(v) for k, v in params.items()})
            mlflow.log_params(
                {
                    "trial_id": trial_id,
                    "status": status,
                    "executor": executor,
                }
            )
            if error:
                mlflow.log_param("error", error)
            if executor_result:
                for k, v in executor_result.items():
                    mlflow.log_param(f"executor.{k}", _to_scalar(v))
            mlflow.log_metric("duration_seconds", float(duration_seconds))
            if score is not None:
                mlflow.log_metric("score", float(score))

    def finish(self, summary: dict[str, Any]) -> None:
        if self._mlflow is None:
            return
        self._mlflow.log_metrics(
            {
                "successful_trials": float(summary.get("successful_trials", 0)),
                "submitted_trials": float(summary.get("submitted_trials", 0)),
                "failed_trials": float(summary.get("failed_trials", 0)),
            }
        )
        best = summary.get("best") or {}
        if "score" in best and best["score"] is not None:
            self._mlflow.log_metric("best_score", float(best["score"]))
        self._mlflow.end_run()


@dataclass
class WandbTracker(BaseTracker):
    project: str | None = None
    run_name: str | None = None
    tags: dict[str, Any] | str | None = None
    mode: str | None = None
    _run: Any = None

    name = "wandb"

    def start(self, *, pipeline: str, maximize: bool, metadata: dict[str, Any]) -> None:
        try:
            import wandb  # type: ignore[import-not-found]
        except Exception as exc:
            raise RuntimeError("wandb is not installed. Install wandb to use tracker='wandb'.") from exc

        tags = _normalize_tags(self.tags)
        cfg = {"pipeline": pipeline, "maximize": bool(maximize), **metadata}
        kwargs: dict[str, Any] = {
            "project": self.project or "basalt-optimize",
            "name": self.run_name,
            "config": cfg,
        }
        if self.mode:
            kwargs["mode"] = self.mode
        if tags:
            kwargs["tags"] = [f"{k}:{v}" for k, v in tags.items()]
        self._run = wandb.init(**kwargs)

    def log_trial(
        self,
        *,
        trial_id: int,
        status: str,
        score: float | None,
        params: dict[str, Any],
        error: str | None,
        executor: str,
        duration_seconds: float,
        executor_result: dict[str, Any] | None,
    ) -> None:
        if self._run is None:
            return
        payload: dict[str, Any] = {
            "trial_id": trial_id,
            "status": status,
            "executor": executor,
            "duration_seconds": float(duration_seconds),
        }
        if score is not None:
            payload["score"] = float(score)
        if error:
            payload["error"] = error
        for k, v in params.items():
            payload[f"param/{k}"] = _to_scalar(v)
        if executor_result:
            for k, v in executor_result.items():
                payload[f"executor/{k}"] = _to_scalar(v)
        self._run.log(payload, step=trial_id)

    def finish(self, summary: dict[str, Any]) -> None:
        if self._run is None:
            return
        best = summary.get("best") or {}
        payload: dict[str, Any] = {
            "successful_trials": summary.get("successful_trials", 0),
            "submitted_trials": summary.get("submitted_trials", 0),
            "failed_trials": summary.get("failed_trials", 0),
        }
        if "score" in best and best["score"] is not None:
            payload["best_score"] = best["score"]
        self._run.log(payload)
        self._run.finish()


def create_tracker(
    *,
    tracker: str = "none",
    tracker_project: str | None = None,
    tracker_experiment: str | None = None,
    tracker_run_name: str | None = None,
    tracker_tags: dict[str, Any] | str | None = None,
    tracker_uri: str | None = None,
    tracker_mode: str | None = None,
) -> BaseTracker:
    name = str(tracker).strip().lower()
    if name in {"", "none", "off", "disabled"}:
        return NoopTracker()
    if name == "mlflow":
        return MlflowTracker(
            tracking_uri=tracker_uri,
            experiment=tracker_experiment or tracker_project,
            run_name=tracker_run_name,
            tags=tracker_tags,
        )
    if name == "wandb":
        return WandbTracker(
            project=tracker_project,
            run_name=tracker_run_name,
            tags=tracker_tags,
            mode=tracker_mode,
        )
    raise ValueError("tracker must be one of: none, mlflow, wandb.")
