from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Sequence

from .objectives import Objective


@dataclass
class DatasetSplit:
    """Train/evaluation split used by objective evaluation."""

    X_train: Any
    y_train: Iterable[float]
    X_eval: Any
    y_eval: Iterable[float]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EvaluationResult:
    """One objective evaluation result."""

    objective: str
    score: float
    higher_is_better: bool


@dataclass
class EvaluationReport:
    """Aggregate report over all configured objectives."""

    results: list[EvaluationResult]
    aggregate_score: float
    primary_objective: str
    uncertainty: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


class ModelObjectiveEvaluator:
    """
    Evaluate third-party predictive models on a training/evaluation split.

    `dataset_builder` receives pipeline artifacts/configuration and must return a
    `DatasetSplit`. `model_factory` must produce an object exposing `fit` and `predict`.
    """

    def __init__(
        self,
        *,
        dataset_builder: Callable[..., DatasetSplit],
        model_factory: Callable[[], Any],
        objectives: Sequence[Objective],
    ) -> None:
        if not objectives:
            raise ValueError("At least one objective is required.")
        self.dataset_builder = dataset_builder
        self.model_factory = model_factory
        self.objectives = list(objectives)

    @staticmethod
    def _normalize_numeric_vector(value: Any, n: int, name: str) -> list[float] | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return [float(value)] * n
        out = [float(v) for v in value]
        if len(out) != n:
            raise ValueError(f"{name} length mismatch: expected {n}, got {len(out)}.")
        return out

    @classmethod
    def _extract_prediction_payload(cls, model: Any, x_eval: Any) -> dict[str, Any]:
        """
        Normalize model outputs to:
        - y_pred: list[float]
        - lower: list[float] | None
        - upper: list[float] | None
        - confidence: list[float] | None
        """
        pred_out = model.predict(x_eval)
        lower = None
        upper = None
        confidence = None

        # Direct dictionary payload from predict.
        if isinstance(pred_out, dict):
            y_pred = pred_out.get("predictions", pred_out.get("pred"))
            lower = pred_out.get("lower")
            upper = pred_out.get("upper")
            confidence = pred_out.get("confidence")
        else:
            y_pred = pred_out

        # Optional interval API.
        if hasattr(model, "predict_interval"):
            interval_out = model.predict_interval(x_eval)
            if isinstance(interval_out, dict):
                y_pred = interval_out.get("predictions", interval_out.get("pred", y_pred))
                lower = interval_out.get("lower", lower)
                upper = interval_out.get("upper", upper)
            elif isinstance(interval_out, tuple) and len(interval_out) == 3:
                y_pred, lower, upper = interval_out
            else:
                raise ValueError(
                    "predict_interval must return dict or tuple(pred, lower, upper)."
                )

        # Optional confidence API.
        if hasattr(model, "predict_with_confidence"):
            conf_out = model.predict_with_confidence(x_eval)
            if isinstance(conf_out, dict):
                y_pred = conf_out.get("predictions", conf_out.get("pred", y_pred))
                confidence = conf_out.get("confidence", confidence)
            elif isinstance(conf_out, tuple) and len(conf_out) == 2:
                y_pred, confidence = conf_out
            else:
                raise ValueError(
                    "predict_with_confidence must return dict or tuple(pred, confidence)."
                )

        y_pred_f = [float(v) for v in y_pred]
        n = len(y_pred_f)
        return {
            "y_pred": y_pred_f,
            "lower": cls._normalize_numeric_vector(lower, n, "lower"),
            "upper": cls._normalize_numeric_vector(upper, n, "upper"),
            "confidence": cls._normalize_numeric_vector(confidence, n, "confidence"),
        }

    @staticmethod
    def _summarize_uncertainty(
        *,
        y_true: list[float],
        lower: list[float] | None,
        upper: list[float] | None,
        confidence: list[float] | None,
    ) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if confidence is not None:
            out["mean_confidence"] = sum(confidence) / float(len(confidence))
            out["min_confidence"] = min(confidence)
            out["max_confidence"] = max(confidence)
        if lower is not None and upper is not None:
            widths = [u - l for l, u in zip(lower, upper)]
            coverage = sum(
                1 for y, l, u in zip(y_true, lower, upper) if l <= y <= u
            ) / float(len(y_true))
            out["mean_interval_width"] = sum(widths) / float(len(widths))
            out["interval_coverage"] = coverage
        return out

    def evaluate(
        self,
        *,
        pipeline_result: Any = None,
        config: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        trial_id: int | None = None,
    ) -> EvaluationReport:
        split = self.dataset_builder(
            pipeline_result=pipeline_result,
            config=config or {},
            params=params or {},
            trial_id=trial_id,
        )
        model = self.model_factory()
        if not hasattr(model, "fit") or not hasattr(model, "predict"):
            raise ValueError("Model must implement fit(X, y) and predict(X).")
        model.fit(split.X_train, split.y_train)
        y_true = list(float(v) for v in split.y_eval)
        payload = self._extract_prediction_payload(model, split.X_eval)
        y_pred_f = payload["y_pred"]
        if len(y_true) != len(y_pred_f):
            raise ValueError(
                f"Model prediction length mismatch: y_true={len(y_true)} y_pred={len(y_pred_f)}."
            )

        rows: list[EvaluationResult] = []
        normalized: list[float] = []
        for objective in self.objectives:
            score = float(objective.evaluate(y_true, y_pred_f))
            rows.append(
                EvaluationResult(
                    objective=objective.name,
                    score=score,
                    higher_is_better=bool(objective.higher_is_better),
                )
            )
            normalized.append(score if objective.higher_is_better else -score)

        aggregate = sum(normalized) / float(len(normalized))
        return EvaluationReport(
            results=rows,
            aggregate_score=aggregate,
            primary_objective=rows[0].objective,
            uncertainty=self._summarize_uncertainty(
                y_true=y_true,
                lower=payload["lower"],
                upper=payload["upper"],
                confidence=payload["confidence"],
            ),
            metadata=dict(split.metadata),
        )


def make_optimization_score_fn(
    evaluator: ModelObjectiveEvaluator,
    *,
    objective: str | None = None,
    use_aggregate: bool = False,
) -> Callable[..., float]:
    """
    Build a `score_fn` compatible with `basalt.optimize`.

    By default, returns the first objective score from the evaluator report.
    Set `use_aggregate=True` to return normalized aggregate score instead.
    """

    def _score_fn(
        *,
        pipeline_result: Any = None,
        config: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        trial_id: int | None = None,
    ) -> float:
        report = evaluator.evaluate(
            pipeline_result=pipeline_result,
            config=config,
            params=params,
            trial_id=trial_id,
        )
        if use_aggregate:
            return float(report.aggregate_score)
        if objective:
            for row in report.results:
                if row.objective == objective:
                    return float(row.score)
            raise ValueError(f"Objective '{objective}' not found in evaluator results.")
        return float(report.results[0].score)

    return _score_fn
