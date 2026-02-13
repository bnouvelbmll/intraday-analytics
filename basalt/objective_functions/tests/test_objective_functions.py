from __future__ import annotations

import math

from basalt.objective_functions import (
    DatasetSplit,
    DirectionalAccuracyObjective,
    MeanAbsoluteErrorObjective,
    MeanSquaredErrorObjective,
    ModelObjectiveEvaluator,
    make_optimization_score_fn,
    objectives_from_names,
)


class _MeanModel:
    def fit(self, _x, y):
        ys = [float(v) for v in y]
        self._mean = sum(ys) / float(len(ys))

    def predict(self, x):
        return [self._mean for _ in x]


class _IntervalModel:
    def fit(self, _x, _y):
        return None

    def predict(self, x):
        return [12.0 for _ in x]

    def predict_interval(self, x):
        pred = [12.0 for _ in x]
        lower = [11.0 for _ in x]
        upper = [13.0 for _ in x]
        return pred, lower, upper


class _ConfidenceModel:
    def fit(self, _x, _y):
        return None

    def predict(self, x):
        return [12.0 for _ in x]

    def predict_with_confidence(self, x):
        pred = [12.0 for _ in x]
        confidence = [0.8 for _ in x]
        return pred, confidence


def _dataset_builder(**_kwargs):
    return DatasetSplit(
        X_train=[1, 2, 3, 4],
        y_train=[10.0, 12.0, 14.0, 16.0],
        X_eval=[5, 6, 7],
        y_eval=[12.0, 12.0, 12.0],
        metadata={"task": "price_level"},
    )


def test_objective_evaluator_returns_metrics():
    evaluator = ModelObjectiveEvaluator(
        dataset_builder=_dataset_builder,
        model_factory=_MeanModel,
        objectives=[
            MeanAbsoluteErrorObjective(),
            MeanSquaredErrorObjective(),
            DirectionalAccuracyObjective(),
        ],
    )
    report = evaluator.evaluate()
    assert report.primary_objective == "mae"
    assert report.metadata["task"] == "price_level"
    by_name = {r.objective: r.score for r in report.results}
    assert math.isclose(by_name["mae"], 1.0, rel_tol=1e-9)
    assert math.isclose(by_name["mse"], 1.0, rel_tol=1e-9)
    assert math.isclose(by_name["directional_accuracy"], 1.0, rel_tol=1e-9)


def test_optimization_score_fn_variants():
    evaluator = ModelObjectiveEvaluator(
        dataset_builder=_dataset_builder,
        model_factory=_MeanModel,
        objectives=[MeanAbsoluteErrorObjective(), DirectionalAccuracyObjective()],
    )
    score_primary = make_optimization_score_fn(evaluator)
    score_named = make_optimization_score_fn(
        evaluator, objective="directional_accuracy"
    )
    score_agg = make_optimization_score_fn(evaluator, use_aggregate=True)
    assert math.isclose(score_primary(), 1.0, rel_tol=1e-9)
    assert math.isclose(score_named(), 1.0, rel_tol=1e-9)
    # Aggregate is normalized: mae contributes as -1.0, directional as +1.0.
    assert math.isclose(score_agg(), 0.0, rel_tol=1e-9)


def test_interval_uncertainty_summary():
    evaluator = ModelObjectiveEvaluator(
        dataset_builder=_dataset_builder,
        model_factory=_IntervalModel,
        objectives=[MeanAbsoluteErrorObjective()],
    )
    report = evaluator.evaluate()
    assert math.isclose(report.uncertainty["mean_interval_width"], 2.0, rel_tol=1e-9)
    assert math.isclose(report.uncertainty["interval_coverage"], 1.0, rel_tol=1e-9)


def test_confidence_uncertainty_summary():
    evaluator = ModelObjectiveEvaluator(
        dataset_builder=_dataset_builder,
        model_factory=_ConfidenceModel,
        objectives=[MeanAbsoluteErrorObjective()],
    )
    report = evaluator.evaluate()
    assert math.isclose(report.uncertainty["mean_confidence"], 0.8, rel_tol=1e-9)
    assert math.isclose(report.uncertainty["min_confidence"], 0.8, rel_tol=1e-9)
    assert math.isclose(report.uncertainty["max_confidence"], 0.8, rel_tol=1e-9)


def test_objectives_from_names_variants():
    objs = objectives_from_names("mae,rmse,direction")
    names = [o.name for o in objs]
    assert names == ["mae", "mse", "directional_accuracy"]
    rmse_obj = objs[1]
    assert getattr(rmse_obj, "root", False) is True
