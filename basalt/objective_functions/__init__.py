from __future__ import annotations

from .evaluator import (
    DatasetSplit,
    EvaluationReport,
    EvaluationResult,
    ModelObjectiveEvaluator,
    make_optimization_score_fn,
)
from .objectives import (
    DirectionalAccuracyObjective,
    MeanAbsoluteErrorObjective,
    MeanSquaredErrorObjective,
    objectives_from_names,
)

__all__ = [
    "DatasetSplit",
    "EvaluationReport",
    "EvaluationResult",
    "ModelObjectiveEvaluator",
    "make_optimization_score_fn",
    "DirectionalAccuracyObjective",
    "MeanAbsoluteErrorObjective",
    "MeanSquaredErrorObjective",
    "objectives_from_names",
]


def get_basalt_plugin():
    return {
        "name": "objective_functions",
        "provides": ["predictive objective evaluation"],
        "cli_extensions": [],
    }
