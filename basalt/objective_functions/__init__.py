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
]


def get_basalt_plugin():
    return {
        "name": "objective_functions",
        "provides": ["predictive objective evaluation"],
        "cli_extensions": [],
    }
