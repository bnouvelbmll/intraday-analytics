from __future__ import annotations

from typing import Any, Callable, Sequence

from basalt.objective_functions import ModelObjectiveEvaluator, make_optimization_score_fn
from basalt.objective_functions.objectives import Objective


def make_model_objective_score_fn(
    *,
    dataset_builder: Callable[..., Any],
    model_factory: Callable[[], Any],
    objectives: Sequence[Objective],
    objective: str | None = None,
    use_aggregate: bool = False,
):
    """
    Build an optimize-compatible score function from model + objective configuration.
    """
    evaluator = ModelObjectiveEvaluator(
        dataset_builder=dataset_builder,
        model_factory=model_factory,
        objectives=objectives,
    )
    return make_optimization_score_fn(
        evaluator,
        objective=objective,
        use_aggregate=use_aggregate,
    )
