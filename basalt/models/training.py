from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from basalt.objective_functions import DatasetSplit, ModelObjectiveEvaluator
from basalt.objective_functions.objectives import Objective

from .base import BasePredictiveModel, ModelArtifact


def train_model(
    model: BasePredictiveModel,
    split: DatasetSplit,
    *,
    artifact_path: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> ModelArtifact:
    """Train a model and optionally persist it as an artifact."""
    model.fit(split.X_train, split.y_train)
    final_path = artifact_path
    if final_path:
        p = Path(final_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        model.save(str(p))
    return ModelArtifact(
        model_type=type(model).__name__,
        artifact_path=str(final_path or ""),
        config=model.to_config() if hasattr(model, "to_config") else {},
        metadata=metadata or {},
    )


def evaluate_model_with_objectives(
    model: BasePredictiveModel,
    split: DatasetSplit,
    objectives: Sequence[Objective],
):
    """
    Evaluate a model with shared objective-functions API.
    """
    evaluator = ModelObjectiveEvaluator(
        dataset_builder=lambda **_: split,
        model_factory=lambda: model,
        objectives=objectives,
    )
    return evaluator.evaluate()
