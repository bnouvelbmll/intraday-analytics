from __future__ import annotations

from .base import BasePredictiveModel, ModelArtifact
from .sklearn_model import SklearnModel
from .autogluon_model import AutoGluonTabularModel
from .pymc_model import PyMCModel
from .training import evaluate_model_with_objectives, train_model
from .optimization import make_model_objective_score_fn

__all__ = [
    "BasePredictiveModel",
    "ModelArtifact",
    "SklearnModel",
    "AutoGluonTabularModel",
    "PyMCModel",
    "train_model",
    "evaluate_model_with_objectives",
    "make_model_objective_score_fn",
]


def get_basalt_plugin():
    return {
        "name": "models",
        "provides": ["model training", "model serialization", "predictive adapters"],
        "cli_extensions": [],
    }
