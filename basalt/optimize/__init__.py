from __future__ import annotations

from .core import optimize_pipeline
from .cli_ext import get_cli_extension
from .tracking import create_tracker
from .presets import (
    autogluon_fast_model_factory,
    dataset_builder_from_last_pass_output,
    history_guided_generator,
    simple_linear_model_factory,
)

__all__ = [
    "optimize_pipeline",
    "get_cli_extension",
    "create_tracker",
    "autogluon_fast_model_factory",
    "dataset_builder_from_last_pass_output",
    "history_guided_generator",
    "simple_linear_model_factory",
]


def get_basalt_plugin():
    return {
        "name": "optimize",
        "provides": ["configuration optimization", "experiment search"],
        "cli_extensions": ["optimize"],
    }
