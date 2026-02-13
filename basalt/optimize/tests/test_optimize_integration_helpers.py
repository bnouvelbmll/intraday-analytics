from __future__ import annotations

from basalt.objective_functions import DatasetSplit, MeanAbsoluteErrorObjective
from basalt.models import SklearnModel


def generator(**_kwargs):
    return {"A": 2}


def dataset_builder(**_kwargs):
    return DatasetSplit(
        X_train=[1, 2, 3],
        y_train=[10.0, 12.0, 14.0],
        X_eval=[4, 5],
        y_eval=[12.0, 12.0],
    )


def model_factory():
    return SklearnModel(estimator_class="basalt.models._testing_estimators:MeanRegressor")


def objectives():
    return [MeanAbsoluteErrorObjective()]
