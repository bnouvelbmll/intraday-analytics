from __future__ import annotations

import math

from basalt.models import SklearnModel, evaluate_model_with_objectives, train_model
from basalt.models.autogluon_model import AutoGluonTabularModel
from basalt.objective_functions import DatasetSplit, MeanAbsoluteErrorObjective


def _split():
    return DatasetSplit(
        X_train=[1, 2, 3],
        y_train=[10.0, 12.0, 14.0],
        X_eval=[4, 5],
        y_eval=[12.0, 12.0],
    )


def test_sklearn_model_fit_predict_save_load(tmp_path):
    model = SklearnModel(estimator_class="basalt.models._testing_estimators:MeanRegressor")
    split = _split()
    model.fit(split.X_train, split.y_train)
    pred = model.predict(split.X_eval)
    assert len(pred) == 2
    path = tmp_path / "model.pkl"
    model.save(str(path))
    loaded = SklearnModel.load(str(path))
    pred2 = loaded.predict(split.X_eval)
    assert pred2 == pred


def test_train_and_objective_evaluation(tmp_path):
    model = SklearnModel(estimator_class="basalt.models._testing_estimators:MeanRegressor")
    split = _split()
    artifact = train_model(
        model,
        split,
        artifact_path=str(tmp_path / "m.pkl"),
        metadata={"task": "price_after_t"},
    )
    assert artifact.model_type == "SklearnModel"
    assert artifact.metadata["task"] == "price_after_t"
    report = evaluate_model_with_objectives(
        model,
        split,
        objectives=[MeanAbsoluteErrorObjective()],
    )
    assert report.results[0].objective == "mae"
    assert math.isclose(report.results[0].score, 0.0, rel_tol=1e-9)


def test_autogluon_predict_normalizes_list_input():
    class _Predictor:
        def predict(self, x):
            assert hasattr(x, "columns")
            return [1.0 for _ in range(len(x))]

    model = AutoGluonTabularModel()
    model.predictor = _Predictor()
    out = model.predict([[1.0, 2.0], [3.0, 4.0]])
    assert out == [1.0, 1.0]
