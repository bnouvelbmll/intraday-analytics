from __future__ import annotations

import pytest

from basalt.models.autogluon_model import AutoGluonTabularModel
from basalt.models.pymc_model import PyMCModel


def test_autogluon_model_requires_package(monkeypatch):
    model = AutoGluonTabularModel()
    # Force import failure through builtin import.
    import builtins

    real_import = builtins.__import__

    def _imp(name, *args, **kwargs):
        if name.startswith("autogluon"):
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _imp)
    with pytest.raises(RuntimeError, match="autogluon is not installed"):
        model.fit([], [])


def test_pymc_model_requires_package(monkeypatch):
    model = PyMCModel(build_and_fit_fn="basalt.models._testing_estimators:MeanRegressor", predict_fn="basalt.models._testing_estimators:MeanRegressor")
    import builtins

    real_import = builtins.__import__

    def _imp(name, *args, **kwargs):
        if name == "pymc":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _imp)
    with pytest.raises(RuntimeError, match="pymc is not installed"):
        model.fit([], [])
