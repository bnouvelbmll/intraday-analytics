from __future__ import annotations

from dataclasses import dataclass, field
import importlib
import pickle
from pathlib import Path
from typing import Any


def _load_class(path: str) -> type:
    if ":" in path:
        module_name, cls_name = path.split(":", 1)
    else:
        module_name, cls_name = path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    cls = getattr(module, cls_name, None)
    if cls is None:
        raise ValueError(f"Estimator class not found: {path}")
    return cls


@dataclass
class SklearnModel:
    """
    Generic sklearn-style adapter.

    `estimator_class` may be any import path exposing a class with
    `fit(X, y)` and `predict(X)`.
    """

    estimator_class: str
    estimator_params: dict[str, Any] = field(default_factory=dict)
    estimator: Any | None = None

    def _ensure_estimator(self) -> Any:
        if self.estimator is None:
            cls = _load_class(self.estimator_class)
            self.estimator = cls(**self.estimator_params)
        return self.estimator

    def fit(self, X: Any, y: Any) -> Any:
        est = self._ensure_estimator()
        return est.fit(X, y)

    def predict(self, X: Any) -> Any:
        est = self._ensure_estimator()
        return est.predict(X)

    def predict_with_confidence(self, X: Any) -> tuple[Any, list[float]]:
        est = self._ensure_estimator()
        preds = est.predict(X)
        if hasattr(est, "predict_proba"):
            probas = est.predict_proba(X)
            conf = [float(max(row)) for row in probas]
            return preds, conf
        return preds, [1.0 for _ in preds]

    def save(self, path: str) -> str:
        est = self._ensure_estimator()
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("wb") as fh:
            pickle.dump(
                {
                    "estimator_class": self.estimator_class,
                    "estimator_params": self.estimator_params,
                    "estimator": est,
                },
                fh,
            )
        return str(p)

    @classmethod
    def load(cls, path: str) -> "SklearnModel":
        with Path(path).open("rb") as fh:
            payload = pickle.load(fh)
        return cls(
            estimator_class=str(payload["estimator_class"]),
            estimator_params=dict(payload.get("estimator_params") or {}),
            estimator=payload.get("estimator"),
        )

    def to_config(self) -> dict[str, Any]:
        return {
            "type": "sklearn",
            "estimator_class": self.estimator_class,
            "estimator_params": dict(self.estimator_params),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "SklearnModel":
        return cls(
            estimator_class=str(config["estimator_class"]),
            estimator_params=dict(config.get("estimator_params") or {}),
        )
