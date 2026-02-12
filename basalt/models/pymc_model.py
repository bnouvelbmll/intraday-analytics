from __future__ import annotations

from dataclasses import dataclass, field
import importlib
import pickle
from pathlib import Path
from typing import Any


def _load_callable(path: str):
    if ":" not in path:
        raise ValueError("Callable path must be module:function")
    module_name, fn_name = path.split(":", 1)
    module = importlib.import_module(module_name)
    fn = getattr(module, fn_name, None)
    if fn is None or not callable(fn):
        raise ValueError(f"Callable not found: {path}")
    return fn


@dataclass
class PyMCModel:
    """
    PyMC adapter with user-provided hooks.

    `build_and_fit_fn` must return a payload containing trained state and
    predict callable/context, or any object accepted by `predict_fn`.
    """

    build_and_fit_fn: str
    predict_fn: str
    fit_params: dict[str, Any] = field(default_factory=dict)
    state: Any = None

    def _require_pymc(self):
        try:
            import pymc  # noqa: F401  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("pymc is not installed. Install it to use PyMCModel.") from exc

    def fit(self, X: Any, y: Any) -> Any:
        self._require_pymc()
        fn = _load_callable(self.build_and_fit_fn)
        self.state = fn(X=X, y=y, **self.fit_params)
        return self.state

    def predict(self, X: Any) -> Any:
        if self.state is None:
            raise ValueError("Model state is empty. Fit or load the model first.")
        fn = _load_callable(self.predict_fn)
        return fn(state=self.state, X=X)

    def save(self, path: str) -> str:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("wb") as fh:
            pickle.dump(
                {
                    "build_and_fit_fn": self.build_and_fit_fn,
                    "predict_fn": self.predict_fn,
                    "fit_params": self.fit_params,
                    "state": self.state,
                },
                fh,
            )
        return str(p)

    @classmethod
    def load(cls, path: str) -> "PyMCModel":
        with Path(path).open("rb") as fh:
            payload = pickle.load(fh)
        return cls(
            build_and_fit_fn=str(payload["build_and_fit_fn"]),
            predict_fn=str(payload["predict_fn"]),
            fit_params=dict(payload.get("fit_params") or {}),
            state=payload.get("state"),
        )

    def to_config(self) -> dict[str, Any]:
        return {
            "type": "pymc",
            "build_and_fit_fn": self.build_and_fit_fn,
            "predict_fn": self.predict_fn,
            "fit_params": dict(self.fit_params),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "PyMCModel":
        return cls(
            build_and_fit_fn=str(config["build_and_fit_fn"]),
            predict_fn=str(config["predict_fn"]),
            fit_params=dict(config.get("fit_params") or {}),
        )
