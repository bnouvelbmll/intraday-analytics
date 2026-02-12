from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class AutoGluonTabularModel:
    """
    AutoGluon TabularPredictor adapter.

    This wrapper uses lazy imports so the models package can remain optional.
    """

    label: str = "target"
    predictor_path: str | None = None
    fit_params: dict[str, Any] = field(default_factory=dict)
    predictor: Any | None = None

    def _require_autogluon(self):
        try:
            from autogluon.tabular import TabularPredictor  # type: ignore
        except Exception as exc:  # pragma: no cover - depends on optional package
            raise RuntimeError(
                "autogluon is not installed. Install it to use AutoGluonTabularModel."
            ) from exc
        return TabularPredictor

    def fit(self, X: Any, y: Any) -> Any:
        TabularPredictor = self._require_autogluon()
        import pandas as pd

        if not hasattr(X, "columns"):
            X = pd.DataFrame(X)
        train_df = X.copy()
        train_df[self.label] = list(y)
        save_path = self.predictor_path or str(Path("autogluon_model").resolve())
        self.predictor = TabularPredictor(label=self.label, path=save_path).fit(
            train_df, **self.fit_params
        )
        self.predictor_path = save_path
        return self.predictor

    def predict(self, X: Any) -> Any:
        if self.predictor is None:
            if not self.predictor_path:
                raise ValueError("Model is not fit and predictor_path is not set.")
            TabularPredictor = self._require_autogluon()
            self.predictor = TabularPredictor.load(self.predictor_path)
        import pandas as pd

        if not hasattr(X, "columns"):
            X = pd.DataFrame(X)
        return self.predictor.predict(X)

    def save(self, path: str) -> str:
        # AutoGluon persists into its own folder; here we only persist a lightweight pointer.
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.predictor_path or "", encoding="utf-8")
        return str(p)

    @classmethod
    def load(cls, path: str) -> "AutoGluonTabularModel":
        predictor_path = Path(path).read_text(encoding="utf-8").strip()
        return cls(predictor_path=predictor_path or None)

    def to_config(self) -> dict[str, Any]:
        return {
            "type": "autogluon_tabular",
            "label": self.label,
            "predictor_path": self.predictor_path,
            "fit_params": dict(self.fit_params),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "AutoGluonTabularModel":
        return cls(
            label=str(config.get("label", "target")),
            predictor_path=config.get("predictor_path"),
            fit_params=dict(config.get("fit_params") or {}),
        )
