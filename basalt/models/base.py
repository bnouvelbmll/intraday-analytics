from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
import json


class BasePredictiveModel(Protocol):
    """Protocol for trainable predictive models used by objective evaluation."""

    def fit(self, X: Any, y: Any) -> Any:
        """Train the model with feature matrix `X` and target `y`."""

    def predict(self, X: Any) -> Any:
        """Return predictions for feature matrix `X`."""

    def save(self, path: str) -> str:
        """Persist model state and return the saved artifact path."""

    @classmethod
    def load(cls, path: str) -> "BasePredictiveModel":
        """Load model state from path and return a model instance."""

    def to_config(self) -> dict[str, Any]:
        """Return a serializable model configuration dictionary."""

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "BasePredictiveModel":
        """Create a model instance from configuration payload."""


@dataclass
class ModelArtifact:
    """Metadata for a persisted model artifact."""

    model_type: str
    artifact_path: str
    config: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def write_manifest(self, path: str) -> str:
        manifest = Path(path)
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(
            json.dumps(
                {
                    "model_type": self.model_type,
                    "artifact_path": self.artifact_path,
                    "config": self.config,
                    "metadata": self.metadata,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return str(manifest)
