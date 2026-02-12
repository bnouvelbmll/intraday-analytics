from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Iterable, Protocol


class Objective(Protocol):
    """Interface for objective functions used to score predictions."""

    name: str
    higher_is_better: bool

    def evaluate(self, y_true: Iterable[float], y_pred: Iterable[float]) -> float:
        """Return a numeric score for predictions against ground truth."""


def _as_float_list(values: Iterable[float]) -> list[float]:
    out = [float(v) for v in values]
    if not out:
        raise ValueError("Objective input cannot be empty.")
    return out


def _validate_lengths(y_true: list[float], y_pred: list[float]) -> None:
    if len(y_true) != len(y_pred):
        raise ValueError(
            f"Mismatched prediction lengths: y_true={len(y_true)} y_pred={len(y_pred)}."
        )


@dataclass(frozen=True)
class MeanAbsoluteErrorObjective:
    """Mean absolute error objective (lower is better)."""

    name: str = "mae"
    higher_is_better: bool = False

    def evaluate(self, y_true: Iterable[float], y_pred: Iterable[float]) -> float:
        yt = _as_float_list(y_true)
        yp = _as_float_list(y_pred)
        _validate_lengths(yt, yp)
        return sum(abs(a - b) for a, b in zip(yt, yp)) / float(len(yt))


@dataclass(frozen=True)
class MeanSquaredErrorObjective:
    """Mean squared error objective (lower is better)."""

    name: str = "mse"
    higher_is_better: bool = False
    root: bool = False

    def evaluate(self, y_true: Iterable[float], y_pred: Iterable[float]) -> float:
        yt = _as_float_list(y_true)
        yp = _as_float_list(y_pred)
        _validate_lengths(yt, yp)
        mse = sum((a - b) ** 2 for a, b in zip(yt, yp)) / float(len(yt))
        return sqrt(mse) if self.root else mse


@dataclass(frozen=True)
class DirectionalAccuracyObjective:
    """Directional accuracy for next-move/return sign prediction (higher is better)."""

    name: str = "directional_accuracy"
    higher_is_better: bool = True

    def evaluate(self, y_true: Iterable[float], y_pred: Iterable[float]) -> float:
        yt = _as_float_list(y_true)
        yp = _as_float_list(y_pred)
        _validate_lengths(yt, yp)
        hits = 0
        for a, b in zip(yt, yp):
            sign_a = 1 if a > 0 else (-1 if a < 0 else 0)
            sign_b = 1 if b > 0 else (-1 if b < 0 else 0)
            if sign_a == sign_b:
                hits += 1
        return hits / float(len(yt))


def objectives_from_names(
    names: str | list[str],
    *,
    rmse: bool = False,
) -> list[Objective]:
    """
    Build objective instances from a comma-separated string or list.

    Supported names: `mae`, `mse`, `rmse`, `directional_accuracy`.
    """
    if isinstance(names, str):
        items = [x.strip() for x in names.split(",") if x.strip()]
    else:
        items = [str(x).strip() for x in names if str(x).strip()]
    out: list[Objective] = []
    for name in items:
        key = name.lower()
        if key == "mae":
            out.append(MeanAbsoluteErrorObjective())
        elif key == "mse":
            out.append(MeanSquaredErrorObjective(root=False))
        elif key == "rmse":
            out.append(MeanSquaredErrorObjective(root=True))
        elif key in {"directional_accuracy", "direction", "dir_acc"}:
            out.append(DirectionalAccuracyObjective())
        else:
            raise ValueError(
                f"Unknown objective '{name}'. "
                "Supported: mae,mse,rmse,directional_accuracy"
            )
    if rmse and not any(getattr(obj, "name", "") == "mse" for obj in out):
        out.append(MeanSquaredErrorObjective(root=True))
    if not out:
        raise ValueError("At least one objective name is required.")
    return out
