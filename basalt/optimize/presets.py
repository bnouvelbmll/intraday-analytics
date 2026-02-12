from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from basalt.objective_functions import DatasetSplit

_KEY_COLUMNS = {
    "listingid",
    "instrumentid",
    "primarylistingid",
    "timebucket",
    "timestamp",
    "date",
    "mic",
    "opol",
}

_TARGET_CANDIDATES = (
    "FutureReturn",
    "FutureMidReturn",
    "NextMidReturn",
    "Return",
    "Label",
    "Target",
)


def _render_output_path(config: dict[str, Any], pass_name: str) -> str:
    output_target = dict(config.get("OUTPUT_TARGET") or {})
    for pass_cfg in config.get("PASSES") or []:
        if str(pass_cfg.get("name")) == pass_name and isinstance(pass_cfg.get("output"), dict):
            output_target = dict(pass_cfg["output"])
            break
    template = str(output_target.get("path_template") or "")
    if not template:
        raise ValueError("Missing OUTPUT_TARGET.path_template in config.")
    datasetname = f"{config.get('DATASETNAME', 'sample2d')}_{pass_name}"
    start_date = str(config.get("START_DATE") or "")
    end_date = str(config.get("END_DATE") or start_date)
    universe = str(config.get("UNIVERSE") or "all")
    rendered = template.format(
        bucket="",
        prefix="",
        datasetname=datasetname,
        **{"pass": pass_name},
        universe=universe,
        start_date=start_date,
        end_date=end_date,
    )
    return rendered.replace("//", "/")


def _find_target_column(columns: list[str]) -> str:
    by_lower = {c.lower(): c for c in columns}
    for candidate in _TARGET_CANDIDATES:
        hit = by_lower.get(candidate.lower())
        if hit:
            return hit
    for col in columns:
        lower = col.lower()
        if "future" in lower and "return" in lower:
            return col
        if lower.startswith("return"):
            return col
    raise ValueError(
        "Could not infer target column in pass output. "
        "Add a target-like column (FutureReturn/Return/Target/Label)."
    )


def _feature_columns(df: pl.DataFrame, target: str) -> list[str]:
    out: list[str] = []
    for col, dtype in zip(df.columns, df.dtypes):
        if col == target:
            continue
        if col.lower() in _KEY_COLUMNS:
            continue
        if dtype.is_numeric():
            out.append(col)
    if not out:
        raise ValueError("No numeric feature columns available after key/target filtering.")
    return out


def dataset_builder_from_last_pass_output(**kwargs) -> DatasetSplit:
    """
    Build a train/eval split from the latest pass parquet output.

    Expected config keys:
    - START_DATE, END_DATE, DATASETNAME, PASSES, OUTPUT_TARGET.path_template
    """
    config = dict(kwargs.get("config") or {})
    passes = list(config.get("PASSES") or [])
    if not passes:
        raise ValueError("Config must define at least one pass in PASSES.")
    pass_name = str(passes[-1].get("name") or "")
    if not pass_name:
        raise ValueError("Last pass has no name.")
    path = _render_output_path(config, pass_name)
    local = Path(path)
    if not local.exists():
        raise ValueError(f"Expected pass output not found: {path}")
    df = pl.read_parquet(str(local))
    if df.height < 10:
        raise ValueError("Not enough rows in pass output for train/eval split.")
    target = _find_target_column(df.columns)
    features = _feature_columns(df, target)
    if "TimeBucket" in df.columns:
        df = df.sort("TimeBucket")
    elif "Timestamp" in df.columns:
        df = df.sort("Timestamp")
    rows = df.height
    split_idx = max(1, int(rows * 0.7))
    train = df.slice(0, split_idx)
    eval_df = df.slice(split_idx, rows - split_idx)
    x_train = train.select(features).to_numpy().tolist()
    y_train = train.get_column(target).cast(pl.Float64).to_list()
    x_eval = eval_df.select(features).to_numpy().tolist()
    y_eval = eval_df.get_column(target).cast(pl.Float64).to_list()
    return DatasetSplit(
        X_train=x_train,
        y_train=y_train,
        X_eval=x_eval,
        y_eval=y_eval,
        metadata={
            "pass_name": pass_name,
            "target_column": target,
            "feature_count": len(features),
            "output_path": path,
        },
    )


@dataclass
class _SimpleLinearRegressor:
    slope: float = 0.0
    intercept: float = 0.0

    @staticmethod
    def _x0(row: Any) -> float:
        if isinstance(row, (list, tuple)):
            return float(row[0])
        return float(row)

    def fit(self, x, y):
        xs = [self._x0(row) for row in x]
        ys = [float(v) for v in y]
        n = len(xs)
        if n == 0:
            raise ValueError("Cannot fit model on empty input.")
        mean_x = sum(xs) / float(n)
        mean_y = sum(ys) / float(n)
        var_x = sum((v - mean_x) ** 2 for v in xs)
        if var_x == 0:
            self.slope = 0.0
            self.intercept = mean_y
            return
        cov_xy = sum((a - mean_x) * (b - mean_y) for a, b in zip(xs, ys))
        self.slope = cov_xy / var_x
        self.intercept = mean_y - self.slope * mean_x

    def predict(self, x):
        xs = [self._x0(row) for row in x]
        return [self.intercept + self.slope * v for v in xs]


def simple_linear_model_factory():
    """Return a minimal linear regressor compatible with objective evaluation."""
    return _SimpleLinearRegressor()


def autogluon_fast_model_factory():
    """
    Return an AutoGluon Tabular model configured for quick optimization loops.

    Requires the `models` subpackage and `autogluon` installation.
    """
    try:
        from basalt.models import AutoGluonTabularModel
    except Exception as exc:
        raise RuntimeError(
            "AutoGluon factory requires bmll-basalt-models and autogluon."
        ) from exc
    return AutoGluonTabularModel(
        label="target",
        fit_params={
            "presets": "medium_quality_faster_train",
            "verbosity": 1,
        },
    )


def history_guided_generator(
    *,
    trial_id: int,
    search_space: dict[str, Any],
    rng,
    history,
    base_config: dict[str, Any],
) -> dict[str, Any]:
    """
    Simple built-in search generator.

    - First trial: random sample from the search space.
    - Next trials: reuse best-known params and resample one random dimension.
    """
    specs = dict(search_space.get("params") or {})
    if not specs:
        return {}

    def _sample(spec: dict[str, Any]) -> Any:
        kind = str(spec.get("type", "choice")).lower()
        if kind == "choice":
            values = list(spec.get("values") or [])
            if not values:
                raise ValueError("choice param requires non-empty values")
            return rng.choice(values)
        if kind == "int":
            return int(rng.randint(int(spec["low"]), int(spec["high"])))
        if kind == "float":
            return float(rng.uniform(float(spec["low"]), float(spec["high"])))
        if kind == "bool":
            return bool(rng.choice([True, False]))
        return spec

    if not history:
        return {path: _sample(dict(spec)) for path, spec in specs.items()}

    scored = [h for h in history if getattr(h, "status", None) == "ok" and getattr(h, "score", None) is not None]
    if not scored:
        return {path: _sample(dict(spec)) for path, spec in specs.items()}
    best = max(scored, key=lambda h: float(h.score))
    proposal = dict(getattr(best, "params", {}) or {})
    mutate_key = rng.choice(list(specs.keys()))
    proposal[mutate_key] = _sample(dict(specs[mutate_key]))
    return proposal
