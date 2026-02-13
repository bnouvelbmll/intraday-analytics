from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import importlib
import importlib.util

import pandas as pd
import polars as pl

from basalt.cli import resolve_user_config


@dataclass(frozen=True)
class DatasetOption:
    pass_name: str
    path: str
    is_default: bool


def _load_pipeline_module(path_or_name: str):
    path = Path(path_or_name)
    if path.exists():
        spec = importlib.util.spec_from_file_location(path.stem, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Unable to load pipeline: {path_or_name}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[attr-defined]
        return module
    return importlib.import_module(path_or_name)


def load_resolved_user_config(
    *,
    pipeline: str,
    config_precedence: str = "yaml_overrides",
) -> dict[str, Any]:
    module = _load_pipeline_module(pipeline)
    if not hasattr(module, "USER_CONFIG"):
        raise ValueError("Pipeline module must define USER_CONFIG.")
    config_file = getattr(module, "__file__", None) or pipeline
    return resolve_user_config(
        dict(module.USER_CONFIG),
        module_file=config_file,
        precedence=config_precedence,
    )


def _render_path_template(
    *,
    output_target: dict[str, Any],
    datasetname: str,
    pass_name: str,
    universe: str,
    start_date: str,
    end_date: str,
) -> str:
    template = str(output_target.get("path_template") or "")
    if not template:
        return ""
    path = template.format(
        bucket="",
        prefix="",
        datasetname=datasetname,
        **{"pass": pass_name},
        universe=universe,
        start_date=start_date,
        end_date=end_date,
    )
    return path.replace("//", "/")


def infer_dataset_options(config: dict[str, Any]) -> list[DatasetOption]:
    passes = list(config.get("PASSES") or [])
    if not passes:
        return []
    universe = str(config.get("UNIVERSE") or "all")
    start_date = str(config.get("START_DATE") or "")
    end_date = str(config.get("END_DATE") or start_date)
    base_target = dict(config.get("OUTPUT_TARGET") or {})
    datasetname_root = str(config.get("DATASETNAME") or "sample2d")
    out: list[DatasetOption] = []
    for idx, pass_cfg in enumerate(passes):
        pass_name = str(pass_cfg.get("name") or f"pass_{idx + 1}")
        target = dict(pass_cfg.get("output") or base_target)
        path = _render_path_template(
            output_target=target,
            datasetname=f"{datasetname_root}_{pass_name}",
            pass_name=pass_name,
            universe=universe,
            start_date=start_date,
            end_date=end_date,
        )
        out.append(
            DatasetOption(
                pass_name=pass_name,
                path=path,
                is_default=(idx == len(passes) - 1),
            )
        )
    return out


def _pick_time_column(df: pl.DataFrame) -> str | None:
    candidates = [
        "TimeBucket",
        "Timestamp",
        "DateTime",
        "Date",
        "EventTime",
    ]
    by_lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        hit = by_lower.get(c.lower())
        if hit:
            return hit
    return None


def _pick_instrument_column(df: pl.DataFrame) -> str | None:
    candidates = [
        "ListingId",
        "InstrumentId",
        "PrimaryListingId",
        "BMLL_OBJECT_ID",
        "Ticker",
        "Symbol",
    ]
    by_lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        hit = by_lower.get(c.lower())
        if hit:
            return hit
    return None


def load_dataset_frame(path: str) -> pl.DataFrame:
    if path.startswith("s3://"):
        return pl.scan_parquet(path).collect()
    local = Path(path)
    if local.exists():
        return pl.read_parquet(local)
    # Best-effort glob support for partitioned outputs.
    matches = sorted(Path(".").glob(path))
    if not matches:
        raise FileNotFoundError(f"Dataset path not found: {path}")
    return pl.scan_parquet([str(p) for p in matches]).collect()


def filter_frame(
    df: pl.DataFrame,
    *,
    date_start: str | None = None,
    date_end: str | None = None,
    instrument_value: str | None = None,
) -> tuple[pl.DataFrame, str | None, str | None]:
    time_col = _pick_time_column(df)
    instrument_col = _pick_instrument_column(df)
    out = df
    if time_col and (date_start or date_end):
        ts = pl.col(time_col)
        out = out.with_columns(ts.cast(pl.Datetime(time_unit="us")).alias("__time"))
        if date_start:
            start = pd.Timestamp(date_start)
            out = out.filter(pl.col("__time") >= start)
        if date_end:
            end = pd.Timestamp(date_end) + pd.Timedelta(days=1)
            out = out.filter(pl.col("__time") < end)
        out = out.drop("__time")
    if instrument_col and instrument_value not in (None, ""):
        out = out.filter(pl.col(instrument_col).cast(pl.String) == str(instrument_value))
    return out, time_col, instrument_col
