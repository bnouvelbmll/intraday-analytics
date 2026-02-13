from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import importlib
import importlib.util

import pandas as pd
import polars as pl

from basalt.cli import resolve_user_config
from basalt.configuration import AnalyticsConfig
from basalt.orchestrator import _derive_tables_to_load
from basalt.tables import ALL_TABLES


@dataclass(frozen=True)
class DatasetOption:
    pass_name: str
    path: str
    is_default: bool
    kind: str = "output"


@dataclass(frozen=True)
class InputTableOption:
    table_name: str
    from_passes: tuple[str, ...]


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


def derive_input_table_options(config: dict[str, Any]) -> list[InputTableOption]:
    model = AnalyticsConfig(**config)
    out: dict[str, list[str]] = {}
    known_context: list[str] = []
    for pass_cfg in model.PASSES:
        tables = _derive_tables_to_load(
            pass_cfg,
            model.TABLES_TO_LOAD,
            known_context_sources=known_context,
        )
        known_context.append(pass_cfg.name)
        for table in tables:
            out.setdefault(table, []).append(pass_cfg.name)
    # include user-requested tables, even if unused by modules in current pass graph.
    for table in model.TABLES_TO_LOAD or []:
        out.setdefault(str(table), [])
    rows = [
        InputTableOption(table_name=name, from_passes=tuple(sorted(set(passes))))
        for name, passes in sorted(out.items())
        if name in ALL_TABLES
    ]
    return rows


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


def load_input_table_frame(
    *,
    pipeline: str,
    config: dict[str, Any],
    table_name: str,
    date_start: str,
    date_end: str,
    source: str = "bmll",
) -> pl.DataFrame:
    if table_name not in ALL_TABLES:
        raise ValueError(f"Unknown input table: {table_name}")
    module = _load_pipeline_module(pipeline)
    if not hasattr(module, "get_universe"):
        raise ValueError("Pipeline module must define get_universe(date).")

    ref = module.get_universe(str(date_start))
    if isinstance(ref, pl.DataFrame):
        ref_df = ref
    else:
        ref_df = pl.DataFrame(ref)
    if ref_df.is_empty():
        raise ValueError("Universe is empty for selected date.")

    if "MIC" in ref_df.columns:
        markets = [m for m in ref_df["MIC"].drop_nulls().unique().to_list() if m is not None]
    elif "OPOL" in ref_df.columns:
        markets = [m for m in ref_df["OPOL"].drop_nulls().unique().to_list() if m is not None]
    else:
        raise ValueError("Universe must contain MIC or OPOL to load BMLL input tables.")

    passes = list((config or {}).get("PASSES") or [])
    first_pass = passes[0] if passes else {}
    bucket_seconds = float(first_pass.get("time_bucket_seconds", 1.0) or 1.0)
    nanoseconds = int(bucket_seconds * 1e9)
    anchor = str(first_pass.get("time_bucket_anchor", "end"))
    closed = str(first_pass.get("time_bucket_closed", "right"))

    table = ALL_TABLES[table_name]
    lf = table.load_from_source(
        source=source,
        markets=markets,
        start_date=str(date_start),
        end_date=str(date_end),
        ref=ref_df,
        nanoseconds=nanoseconds,
        time_bucket_anchor=anchor,
        time_bucket_closed=closed,
    )
    if isinstance(lf, pl.DataFrame):
        return lf
    return lf.collect()


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
