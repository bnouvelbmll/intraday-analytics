from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
from dagster import IOManager, io_manager

from intraday_analytics.configuration import AnalyticsConfig, OutputTarget
from intraday_analytics.dagster_compat import (
    _safe_partition_keys,
    parse_date_key,
    parse_universe_spec,
)
from intraday_analytics.process import get_final_output_path


class OutputTargetIOManager(IOManager):
    def __init__(
        self,
        config: AnalyticsConfig,
        output_target: OutputTarget,
        universe_dim: str = "universe",
        date_dim: str = "date",
    ):
        self.config = config
        self.output_target = output_target
        self.universe_dim = universe_dim
        self.date_dim = date_dim

    def _resolve_partition(self, context) -> tuple[str, str]:
        keys = _safe_partition_keys(context) or {}
        date_key = keys.get(self.date_dim)
        if date_key:
            dates = parse_date_key(date_key)
        else:
            dates = parse_date_key(self.config.START_DATE)
        universe_key = keys.get(self.universe_dim)
        if universe_key:
            universe = parse_universe_spec(universe_key)
            self.config.UNIVERSE = universe.value
        return dates.start_date, dates.end_date

    def handle_output(self, context, obj: Any) -> None:
        if obj is None:
            return
        if isinstance(obj, (str, Path)):
            context.add_output_metadata({"path": str(obj)})
            return

        try:
            import pandas as pd
        except Exception:
            pd = None

        start_date, end_date = self._resolve_partition(context)
        path = get_final_output_path(
            start_date,
            end_date,
            self.config,
            context.asset_key.path[-1],
            self.output_target,
        )

        if isinstance(obj, pl.LazyFrame):
            df = obj.collect()
        elif isinstance(obj, pl.DataFrame):
            df = obj
        elif pd is not None and isinstance(obj, pd.DataFrame):
            df = obj
        else:
            context.add_output_metadata({"value": str(obj)})
            return

        output_type = self.output_target.type.value
        if output_type == "parquet":
            if pd is not None and isinstance(df, pd.DataFrame):
                df.to_parquet(path, index=self.output_target.preserve_index)
            else:
                df.write_parquet(path)
        elif output_type == "delta":
            try:
                from deltalake import write_deltalake
            except Exception as exc:
                raise RuntimeError("deltalake is required for delta outputs") from exc
            mode = self.output_target.delta_mode or "append"
            if pd is not None and isinstance(df, pd.DataFrame):
                if self.output_target.preserve_index:
                    df = df.reset_index()
                write_deltalake(path, df, mode=mode)
            else:
                write_deltalake(path, df.to_arrow(), mode=mode)
        elif output_type == "sql":
            try:
                from sqlalchemy import create_engine
            except Exception as exc:
                raise RuntimeError("sqlalchemy + pandas are required for sql outputs") from exc
            if not self.output_target.sql_connection or not self.output_target.sql_table:
                raise RuntimeError("sql_connection and sql_table are required for sql outputs")
            engine = create_engine(self.output_target.sql_connection)
            table = self.output_target.sql_table
            if pd is not None and isinstance(df, pd.DataFrame):
                pdf = df
            else:
                pdf = df.to_pandas()
            if self.output_target.preserve_index:
                if self.output_target.index_name and not all(pdf.index.names):
                    if pdf.index.nlevels == 1:
                        pdf.index.name = self.output_target.index_name
                    else:
                        pdf.index.names = [
                            self.output_target.index_name if not n else n
                            for n in pdf.index.names
                        ]
            pdf.to_sql(
                table,
                engine,
                if_exists=self.output_target.sql_if_exists,
                index=self.output_target.preserve_index,
            )
        else:
            raise RuntimeError(f"Unsupported output type: {output_type}")

        context.add_output_metadata({"path": path})

    def load_input(self, context):
        upstream = context.upstream_output
        path = upstream.metadata.get("path") if upstream else None
        if not path and upstream and isinstance(upstream.value, (str, Path)):
            path = str(upstream.value)
        if not path:
            return None
        output_type = self.output_target.type.value
        if output_type == "sql":
            try:
                import pandas as pd
                from sqlalchemy import create_engine
            except Exception as exc:
                raise RuntimeError("sqlalchemy + pandas are required for sql outputs") from exc
            engine = create_engine(self.output_target.sql_connection)
            return pd.read_sql_table(self.output_target.sql_table, engine)
        if output_type == "delta":
            try:
                from deltalake import DeltaTable
            except Exception as exc:
                raise RuntimeError("deltalake is required for delta outputs") from exc
            return pl.from_arrow(DeltaTable(path).to_pyarrow_table())
        return pl.read_parquet(path)


@io_manager(
    config_schema={
        "base_config": dict,
        "output_target": dict,
        "universe_dim": str,
        "date_dim": str,
    }
)
def output_target_io_manager(init_context):
    cfg_data = init_context.resource_config.get("base_config") or {}
    output_target_data = init_context.resource_config.get("output_target")
    config = AnalyticsConfig(**cfg_data) if cfg_data else AnalyticsConfig()
    output_target = (
        OutputTarget.model_validate(output_target_data)
        if output_target_data
        else config.OUTPUT_TARGET
    )
    universe_dim = init_context.resource_config.get("universe_dim", "universe")
    date_dim = init_context.resource_config.get("date_dim", "date")
    return OutputTargetIOManager(config, output_target, universe_dim, date_dim)
