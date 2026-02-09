import os
import logging
import threading
import glob
import polars as pl
import pyarrow.parquet as pq

from intraday_analytics.utils import (
    is_s3_path,
    retry_s3,
    normalize_float_lf,
    normalize_float_df,
)
from intraday_analytics.configuration import OutputTarget


def get_final_output_path(start_date, end_date, config, pass_name, output_target=None):
    """Constructs the final output path for a given date range and pass."""
    import bmll2

    dataset_name = config.DATASETNAME
    output_bucket = bmll2.storage_paths()[config.AREA]["bucket"]
    output_prefix = bmll2.storage_paths()[config.AREA]["prefix"]

    # Append pass_name to the datasetname to distinguish pass outputs
    final_dataset_name = f"{dataset_name}_{pass_name}"

    template = config.FINAL_OUTPUT_PATH_TEMPLATE
    if output_target is not None and getattr(output_target, "path_template", None):
        template = output_target.path_template

    universe = config.UNIVERSE or "all"
    def _coerce_date(value):
        try:
            return value.date()
        except Exception:
            return value

    final_s3_path = template.format(
        bucket=output_bucket,
        prefix=output_prefix,
        datasetname=final_dataset_name,
        **{"pass": pass_name},
        universe=universe,
        start_date=_coerce_date(start_date),
        end_date=_coerce_date(end_date),
    )

    if final_s3_path.startswith("s3://"):
        protocol = "s3://"
        path_part = final_s3_path[5:]
        final_s3_path = protocol + path_part.replace("//", "/")
    else:
        final_s3_path = final_s3_path.replace("//", "/")
    return final_s3_path


def get_final_s3_path(start_date, end_date, config, pass_name, output_target=None):
    return get_final_output_path(start_date, end_date, config, pass_name, output_target)


def aggregate_and_write_final_output(
    start_date, end_date, config, pass_config, temp_dir, sort_keys=None
):
    """
    Aggregates all processed batch-metrics files for a single pass into a final
    output file and writes it to the specified S3 location.
    """
    import bmll2

    logging.info(
        f"Aggregating metrics for {start_date} to {end_date} (Pass {pass_config.name})..."
    )

    all_metrics_files = glob.glob(os.path.join(temp_dir, "batch-metrics-*.parquet"))

    if not all_metrics_files:
        logging.warning(
            f"No batch-metrics files found in {temp_dir} to aggregate for Pass {pass_config.name}."
        )
        return

    combined_df = pl.scan_parquet(all_metrics_files)

    if sort_keys is None:
        sort_keys = ["ListingId", "TimeBucket"]

    # Filter sort_keys to only those present in the dataframe
    schema_keys = combined_df.collect_schema().names()
    valid_sort_keys = [k for k in sort_keys if k in schema_keys]

    if valid_sort_keys:
        final_df = combined_df.sort(valid_sort_keys)
    else:
        final_df = combined_df

    final_df = normalize_float_lf(final_df)

    output_target = pass_config.output or config.OUTPUT_TARGET
    if output_target is None:
        raise RuntimeError("No output target configured.")
    if not isinstance(output_target, OutputTarget):
        output_target = OutputTarget.model_validate(output_target)
    def _output_type():
        t = output_target.type
        try:
            return t.value
        except Exception:
            return str(t)

    output_type = _output_type()
    if output_target and getattr(output_target, "path_template", None):
        final_s3_path = get_final_output_path(
            start_date, end_date, config, pass_config.name, output_target
        )
    else:
        final_s3_path = get_final_s3_path(
            start_date, end_date, config, pass_config.name
        )
    logging.info(
        f"Writing aggregated analytics for Pass {pass_config.name} to {final_s3_path}"
    )
    logging.debug(
        "Output target for Pass %s: type=%s sql_table=%s sql_connection=%s",
        pass_config.name,
        output_target.type,
        getattr(output_target, "sql_table", None),
        getattr(output_target, "sql_connection", None),
    )
    def _partition_key_value() -> str:
        universe = config.UNIVERSE or "all"
        try:
            date_part = start_date.date().isoformat()
        except Exception:
            try:
                date_part = str(start_date)
            except Exception:
                date_part = "unknown"
        if "=" in universe:
            universe_value = universe.split("=", 1)[1]
        else:
            universe_value = universe
        return f"{date_part}|{universe_value}"

    def _sql_quote(value: object) -> str:
        text = str(value)
        return text.replace("'", "''")

    def _resolve_partition_columns(schema_keys: list[str]) -> list[str]:
        if output_target.partition_columns:
            return list(output_target.partition_columns)
        if "MIC" in schema_keys and "Date" in schema_keys:
            return ["MIC", "Date"]
        return ["$dagsterpartitionkey"]

    def _write_output():
        schema_keys = final_df.collect_schema().names()
        partition_cols = _resolve_partition_columns(schema_keys)
        final_with_pk = final_df
        if "$dagsterpartitionkey" in partition_cols and "$dagsterpartitionkey" not in schema_keys:
            final_with_pk = final_with_pk.with_columns(
                pl.lit(_partition_key_value()).alias("$dagsterpartitionkey")
            )
            schema_keys = final_with_pk.collect_schema().names()
        missing = [col for col in partition_cols if col not in schema_keys and col != "$dagsterpartitionkey"]
        if missing:
            raise RuntimeError(
                f"Partition columns missing from output: {missing}. "
                "Set OutputTarget.partition_columns to existing columns."
            )
        df_to_write = final_with_pk
        if output_type == "parquet":
            return df_to_write.sink_parquet(final_s3_path, compression="snappy")
        if output_type == "delta":
            try:
                from deltalake.writer import write_deltalake
                from deltalake import DeltaTable
            except Exception as exc:
                raise RuntimeError("deltalake is required for delta outputs") from exc
            if output_target.dedupe_on_partition and partition_cols:
                try:
                    dt = DeltaTable(final_s3_path)
                    partition_values = (
                        df_to_write.select(partition_cols).unique().to_dicts()
                    )
                    for vals in partition_values:
                        predicate = " AND ".join(
                            [f"{k} = '{_sql_quote(vals[k])}'" for k in partition_cols]
                        )
                        dt.delete(predicate)
                except Exception:
                    # If table doesn't exist yet or delete fails, proceed with append.
                    pass
            return write_deltalake(
                final_s3_path,
                df_to_write.collect(),
                mode=output_target.delta_mode,
                storage_options=config.S3_STORAGE_OPTIONS,
                partition_by=partition_cols or None,
            )
        if output_type == "sql":
            try:
                from sqlalchemy import (
                    create_engine,
                    MetaData,
                    Table,
                    Column,
                    Integer,
                    Float,
                    Boolean,
                    Date,
                    DateTime,
                    Text,
                    text as sql_text,
                )
            except Exception as exc:
                raise RuntimeError("sqlalchemy is required for sql outputs") from exc
            if not output_target.sql_connection or not output_target.sql_table:
                raise RuntimeError("sql_connection and sql_table are required for sql outputs")
            engine = create_engine(output_target.sql_connection)
            try:
                df = df_to_write.collect(streaming=True)
                if output_target.dedupe_on_partition and partition_cols and output_target.sql_if_exists != "replace":
                    partition_values = (
                        df_to_write.select(partition_cols).unique().to_dicts()
                    )
                    if partition_values:
                        try:
                            with engine.begin() as conn:
                                for vals in partition_values:
                                    where = " AND ".join([f"{k} = :{k}" for k in partition_cols])
                                    conn.execute(
                                        sql_text(
                                            f"DELETE FROM {output_target.sql_table} WHERE {where}"
                                        ),
                                        vals,
                                    )
                        except Exception:
                            pass
                if output_target.sql_use_pandas:
                    batch = output_target.sql_batch_size or len(df)
                    with engine.begin() as conn:
                        for offset in range(0, len(df), batch):
                            chunk = df.slice(offset, batch)
                            chunk.to_pandas().to_sql(
                                output_target.sql_table,
                                conn,
                                if_exists=output_target.sql_if_exists if offset == 0 else "append",
                                index=False,
                                method="multi",
                            )
                    return None
                # SQLAlchemy core insert path (no pandas)
                metadata = MetaData()
                if output_target.sql_if_exists == "replace":
                    with engine.begin() as conn:
                        conn.execute(sql_text(f"DROP TABLE IF EXISTS {output_target.sql_table}"))
                # Define table schema (basic mapping)
                cols = []
                for name, dtype in zip(df.columns, df.dtypes):
                    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
                        coltype = Integer
                    elif dtype in (pl.Float32, pl.Float64):
                        coltype = Float
                    elif dtype == pl.Boolean:
                        coltype = Boolean
                    elif dtype in (pl.Date,):
                        coltype = Date
                    elif dtype in (pl.Datetime,):
                        coltype = DateTime
                    else:
                        coltype = Text
                    cols.append(Column(name, coltype))
                table = Table(output_target.sql_table, metadata, *cols)
                metadata.create_all(engine)
                batch = output_target.sql_batch_size or len(df)
                with engine.begin() as conn:
                    for chunk in df.iter_slices(batch):
                        conn.execute(table.insert(), chunk.to_dicts())
                return None
            finally:
                engine.dispose()
        raise RuntimeError(f"Unsupported output type: {output_target.type}")

    if output_type == "parquet" and is_s3_path(final_s3_path):
        retry_s3(
            _write_output,
            desc=f"write final output for pass {pass_config.name}",
        )
    else:
        _write_output()

    logging.info(f"Aggregation and final write for Pass {pass_config.name} complete.")

    for f in all_metrics_files:
        try:
            os.remove(f)
        except OSError as e:
            logging.error(f"Error removing temporary aggregated file {f}: {e}")
    return final_s3_path


class BatchWriter:
    """
    A simple writer that appends Polars DataFrames to a single Parquet file.
    """

    def __init__(self, outfile):
        self.out_path = outfile
        self.writer = None
        self.lock = threading.Lock()  # Use a lock for thread-safe writing

    def write(self, df, listing_id=None):
        """
        Writes a Polars DataFrame to the output Parquet file.
        Initializes the ParquetWriter if it's the first write.
        """
        with self.lock:  # Ensure only one thread writes at a time
            df = normalize_float_df(df)
            tbl = df.to_arrow()
            if self.writer is None:
                # Ensure the directory exists
                os.makedirs(os.path.dirname(self.out_path), exist_ok=True)
                self.writer = pq.ParquetWriter(self.out_path, tbl.schema)
                logging.info(f"Opened ParquetWriter for {self.out_path}")
            self.writer.write_table(tbl)

    def close(self):
        """
        Closes the ParquetWriter.
        """
        if self.writer:
            self.writer.close()
            self.writer = None
            logging.debug(f"Closed ParquetWriter for {self.out_path}")
