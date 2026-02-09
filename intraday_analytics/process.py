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
    final_s3_path = get_final_output_path(
        start_date, end_date, config, pass_config.name, output_target
    )

    def _output_type():
        t = output_target.type
        try:
            return t.value
        except Exception:
            return str(t)

    output_type = _output_type()
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
    def _write_output():
        if output_type == "parquet":
            return final_df.sink_parquet(final_s3_path, compression="snappy")
        if output_type == "delta":
            try:
                from deltalake.writer import write_deltalake
            except Exception as exc:
                raise RuntimeError("deltalake is required for delta outputs") from exc
            return write_deltalake(
                final_s3_path,
                final_df.collect(),
                mode=output_target.delta_mode,
                storage_options=config.S3_STORAGE_OPTIONS,
            )
        if output_type == "sql":
            try:
                from sqlalchemy import create_engine
            except Exception as exc:
                raise RuntimeError("sqlalchemy is required for sql outputs") from exc
            if not output_target.sql_connection or not output_target.sql_table:
                raise RuntimeError("sql_connection and sql_table are required for sql outputs")
            engine = create_engine(output_target.sql_connection)
            try:
                with engine.begin() as conn:
                    result = final_df.collect().to_pandas().to_sql(
                        output_target.sql_table,
                        conn,
                        if_exists=output_target.sql_if_exists,
                        index=False,
                    )
                return result
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
