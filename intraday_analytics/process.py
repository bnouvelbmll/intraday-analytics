import os
import logging
import threading
import glob
import polars as pl
import pyarrow.parquet as pq

from intraday_analytics.utils import is_s3_path, retry_s3


def get_final_s3_path(start_date, end_date, config, pass_name):
    """Constructs the final S3 output path for a given date range and pass."""
    import bmll2

    dataset_name = config.DATASETNAME
    output_bucket = bmll2.storage_paths()[config.AREA]["bucket"]
    output_prefix = bmll2.storage_paths()[config.AREA]["prefix"]

    # Append pass_name to the datasetname to distinguish pass outputs
    final_dataset_name = f"{dataset_name}_{pass_name}"

    final_s3_path = config.FINAL_OUTPUT_PATH_TEMPLATE.format(
        bucket=output_bucket,
        prefix=output_prefix,
        datasetname=final_dataset_name,
        start_date=start_date.date(),
        end_date=end_date.date(),
    )

    if final_s3_path.startswith("s3://"):
        protocol = "s3://"
        path_part = final_s3_path[5:]
        final_s3_path = protocol + path_part.replace("//", "/")
    else:
        final_s3_path = final_s3_path.replace("//", "/")
    return final_s3_path


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

    final_s3_path = get_final_s3_path(start_date, end_date, config, pass_config.name)

    logging.info(
        f"Writing aggregated analytics for Pass {pass_config.name} to {final_s3_path}"
    )
    def _write_output():
        return final_df.sink_parquet(final_s3_path, compression="snappy")

    if is_s3_path(final_s3_path):
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
