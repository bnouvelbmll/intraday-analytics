import os
import logging
import threading
import glob
import polars as pl
import pyarrow.parquet as pq

# ... (rest of the file content before aggregate_and_write_final_output) ...

def aggregate_and_write_final_output(start_date, end_date, config, temp_dir):
    """
    Aggregates all processed batch-metrics files into a single final output file
    and writes it to the specified S3 location.
    """
    import bmll2
    logging.info(f"Aggregating metrics for {start_date} to {end_date}...")

    all_metrics_files = glob.glob(os.path.join(temp_dir, "batch-metrics-*.parquet"))

    if not all_metrics_files:
        logging.warning(f"No batch-metrics files found in {temp_dir} to aggregate.")
        return

    # Read all batch-metrics files into a single LazyFrame
    combined_df = pl.scan_parquet(all_metrics_files)

    # Collect and sort the final DataFrame
    final_df = combined_df.collect().sort(["ListingId", "TimeBucket"])

    # Define the final output path
    dataset_name = config["DATASETNAME"]
    output_bucket = bmll2.storage_paths()[config["AREA"]]["bucket"]
    output_prefix = bmll2.storage_paths()[config["AREA"]]["prefix"]

    # Construct the final S3 path using the template from config
    # Format dates to ensure only the date part is used in the filename
    final_s3_path = config["FINAL_OUTPUT_PATH_TEMPLATE"].format(
        bucket=output_bucket,
        prefix=output_prefix,
        datasetname=dataset_name,
        start_date=start_date.date(),
        end_date=end_date.date()
    )

    logging.info(f"Writing aggregated analytics to {final_s3_path}")
    final_df.write_parquet(final_s3_path, compression="snappy")
    logging.info("Aggregation and final write complete.")

    # Clean up the temporary batch-metrics files
    for f in all_metrics_files:
        try:
            os.remove(f)
            logging.debug(f"Removed temporary aggregated file: {f}")
        except OSError as e:
            logging.error(f"Error removing temporary aggregated file {f}: {e}")


class BatchWriter:
    """
    A simple writer that appends Polars DataFrames to a single Parquet file.
    """

    def __init__(self, outfile):
        self.out_path = outfile
        self.writer = None
        self.lock = threading.Lock() # Use a lock for thread-safe writing

    def write(self, df, listing_id=None):
        """
        Writes a Polars DataFrame to the output Parquet file.
        Initializes the ParquetWriter if it's the first write.
        """
        with self.lock: # Ensure only one thread writes at a time
            tbl = df.to_arrow()
            if self.writer is None:
                # Ensure the directory exists
                os.makedirs(os.path.dirname(self.out_path), exist_ok=True)
                self.writer = pq.ParquetWriter(self.out_path, tbl.schema)
            self.writer.write_table(tbl)

    def close(self):
        """Closes the ParquetWriter."""
        if self.writer:
            self.writer.close()
            self.writer = None
            logging.debug(f"Closed ParquetWriter for {self.out_path}")