import os
from multiprocessing import Process
import logging
from joblib import Parallel, delayed
import glob
import polars as pl

from .batching import SymbolBatcherStreaming, S3SymbolBatcher, HeuristicBatchingStrategy, SymbolSizeEstimator
from .pipeline import AnalyticsRunner
from .utils import preload, get_files_for_date_range
from .tables import ALL_TABLES
from .process import aggregate_and_write_final_output, BatchWriter

def remote_process_executor_wrapper(func):
    """
    A decorator that executes a function in a separate, isolated subprocess using joblib.

    This approach is useful for running code that might have memory leaks or other
    side effects, as joblib with `max_tasks_per_child=1` ensures a fresh process
    for each task, effectively providing a "clean worker".
    """
    def wrapper(*args, **kwargs):
        # max_tasks_per_child=1 ensures a new process is spawned for each task
        results = Parallel(max_tasks_per_child=1, prefer="processes")(
            delayed(func)(*args, **kwargs)
        )
        return results[0]
    return wrapper


try:
    import viztracer
    from viztracer.vizlogging import VizLoggingHandler
    from viztracer import get_tracer
except ImportError:
    viztracer = None


from .tables import ALL_TABLES


class ProcessInterval(Process):
    """
    A multiprocessing.Process subclass for preparing data for the analytics pipeline.

    This class encapsulates the logic for loading, batching, and processing data
    in a separate process for a given time interval.
    """

    def __init__(self, sd, ed, config, get_pipeline, get_universe):
        super().__init__()
        self.sd = sd
        self.ed = ed
        self.config = config
        self.get_pipeline = get_pipeline
        self.get_universe = get_universe

    def run(self):
        # Configure logging for the new process
        logging.basicConfig(
            level=self.config.get("LOGGING_LEVEL", "INFO").upper(),
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        )

        tracer = None
        if self.config.get("ENABLE_PROFILER_TOOL", False):
            try:
                profiling_output_dir = self.config.get(
                    "PROFILING_OUTPUT_DIR", "/tmp/perf_traces"
                )
                tracer = viztracer.VizTracer(output_dir=profiling_output_dir)
                tracer.start()
                handler = VizLoggingHandler()
                handler.setTracer(get_tracer())
                logging.getLogger().addHandler(handler)
                logging.info("📊 VizTracer started in child process.")
            except Exception as e:
                logging.error(f"Failed to start VizTracer in child process: {e}")
                tracer = None

        try:
            ref = self.get_universe(self.sd)
            nanoseconds = int(self.config["TIME_BUCKET_SECONDS"] * 1e9)
            pipe = self.get_pipeline(self.config["L2_LEVELS"], ref=ref, date=self.sd)

            MODE = self.config["PREPARE_DATA_MODE"]
            TEMP_DIR = self.config["TEMP_DIR"]

            if MODE == "naive":
                logging.info("🚚 Creating batch files...")

                tables_to_load_names = self.config.get(
                    "TABLES_TO_LOAD", ["trades", "l2", "l3"]
                )
                table_definitions = [ALL_TABLES[name] for name in tables_to_load_names]
                loaded_tables = preload(
                    self.sd, self.ed, ref, nanoseconds, table_definitions
                )

                def write_per_listing(df, listing_id):
                    out_path = os.path.join(
                        TEMP_DIR, f"out-listing-{listing_id}.parquet"
                    )
                    df.write_parquet(out_path)

                writer = write_per_listing

                sbs_input = {}
                for table in table_definitions:
                    sbs_input[table.name] = loaded_tables[table.name].sort(
                        ["ListingId", table.timestamp_col]
                    )

                sbs = SymbolBatcherStreaming(
                    sbs_input,
                    max_rows_per_table=self.config["MAX_ROWS_PER_TABLE"],
                )
                pid = AnalyticsRunner(pipe, writer, self.config)

                def process_batch(b):
                    pid.run_batch(b)

                logging.info("Writing batches to disk...")
                for i, batch in enumerate(sbs.stream_batches()):
                    logging.info(
                        f"  - Writing batch {i} (l2 length: {len(batch.get('l2', []))})"
                    )
                    for table_name, data in batch.items():
                        data.write_parquet(
                            os.path.join(TEMP_DIR, f"batch-{table_name}-{i}.parquet")
                        )

            elif MODE == "s3_shredding":
                logging.info("🚚 Starting S3 Shredding...")

                # 1. Identify tables to load
                tables_to_load_names = self.config.get(
                    "TABLES_TO_LOAD", ["trades", "l2", "l3"]
                )

                # 2. Generate S3 file lists
                s3_file_lists = {}
                mics = ref["MIC"].unique().tolist()
                for table_name in tables_to_load_names:
                    s3_file_lists[table_name] = get_files_for_date_range(
                        self.sd, self.ed, mics, table_name
                    )

                # 3. Initialize S3SymbolBatcher
                sbs = S3SymbolBatcher(
                    s3_file_lists=s3_file_lists,
                    transform_fns={},
                    batching_strategy=HeuristicBatchingStrategy(
                        SymbolSizeEstimator(self.sd, self.get_universe),
                        self.config["MAX_ROWS_PER_TABLE"],
                    ),
                    temp_dir=TEMP_DIR,
                    storage_options=self.config.get("S3_STORAGE_OPTIONS", {}),
                    date=self.sd,
                    get_universe=self.get_universe,
                )

                # 4. Run the shredding process
                sbs.process(num_workers=self.config["NUM_WORKERS"])

        finally:
            if tracer:
                tracer.stop()
                logging.info("📊 VizTracer stopped in child process.")


def compute_metrics(config, get_pipeline, get_universe):
    """
    Computes metrics for all batches in the temporary directory and aggregates them.
    """
    temp_dir = config["TEMP_DIR"]
    logging.info(f"Computing metrics in {temp_dir}...")

    # Find all batch indices by looking for one of the tables (e.g., trades)
    # Pattern: batch-trades-{i}.parquet
    batch_files = glob.glob(os.path.join(temp_dir, "batch-trades-*.parquet"))
    batch_indices = sorted([int(f.split("-")[-1].split(".")[0]) for f in batch_files])

    if not batch_indices:
        logging.warning("No batches found to process.")
        return

    # We need a reference date for the pipeline.
    start_date = config["START_DATE"]
    ref = get_universe(start_date)

    pipe = get_pipeline(config["L2_LEVELS"], ref=ref, date=start_date)

    for i in batch_indices:
        logging.info(f"Processing batch {i}...")

        # Load batch data
        batch_data = {}
        for table_name in config.get("TABLES_TO_LOAD", ["trades", "l2", "l3"]):
            path = os.path.join(temp_dir, f"batch-{table_name}-{i}.parquet")
            if os.path.exists(path):
                batch_data[table_name] = pl.read_parquet(path)
            else:
                batch_data[table_name] = pl.DataFrame()

        # Run pipeline
        # We need an output writer for the batch results
        out_path = os.path.join(temp_dir, f"batch-metrics-{i}.parquet")
        writer = BatchWriter(out_path)

        runner = AnalyticsRunner(pipe, writer.write, config)
        runner.run_batch(batch_data)
        writer.close()

    # Aggregate
    aggregate_and_write_final_output(
        config["START_DATE"], config["END_DATE"], config, temp_dir
    )