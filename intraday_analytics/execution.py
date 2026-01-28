import cloudpickle
import sys
import os
import traceback
import subprocess
import tempfile
import random
import time
from multiprocessing import Process
import logging
from joblib import Parallel, delayed

from .batching import SymbolBatcherStreaming, PipelineDispatcher
from .utils import preload
from ..config import DEFAULT_CONFIG as CONFIG  # Added import

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


import viztracer
from viztracer import VizLoggingHandler, get_tracer

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
                pid = PipelineDispatcher(pipe, writer, self.config)

                def process_batch(b):
                    import resource

                    # Limit memory to 16gb per batch
                    # resource.setrlimit(resource.RLIMIT_AS, (16 * 1024**3, 16 * 1024**3))
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
        finally:
            if tracer:
                tracer.stop()
                logging.info("📊 VizTracer stopped in child process.")
