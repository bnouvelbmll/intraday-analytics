import os
from multiprocessing import Process
import logging
from joblib import Parallel, delayed
import glob
import polars as pl
import pandas as pd
import shutil

from .batching import SymbolBatcherStreaming, S3SymbolBatcher, HeuristicBatchingStrategy, SymbolSizeEstimator
from .pipeline import AnalyticsRunner
from .utils import preload, get_files_for_date_range, create_date_batches
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
            level=self.config.LOGGING_LEVEL.upper(),
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        )

        try:
            # We process day by day to save memory and ensure metric safety
            date_range = pd.date_range(self.sd, self.ed, freq='D')
            TEMP_DIR = self.config.TEMP_DIR
            MODE = self.config.PREPARE_DATA_MODE
            
            # Use the partition start date for universe/batching consistency across days
            ref_partition = self.get_universe(self.sd)
            
            for current_date in date_range:
                logging.info(f"Processing date: {current_date.date()}")
                
                ref = self.get_universe(current_date)
                nanoseconds = int(self.config.TIME_BUCKET_SECONDS * 1e9)
                pipe = self.get_pipeline(ref=ref, date=current_date)

                if MODE == "naive":
                    logging.info("🚚 Creating batch files (Naive)...")

                    tables_to_load_names = self.config.TABLES_TO_LOAD
                    table_definitions = [ALL_TABLES[name] for name in tables_to_load_names]
                    
                    # Preload only for current_date
                    loaded_tables = preload(
                        current_date, current_date, ref, nanoseconds, table_definitions
                    )

                    sbs_input = {}
                    for table in table_definitions:
                        sbs_input[table.name] = loaded_tables[table.name].sort(
                            ["ListingId", table.timestamp_col]
                        )

                    sbs = SymbolBatcherStreaming(
                        sbs_input,
                        max_rows_per_table=self.config.MAX_ROWS_PER_TABLE,
                    )

                    logging.info("Writing batches to disk...")
                    for i, batch in enumerate(sbs.stream_batches()):
                        # Write batch files
                        for table_name, data in batch.items():
                            data.write_parquet(
                                os.path.join(TEMP_DIR, f"batch-{table_name}-{i}.parquet")
                            )

                elif MODE == "s3_shredding":
                    logging.info("🚚 Starting S3 Shredding...")

                    tables_to_load_names = self.config.TABLES_TO_LOAD

                    s3_file_lists = {}
                    mics = ref["MIC"].unique().to_list()
                    exclude_weekends = self.config.EXCLUDE_WEEKENDS
                    for table_name in tables_to_load_names:
                        s3_file_lists[table_name] = get_files_for_date_range(
                            current_date, current_date, mics, table_name, exclude_weekends=exclude_weekends
                        )

                    table_definitions = [ALL_TABLES[name] for name in tables_to_load_names]
                    transform_fns = {
                        table.name: table.get_transform_fn(ref, nanoseconds)
                        for table in table_definitions
                    }
                    
                    sbs = S3SymbolBatcher(
                        s3_file_lists=s3_file_lists,
                        transform_fns=transform_fns,
                        batching_strategy=HeuristicBatchingStrategy(
                            SymbolSizeEstimator(self.sd, self.get_universe),
                            self.config.MAX_ROWS_PER_TABLE,
                        ),
                        temp_dir=TEMP_DIR,
                        storage_options=self.config.S3_STORAGE_OPTIONS,
                        date=current_date, 
                        get_universe=self.get_universe,
                        memory_per_worker=self.config.MEMORY_PER_WORKER,
                    )

                    sbs.process(num_workers=self.config.NUM_WORKERS)

                else:
                    logging.error(f"Unknown PREPARE_DATA_MODE: {MODE}")
                    raise ValueError(f"Unknown PREPARE_DATA_MODE: {MODE}")

                # --- Compute Metrics for current_date ---
                logging.info(f"📊 Computing metrics for date: {current_date.date()}")
                
                # Find batches
                batch_files = glob.glob(os.path.join(TEMP_DIR, "batch-trades-*.parquet"))
                batch_indices = sorted([int(f.split("-")[-1].split(".")[0]) for f in batch_files])
                
                for i in batch_indices:
                    # Load batch data
                    batch_data = {}
                    for table_name in self.config.TABLES_TO_LOAD:
                        path = os.path.join(TEMP_DIR, f"batch-{table_name}-{i}.parquet")
                        if os.path.exists(path):
                            batch_data[table_name] = pl.read_parquet(path)
                        else:
                            batch_data[table_name] = pl.DataFrame()

                    # Run pipeline
                    # Write to a date-specific file
                    out_path = os.path.join(TEMP_DIR, f"batch-metrics-{i}-{current_date.date()}.parquet")
                    writer = BatchWriter(out_path)

                    runner = AnalyticsRunner(pipe, writer.write, self.config)
                    runner.run_batch(batch_data)
                    writer.close()
                    
                    # Clean up batch input files immediately to save space
                    for table_name in self.config.TABLES_TO_LOAD:
                        path = os.path.join(TEMP_DIR, f"batch-{table_name}-{i}.parquet")
                        if os.path.exists(path):
                            os.remove(path)

            # --- End of Date Loop ---
            
            # Aggregate all daily metrics
            aggregate_and_write_final_output(
                self.sd, self.ed, self.config, TEMP_DIR
            )

        except Exception as e:
            logging.error(f"Critical error in ProcessInterval: {e}", exc_info=True)
            raise

        finally:
            pass


def run_metrics_pipeline(config, get_pipeline, get_universe):
    """
    Runs the full intraday analytics pipeline:
    1. Creates date batches.
    2. For each batch:
       a. Runs ProcessInterval (shredding + computation + aggregation).
    """
    import shutil
    
    # Configure logging
    logging.basicConfig(
        level=config.LOGGING_LEVEL.upper(),
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,
    )

    date_batches = create_date_batches(
        config.START_DATE, config.END_DATE, config.DEFAULT_FREQ
    )
    logging.info(f"📅 Created {len(date_batches)} date batches.")

    temp_dir = config.TEMP_DIR
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)

    try:
        for sd, ed in date_batches:
            logging.info(f"🚀 Starting batch for dates: {sd.date()} -> {ed.date()}")
            p = ProcessInterval(
                sd=sd,
                ed=ed,
                config=config,
                get_pipeline=get_pipeline,
                get_universe=get_universe,
            )
            p.start()
            p.join()
            
            if p.exitcode != 0:
                logging.error(f"ProcessInterval failed with exit code {p.exitcode}")
                raise RuntimeError("ProcessInterval failed")

        logging.info("✅ All data preparation and metric computation processes completed.")

    finally:
        if config.CLEAN_UP_TEMP_DIR and os.path.exists(temp_dir):
            logging.info(f"🧹 Cleaning up temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir)
