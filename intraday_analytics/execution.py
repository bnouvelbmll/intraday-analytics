import os
from multiprocessing import Process, get_context
import logging
from joblib import Parallel, delayed
import glob
import polars as pl
import pandas as pd
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed

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


def process_batch_task(i, temp_dir, current_date, config, pipe):
    """
    Worker function to process a single batch in a separate process.
    """
    try:
        with open("/tmp/debug_worker.txt", "a") as f:
            f.write(f"Worker {os.getpid()} processing batch {i} for {current_date}\n")
            f.write(f"Config TABLES_TO_LOAD: {config.TABLES_TO_LOAD}\n")
            f.write(f"Temp dir: {temp_dir}\n")

        # Load batch data
        batch_data = {}
        for table_name in config.TABLES_TO_LOAD:
            path = os.path.join(temp_dir, f"batch-{table_name}-{i}.parquet")
            if os.path.exists(path):
                batch_data[table_name] = pl.read_parquet(path)
            else:
                batch_data[table_name] = pl.DataFrame()

        # Run pipeline
        # Write to a date-specific file
        out_path = os.path.join(temp_dir, f"batch-metrics-{i}-{current_date.date()}.parquet")
        writer = BatchWriter(out_path)

        runner = AnalyticsRunner(pipe, writer.write, config)
        runner.run_batch(batch_data)
        writer.close()
        
        # Clean up batch input files immediately to save space
        for table_name in config.TABLES_TO_LOAD:
            path = os.path.join(temp_dir, f"batch-{table_name}-{i}.parquet")
            if os.path.exists(path):
                os.remove(path)
        return True
    except Exception as e:
        logging.error(f"Error processing batch {i}: {e}", exc_info=True)
        raise e


def shred_data_task(
    s3_file_lists,
    table_definitions,
    ref,
    nanoseconds,
    config,
    current_date,
    temp_dir,
    get_universe,
):
    """
    Worker function to run S3 shredding in a separate process.
    """
    try:
        # Reconstruct transform_fns inside the worker
        transform_fns = {
            table.name: table.get_transform_fn(ref, nanoseconds)
            for table in table_definitions
        }

        sbs = S3SymbolBatcher(
            s3_file_lists=s3_file_lists,
            transform_fns=transform_fns,
            batching_strategy=HeuristicBatchingStrategy(
                SymbolSizeEstimator(current_date, get_universe),
                config.MAX_ROWS_PER_TABLE,
            ),
            temp_dir=temp_dir,
            storage_options=config.S3_STORAGE_OPTIONS,
            date=current_date,
            get_universe=get_universe,
            memory_per_worker=config.MEMORY_PER_WORKER,
        )

        sbs.process(num_workers=config.NUM_WORKERS)
        return True
    except Exception as e:
        logging.error(f"Shredding task failed: {e}", exc_info=True)
        raise e


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
                
                try:
                    ref = self.get_universe(current_date)
                except Exception as e:
                    if "No data available" in str(e):
                        logging.warning(f"Skipping {current_date.date()} due to missing data: {e}")
                        continue
                    raise e

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
                    logging.info("🚚 Starting S3 Shredding (Spawned Process)...")

                    tables_to_load_names = self.config.TABLES_TO_LOAD

                    s3_file_lists = {}
                    mics = ref["MIC"].unique().to_list()
                    exclude_weekends = self.config.EXCLUDE_WEEKENDS
                    for table_name in tables_to_load_names:
                        s3_file_lists[table_name] = get_files_for_date_range(
                            current_date, current_date, mics, table_name, exclude_weekends=exclude_weekends
                        )

                    table_definitions = [ALL_TABLES[name] for name in tables_to_load_names]
                    
                    # Run shredding in a separate process to ensure memory cleanup
                    with ProcessPoolExecutor(max_workers=1, mp_context=get_context('spawn')) as executor:
                        future = executor.submit(
                            shred_data_task,
                            s3_file_lists,
                            table_definitions,
                            ref,
                            nanoseconds,
                            self.config,
                            current_date,
                            TEMP_DIR,
                            self.get_universe
                        )
                        future.result() # Wait for completion and raise exceptions

                else:
                    logging.error(f"Unknown PREPARE_DATA_MODE: {MODE}")
                    raise ValueError(f"Unknown PREPARE_DATA_MODE: {MODE}")

                # --- Compute Metrics for current_date ---
                logging.info(f"📊 Computing metrics for date: {current_date.date()}")
                
                # Find batches
                logging.info(f"Listing {TEMP_DIR}: {os.listdir(TEMP_DIR)}")
                batch_files = glob.glob(os.path.join(TEMP_DIR, "batch-trades-*.parquet"))
                logging.info(f"Found batch files in {TEMP_DIR}: {batch_files}")
                batch_indices = sorted([int(f.split("-")[-1].split(".")[0]) for f in batch_files])
                
                # Determine max_workers
                max_workers = self.config.NUM_WORKERS
                if max_workers <= 0:
                    max_workers = os.cpu_count()

                with ProcessPoolExecutor(max_workers=max_workers, mp_context=get_context('spawn')) as executor:
                    futures = []
                    for i in batch_indices:
                        futures.append(
                            executor.submit(
                                process_batch_task,
                                i,
                                TEMP_DIR,
                                current_date,
                                self.config,
                                pipe
                            )
                        )
                    
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            logging.error(f"Batch processing failed: {e}")
                            raise e

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
        config.START_DATE, config.END_DATE, config.BATCH_FREQ
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
