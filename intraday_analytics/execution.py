import os
import pickle
from multiprocessing import Process, get_context
import logging
from joblib import Parallel, delayed
import glob
import polars as pl
import pandas as pd
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed

from .batching import (
    SymbolBatcherStreaming,
    S3SymbolBatcher,
    HeuristicBatchingStrategy,
    SymbolSizeEstimator,
)
from .pipeline import AnalyticsRunner
from .utils import preload, get_files_for_date_range, create_date_batches
from .tables import ALL_TABLES
from .process import aggregate_and_write_final_output, BatchWriter, get_final_s3_path


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
        out_path = os.path.join(
            temp_dir, f"batch-metrics-{i}-{current_date.date()}.parquet"
        )
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


class ProcessInterval(Process):
    """
    A multiprocessing.Process subclass for preparing data and running a single pass of the analytics pipeline.
    """

    def __init__(
        self, sd, ed, config, pass_config, get_pipeline, get_universe, context_path
    ):
        super().__init__()
        self.sd = sd
        self.ed = ed
        self.config = config
        self.pass_config = pass_config
        self.get_pipeline = get_pipeline
        self.get_universe = get_universe
        self.context_path = context_path

    def update_and_persist_context(self, pipe, final_path):
        """
        Updates the pipeline context with the result of the current pass and persists it to disk.
        """
        try:
            # Store the result as a LazyFrame in the context
            # This allows subsequent passes to use it as input
            pipe.context[self.pass_config.name] = pl.scan_parquet(final_path)
        except Exception as e:
            logging.warning(
                f"Could not load output of pass {self.pass_config.name} into context: {e}"
            )

        # After the pass is complete, save the context
        with open(self.context_path, "wb") as f:
            pickle.dump(pipe.context, f)

    def run(self):
        logging.basicConfig(
            level=self.config.LOGGING_LEVEL.upper(),
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        )

        context = {}
        if os.path.exists(self.context_path):
            with open(self.context_path, "rb") as f:
                context = pickle.load(f)

        try:
            date_range = pd.date_range(self.sd, self.ed, freq="D")
            TEMP_DIR = self.config.TEMP_DIR
            MODE = self.config.PREPARE_DATA_MODE

            ref_partition = self.get_universe(self.sd.date().isoformat())

            for current_date in date_range:
                logging.info(
                    f"Processing date: {current_date.date()} (Pass {self.pass_config.name})"
                )

                try:
                    ref = self.get_universe(current_date.date().isoformat())
                except Exception as e:
                    if "No data available" in str(e):
                        logging.warning(
                            f"Skipping {current_date.date()} due to missing data: {e}"
                        )
                        continue
                    raise e

                symbols = ref["ListingId"].unique().to_list()
                pipe = self.get_pipeline(
                    pass_config=self.pass_config,
                    context=context,
                    ref=ref,
                    date=current_date.date().isoformat(),
                    symbols=symbols,
                )

                if MODE == "s3_shredding":
                    # ... (S3 shredding logic remains the same)
                    pass

                # Metric computation logic

                # 1. Identify input files
                lf_dict = {}
                mics = ref["MIC"].unique().to_list()
                nanoseconds = int(self.pass_config.time_bucket_seconds * 1e9)

                for table_name in self.config.TABLES_TO_LOAD:
                    files = get_files_for_date_range(
                        current_date, current_date, mics, table_name
                    )
                    if files:
                        lf = pl.scan_parquet(files)
                        # Apply post_load_process
                        table = ALL_TABLES.get(table_name)
                        if table:
                            lf = table.post_load_process(lf, ref, nanoseconds)
                        lf_dict[table_name] = lf

                if not lf_dict:
                    logging.warning(f"No data found for {current_date}")
                    continue

                # 2. Batching
                batcher = SymbolBatcherStreaming(
                    lf_dict, self.config.MAX_ROWS_PER_TABLE
                )

                tasks = []
                for i, batch_data in enumerate(batcher.stream_batches()):
                    # Write batch inputs
                    for table_name, df in batch_data.items():
                        out_path = os.path.join(
                            TEMP_DIR, f"batch-{table_name}-{i}.parquet"
                        )
                        df.write_parquet(out_path)

                    tasks.append(i)

                # 3. Process batches
                n_jobs = self.config.NUM_WORKERS
                if n_jobs == -1:
                    n_jobs = os.cpu_count()

                # Use Parallel to run process_batch_task
                # We need to ensure pipe is picklable. It should be.
                Parallel(n_jobs=n_jobs)(
                    delayed(process_batch_task)(
                        i, TEMP_DIR, current_date, self.config, pipe
                    )
                    for i in tasks
                )

            # Determine sort keys based on modules
            sort_keys = ["ListingId", "TimeBucket"]
            if "generic" in self.pass_config.modules:
                sort_keys = self.pass_config.generic_analytics.group_by

            aggregate_and_write_final_output(
                self.sd,
                self.ed,
                self.config,
                self.pass_config,
                TEMP_DIR,
                sort_keys=sort_keys,
            )

            # Update context with the result of this pass
            final_path = get_final_s3_path(
                self.sd, self.ed, self.config, self.pass_config.name
            )

            self.update_and_persist_context(pipe, final_path)

        except Exception as e:
            logging.error(
                f"Critical error in ProcessInterval (Pass {self.pass_config.name}): {e}",
                exc_info=True,
            )
            raise


def run_metrics_pipeline(config, get_pipeline, get_universe):
    """
    Runs the full intraday analytics pipeline for all configured passes.
    """
    import shutil
    import bmll2

    logging.basicConfig(
        level=config.LOGGING_LEVEL.upper(),
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,
    )

    temp_dir = config.TEMP_DIR
    if os.path.exists(temp_dir) and config.OVERWRITE_TEMP_DIR:
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)

    context_path = os.path.join(temp_dir, "context.pkl")

    try:
        for pass_config in config.PASSES:
            logging.info(f"🚀 Starting Pass {pass_config.name}")

            date_batches = create_date_batches(
                config.START_DATE, config.END_DATE, config.BATCH_FREQ
            )
            logging.info(
                f"📅 Created {len(date_batches)} date batches for Pass {pass_config.name}."
            )

            for sd, ed in date_batches:
                if config.SKIP_EXISTING_OUTPUT:
                    final_s3_path = get_final_s3_path(sd, ed, config, pass_config.name)
                    if bmll2.file_exists(final_s3_path, area=config.AREA):
                        logging.info(
                            f"✅ Output already exists for {sd.date()} -> {ed.date()} (Pass {pass_config.name}). Skipping."
                        )
                        continue

                logging.info(
                    f"🚀 Starting batch for dates: {sd.date()} -> {ed.date()} (Pass {pass_config.name})"
                )
                p = ProcessInterval(
                    sd=sd,
                    ed=ed,
                    config=config,
                    pass_config=pass_config,
                    get_pipeline=get_pipeline,
                    get_universe=get_universe,
                    context_path=context_path,
                )
                p.start()
                p.join()

                if p.exitcode != 0:
                    logging.error(
                        f"ProcessInterval for Pass {pass_config.name} failed with exit code {p.exitcode}"
                    )
                    raise RuntimeError(
                        f"ProcessInterval for Pass {pass_config.name} failed"
                    )

            logging.info(f"✅ Pass {pass_config.name} completed.")

    finally:
        if config.CLEAN_UP_TEMP_DIR and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        if os.path.exists(context_path):
            os.remove(context_path)

    logging.info("✅ All analytics passes completed.")
