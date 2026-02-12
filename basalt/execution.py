import os
import pickle
from multiprocessing import Process, get_context
import logging
import inspect
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
    PolarsScanBatchingStrategy,
    shard_lazyframes_to_batches,
    DEFAULT_GROUPED_BUFFER_ROWS,
)
from .analytics_registry import resolve_batch_group_by
from .pipeline import AnalyticsRunner
from .utils import (
    preload,
    get_files_for_date_range,
    create_date_batches,
    is_s3_path,
    retry_s3,
    SYMBOL_COL,
)
from .tables import ALL_TABLES
from .api_stats import init_api_stats, summarize_api_stats
from .process import (
    aggregate_and_write_final_output,
    BatchWriter,
    get_final_s3_path,
    get_final_output_path,
)
from .schema_utils import get_output_schema
from .configuration import OutputTarget


def _coerce_to_iso_date(value):
    if isinstance(value, str):
        try:
            return pd.Timestamp(value).date().isoformat()
        except Exception:
            return value
    return value


def _resolve_module_names(pass_config) -> list[str]:
    module_names = list(pass_config.modules)
    if pass_config.trade_analytics.enable_retail_imbalance:
        if "retail_imbalance" not in module_names:
            module_names.append("retail_imbalance")
    if pass_config.trade_analytics.use_tagged_trades:
        if "iceberg" not in module_names:
            module_names.insert(0, "iceberg")
        else:
            try:
                trade_idx = module_names.index("trade")
                iceberg_idx = module_names.index("iceberg")
                if iceberg_idx > trade_idx:
                    module_names.pop(iceberg_idx)
                    module_names.insert(trade_idx, "iceberg")
            except ValueError:
                pass
    return module_names


def _resolve_batch_group_map(pass_config, ref: pl.DataFrame) -> dict | None:
    module_names = _resolve_module_names(pass_config)
    group_key = resolve_batch_group_by(module_names)
    if not group_key:
        return None
    if group_key not in ref.columns:
        logging.warning(
            "Batch grouping requested on '%s' but column missing from universe.",
            group_key,
        )
        return None
    if SYMBOL_COL not in ref.columns:
        logging.warning(
            "Batch grouping requested but universe missing '%s'.", SYMBOL_COL
        )
        return None
    return dict(zip(ref[SYMBOL_COL].to_list(), ref[group_key].to_list()))
    try:
        return pd.Timestamp(value).date().isoformat()
    except Exception:
        return value


class _GetUniverseWrapper:
    def __init__(self, func):
        self._func = func

    def __call__(self, date_value):
        return self._func(_coerce_to_iso_date(date_value))


def _derive_tables_to_load(pass_config, user_tables):
    from .dense_analytics import DenseAnalytics
    from .analytics.trade import TradeAnalytics
    from .analytics.l2 import L2AnalyticsLast, L2AnalyticsTW
    from .analytics.l3 import L3Analytics
    from .analytics.execution import ExecutionAnalytics
    from .analytics.generic import GenericAnalytics
    from .characteristics.l3_characteristics import L3CharacteristicsAnalytics
    from .characteristics.trade_characteristics import TradeCharacteristicsAnalytics

    module_requires = {
        "dense": DenseAnalytics.REQUIRES,
        "trade": TradeAnalytics.REQUIRES,
        "l2": L2AnalyticsLast.REQUIRES,
        "l2tw": L2AnalyticsTW.REQUIRES,
        "l3": L3Analytics.REQUIRES,
        "execution": ExecutionAnalytics.REQUIRES,
        "generic": GenericAnalytics.REQUIRES,
        "l3_characteristics": L3CharacteristicsAnalytics.REQUIRES,
        "trade_characteristics": TradeCharacteristicsAnalytics.REQUIRES,
    }

    from .tables import ALL_TABLES

    tables = []

    def add_table(name):
        if name not in tables:
            tables.append(name)

    if user_tables:
        for name in user_tables:
            add_table(name)

    module_inputs = pass_config.module_inputs or {}
    table_names = set(ALL_TABLES.keys())

    for module in pass_config.modules:
        overrides = module_inputs.get(module)
        requires = module_requires.get(module, [])
        if overrides is None:
            for name in requires:
                add_table(name)
            continue

        if isinstance(overrides, str):
            if len(requires) != 1:
                raise ValueError(
                    f"module_inputs for '{module}' must be a mapping when REQUIRES has multiple entries."
                )
            if overrides in table_names:
                add_table(overrides)
            continue

        if isinstance(overrides, dict):
            for name in requires:
                source = overrides.get(name, name)
                if source in table_names:
                    add_table(source)
            continue

        raise ValueError(
            f"module_inputs for '{module}' must be a string or mapping."
        )

    return tables


def _call_get_pipeline(get_pipeline, **kwargs):
    sig = inspect.signature(get_pipeline)
    if any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values()):
        return get_pipeline(**kwargs)
    filtered = {k: v for k, v in kwargs.items() if k in sig.parameters}
    return get_pipeline(**filtered)


def process_batch_task(i, temp_dir, current_date, config, pipe):
    """
    Worker function to process a single batch in a separate process.
    """
    try:
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
        result = runner.run_batch(batch_data)
        writer.close()
        if result is not None:
            logging.info(
                f"Batch {i} for {current_date.date()} produced {len(result)} rows."
            )

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
    time_bucket_anchor,
    time_bucket_closed,
    group_map,
):
    """
    Worker function to run S3 shredding in a separate process.
    """
    try:
        # Reconstruct transform_fns inside the worker
        transform_fns = {
            table.name: table.get_transform_fn(
                ref,
                nanoseconds,
                time_bucket_anchor=time_bucket_anchor,
                time_bucket_closed=time_bucket_closed,
            )
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
            group_map=group_map,
            storage_options=config.S3_STORAGE_OPTIONS,
            date=current_date,
            get_universe=get_universe,
            memory_per_worker=config.MEMORY_PER_WORKER,
        )

        sbs.process(num_workers=config.NUM_WORKERS)
        logging.info(
            f"Shredding complete for {current_date.date()}: {sbs.last_total_rows} rows."
        )
        return True
    except Exception as e:
        logging.error(f"Shredding task failed: {e}", exc_info=True)
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
            if not isinstance(final_path, str) or not final_path:
                return

            # Store the result as a LazyFrame in the context
            # This allows subsequent passes to use it as input
            def _scan_output():
                return pl.scan_parquet(final_path)

            if is_s3_path(final_path):
                pipe.context[self.pass_config.name] = retry_s3(
                    _scan_output,
                    desc=f"scan final output for pass {self.pass_config.name}",
                )
            else:
                pipe.context[self.pass_config.name] = _scan_output()
        except Exception as e:
            logging.warning(
                f"Could not load output of pass {self.pass_config.name} into context: {e}"
            )

        # After the pass is complete, save the context
        with open(self.context_path, "wb") as f:
            pickle.dump(pipe.context, f)

    def run_naive(self, pipe, nanoseconds, ref, current_date):
        logging.info("🚚 Creating batch files (Streaming)...")

        # 1. Identify input files and create LazyFrames
        lf_dict = {}
        mics = ref["MIC"].unique().to_list()
        for table_name in self.config.TABLES_TO_LOAD:
            files = get_files_for_date_range(
                current_date, current_date, mics, table_name
            )
            if files:
                try:
                    lf = pl.scan_parquet(files)
                except Exception as e:
                    logging.warning(
                        f"Skipping table {table_name} for {current_date.date()} due to "
                        f"read error: {e}"
                    )
                    continue
                table = ALL_TABLES.get(table_name)
                if table:
                    transform = table.get_transform_fn(
                        ref,
                        nanoseconds,
                        time_bucket_anchor=self.pass_config.time_bucket_anchor,
                        time_bucket_closed=self.pass_config.time_bucket_closed,
                    )
                    lf = transform(lf)
                lf_dict[table_name] = lf

        if not lf_dict:
            has_overrides = bool(self.pass_config.module_inputs)
            if not has_overrides or self.config.TABLES_TO_LOAD:
                logging.warning(f"No data found for {current_date}")
                return
            process_batch_task(
                0, self.config.TEMP_DIR, current_date, self.config, pipe
            )
            return

        group_map = _resolve_batch_group_map(self.pass_config, ref)

        tasks = []
        if group_map:
            symbols = sorted(ref[SYMBOL_COL].unique().to_list())
            strategy = PolarsScanBatchingStrategy(lf_dict, self.config.MAX_ROWS_PER_TABLE)
            batches = strategy.create_batches(symbols, group_map=group_map)
            mode = os.environ.get("INTRADAY_GROUPED_BATCHING_MODE", "buffered").lower()
            buffer_rows = 0 if mode == "preshard" else DEFAULT_GROUPED_BUFFER_ROWS
            shard_lazyframes_to_batches(
                lf_dict=lf_dict,
                batches=batches,
                temp_dir=self.config.TEMP_DIR,
                buffer_rows=buffer_rows,
            )
            tasks = list(range(len(batches)))
        else:
            # 2. Batching from LazyFrames
            batcher = SymbolBatcherStreaming(lf_dict, self.config.MAX_ROWS_PER_TABLE)

            for i, batch_data in enumerate(batcher.stream_batches()):
                for table_name, df in batch_data.items():
                    out_path = os.path.join(
                        self.config.TEMP_DIR, f"batch-{table_name}-{i}.parquet"
                    )
                    df.write_parquet(out_path)
                tasks.append(i)

        # 3. Process batches in parallel
        n_jobs = self.config.NUM_WORKERS
        if n_jobs == -1:
            n_jobs = os.cpu_count()

        Parallel(n_jobs=n_jobs)(
            delayed(process_batch_task)(
                i, self.config.TEMP_DIR, current_date, self.config, pipe
            )
            for i in tasks
        )

    def run_shredding(self, pipe, nanoseconds, ref, current_date):
        logging.info("🚚 Starting S3 Shredding (Spawned Process)...")

        tables_to_load_names = self.config.TABLES_TO_LOAD
        if not tables_to_load_names and self.pass_config.module_inputs:
            process_batch_task(
                0, self.config.TEMP_DIR, current_date, self.config, pipe
            )
            return
        s3_file_lists = {
            name: get_files_for_date_range(
                current_date,
                current_date,
                ref["MIC"].unique().to_list(),
                name,
                exclude_weekends=self.config.EXCLUDE_WEEKENDS,
            )
            for name in tables_to_load_names
        }

        table_definitions = [ALL_TABLES[name] for name in tables_to_load_names]

        with ProcessPoolExecutor(
            max_workers=1, mp_context=get_context("spawn")
        ) as executor:
            group_map = _resolve_batch_group_map(self.pass_config, ref)
            future = executor.submit(
                shred_data_task,
                s3_file_lists,
                table_definitions,
                ref,
                nanoseconds,
                self.config,
                current_date,
                self.config.TEMP_DIR,
                self.get_universe,
                self.pass_config.time_bucket_anchor,
                self.pass_config.time_bucket_closed,
                group_map,
            )
            future.result()

        # Determine batch indices from available tables in priority order.
        priority_tables = [
            "reference",
            "marketstate",
            "previous_pass",
            "trades",
            "l2",
            "l3",
        ]
        tables_to_check = [
            name for name in priority_tables if name in tables_to_load_names
        ]
        tables_to_check.extend(
            [name for name in tables_to_load_names if name not in tables_to_check]
        )

        batch_indices = []
        for table_name in tables_to_check:
            pattern = os.path.join(
                self.config.TEMP_DIR, f"batch-{table_name}-*.parquet"
            )
            batch_files = glob.glob(pattern)
            if batch_files:
                batch_indices = sorted(
                    [int(f.split("-")[-1].split(".")[0]) for f in batch_files]
                )
                break

        if not batch_indices:
            logging.warning(
                "No batch files found for any table; skipping batch processing."
            )
            return

        max_workers = self.config.NUM_WORKERS
        if max_workers <= 0:
            max_workers = os.cpu_count()

        with ProcessPoolExecutor(
            max_workers=max_workers, mp_context=get_context("spawn")
        ) as executor:
            futures = [
                executor.submit(
                    process_batch_task,
                    i,
                    self.config.TEMP_DIR,
                    current_date,
                    self.config,
                    pipe,
                )
                for i in batch_indices
            ]
            for future in as_completed(futures):
                future.result()

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

            for current_date in date_range:
                if self.config.EXCLUDE_WEEKENDS and current_date.weekday() >= 5:
                    logging.info(
                        f"Skipping weekend date: {current_date.date()} (Pass {self.pass_config.name})"
                    )
                    continue
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
                pipe = _call_get_pipeline(
                    self.get_pipeline,
                    pass_config=self.pass_config,
                    context=context,
                    ref=ref,
                    date=current_date.date().isoformat(),
                    symbols=symbols,
                    config=self.config,
                )
                nanoseconds = int(self.pass_config.time_bucket_seconds * 1e9)

                if MODE == "naive":
                    self.run_naive(pipe, nanoseconds, ref, current_date)
                elif MODE == "s3_shredding":
                    self.run_shredding(pipe, nanoseconds, ref, current_date)
                else:
                    raise ValueError(f"Unknown PREPARE_DATA_MODE: {MODE}")

            # Determine sort keys based on modules
            if self.pass_config.sort_keys:
                sort_keys = list(self.pass_config.sort_keys)
                if "TimeBucket" not in sort_keys:
                    sort_keys.append("TimeBucket")
            elif "generic" in self.pass_config.modules:
                sort_keys = self.pass_config.generic_analytics.group_by
            else:
                sort_keys = ["ListingId", "TimeBucket"]

            final_path = aggregate_and_write_final_output(
                self.sd,
                self.ed,
                self.config,
                self.pass_config,
                TEMP_DIR,
                sort_keys=sort_keys,
            )

            # Update context with the result of this pass
            self.update_and_persist_context(pipe, final_path)

        except Exception as e:
            logging.error(
                f"Critical error in ProcessInterval (Pass {self.pass_config.name}): {e}",
                exc_info=True,
            )
            raise


import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse


def check_s3_url_exists(s3_url):
    # 1. Parse the URL (e.g., s3://my-bucket/folder/file.txt)
    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    # lstrip('/') is critical because urlparse keeps the leading slash
    key = parsed.path.lstrip("/")
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        # 404 means the object definitely does not exist
        if e.response["Error"]["Code"] == "404":
            return False
        # 403 means you don't have permission to see if it exists
        raise e


def run_metrics_pipeline(config, get_universe, get_pipeline=None):
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

    # If no custom pipeline function is provided, use the default factory
    if get_pipeline is None:
        from .pipeline import create_pipeline

        get_pipeline = create_pipeline

    if config.SCHEMA_LOCK_MODE != "off" and config.SCHEMA_LOCK_PATH:
        _check_schema_lock(config)

    if "region" not in config.S3_STORAGE_OPTIONS and config.DEFAULT_S3_REGION:
        config.S3_STORAGE_OPTIONS["region"] = config.DEFAULT_S3_REGION

    temp_dir = config.TEMP_DIR
    if not temp_dir:
        import tempfile

        temp_dir = tempfile.mkdtemp(prefix="ian3_")
        config = config.model_copy(update={"TEMP_DIR": temp_dir})
        os.environ["INTRADAY_ANALYTICS_TEMP_DIR"] = temp_dir
    if os.path.exists(temp_dir) and config.OVERWRITE_TEMP_DIR:
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)
    init_api_stats(temp_dir)

    context_path = os.path.join(temp_dir, "context.pkl")

    get_universe = _GetUniverseWrapper(get_universe)

    try:
        for pass_config in config.PASSES:
            logging.info(f"🚀 Starting Pass {pass_config.name}")

            batch_freq = pass_config.batch_freq or config.BATCH_FREQ
            date_batches = create_date_batches(
                config.START_DATE, config.END_DATE, batch_freq
            )
            logging.info(
                f"📅 Created {len(date_batches)} date batches for Pass {pass_config.name}."
            )

            for sd, ed in date_batches:
                if config.SKIP_EXISTING_OUTPUT:
                    output_target = pass_config.output or config.OUTPUT_TARGET
                    if output_target is None:
                        raise RuntimeError("No output target configured.")
                    if not isinstance(output_target, OutputTarget):
                        output_target = OutputTarget.model_validate(output_target)
                    pass_label = pass_config.name
                    if output_target and getattr(output_target, "path_template", None):
                        final_s3_path = get_final_output_path(
                            sd, ed, config, pass_label, output_target
                        )
                    else:
                        final_s3_path = get_final_s3_path(sd, ed, config, pass_label)
                    if output_target.type.value == "parquet" and check_s3_url_exists(
                        final_s3_path
                    ):
                        logging.info(
                            f"✅ Output already exists for {sd.date()} -> {ed.date()} (Pass {pass_config.name}). Skipping."
                        )
                        continue

                logging.info(
                    f"🚀 Starting batch for dates: {sd.date()} -> {ed.date()} (Pass {pass_config.name})"
                )
                pass_tables_to_load = _derive_tables_to_load(
                    pass_config, config.TABLES_TO_LOAD
                )
                config = config.model_copy(
                    update={"TABLES_TO_LOAD": pass_tables_to_load}
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
        summarize_api_stats(temp_dir)
        if config.CLEAN_UP_TEMP_DIR and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        if os.path.exists(context_path):
            os.remove(context_path)

    logging.info("✅ All analytics passes completed.")


def run_multiday_pipeline(config, get_universe, get_pipeline=None):
    """
    Compatibility wrapper for running multi-day pipelines.
    """
    return run_metrics_pipeline(config, get_universe, get_pipeline=get_pipeline)


def _check_schema_lock(config):
    import json

    lock_path = config.SCHEMA_LOCK_PATH
    if not lock_path:
        return
    try:
        with open(lock_path, "r", encoding="utf-8") as handle:
            locked = json.load(handle)
    except FileNotFoundError:
        locked = {}
    except Exception:
        locked = {}

    current = {}
    for pass_cfg in config.PASSES or []:
        schema = get_output_schema(pass_cfg)
        cols = []
        for values in schema.values():
            if isinstance(values, list):
                cols.extend(values)
        current[pass_cfg.name] = cols

    if config.SCHEMA_LOCK_MODE == "update" or not locked:
        with open(lock_path, "w", encoding="utf-8") as handle:
            json.dump(current, handle, indent=2)
        return

    if locked != current:
        message = f"Schema lock mismatch. Expected={locked} Current={current}"
        if config.SCHEMA_LOCK_MODE == "raise":
            raise RuntimeError(message)
        logging.warning(message)
