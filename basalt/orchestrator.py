import os
import pickle
import time
from multiprocessing import Process, get_context
import logging
import inspect
from joblib import Parallel, delayed
import glob
import polars as pl
import pandas as pd
import shutil
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

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


def _normalized_data_source_path(config) -> list[str]:
    raw = getattr(config, "DATA_SOURCE_PATH", None) or ["bmll"]
    out: list[str] = []
    for entry in raw:
        source = str(getattr(entry, "value", entry)).strip().lower()
        if not source:
            continue
        if source not in out:
            out.append(source)
    return out or ["bmll"]


def _load_table_lazy_with_fallback(
    *,
    table_name: str,
    config,
    pass_config,
    current_date,
    ref: pl.DataFrame,
    nanoseconds: int,
) -> tuple[pl.LazyFrame | None, str | None]:
    table = ALL_TABLES.get(table_name)
    mics = ref["MIC"].unique().to_list()
    for source in _normalized_data_source_path(config):
        try:
            if source == "bmll":
                files = []
                try:
                    files = get_files_for_date_range(
                        current_date,
                        current_date,
                        mics,
                        table_name,
                        exclude_weekends=config.EXCLUDE_WEEKENDS,
                    )
                except Exception:
                    files = []
                if files:
                    lf = pl.scan_parquet(files)
                    if table:
                        transform = table.get_transform_fn(
                            ref,
                            nanoseconds,
                            time_bucket_anchor=pass_config.time_bucket_anchor,
                            time_bucket_closed=pass_config.time_bucket_closed,
                        )
                        lf = transform(lf)
                    return lf, source
                if table is None:
                    continue
                lf = table.load_from_source(
                    "bmll",
                    markets=mics,
                    start_date=current_date,
                    end_date=current_date,
                    ref=ref,
                    nanoseconds=nanoseconds,
                    time_bucket_anchor=pass_config.time_bucket_anchor,
                    time_bucket_closed=pass_config.time_bucket_closed,
                )
                return lf.lazy() if isinstance(lf, pl.DataFrame) else lf, source

            if table is None:
                logging.warning(
                    "Skipping table '%s' via source '%s': table definition not found.",
                    table_name,
                    source,
                )
                continue
            lf = table.load_from_source(
                source,
                markets=mics,
                start_date=current_date,
                end_date=current_date,
                ref=ref,
                nanoseconds=nanoseconds,
                time_bucket_anchor=pass_config.time_bucket_anchor,
                time_bucket_closed=pass_config.time_bucket_closed,
            )
            return lf, source
        except NotImplementedError as e:
            logging.info(
                "Table '%s' source '%s' not available yet: %s",
                table_name,
                source,
                e,
            )
        except Exception as e:
            logging.warning(
                "Table '%s' source '%s' failed for %s: %s",
                table_name,
                source,
                current_date,
                e,
            )
    return None, None


def _resolve_module_names(pass_config) -> list[str]:
    module_names = list(getattr(pass_config, "preprocess_modules", []) or []) + list(pass_config.modules)
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


class _GetUniverseWrapper:
    def __init__(
        self,
        func,
        *,
        attempts: int = 4,
        base_delay: float = 1.0,
        factor: float = 2.0,
        max_delay: float = 30.0,
    ):
        self._func = func
        self.attempts = int(max(1, attempts))
        self.base_delay = float(max(0.0, base_delay))
        self.factor = float(max(1.0, factor))
        self.max_delay = float(max(0.0, max_delay))

    @staticmethod
    def _is_retryable_universe_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        retryable_markers = (
            "500",
            "502",
            "503",
            "504",
            "429",
            "timeout",
            "timed out",
            "temporar",
            "connection reset",
            "connection aborted",
            "name or service not known",
            "dns",
            "service unavailable",
        )
        return any(marker in msg for marker in retryable_markers)

    def __call__(self, date_value):
        from .api_stats import api_call

        date_str = _coerce_to_iso_date(date_value)
        for attempt in range(self.attempts):
            try:
                return api_call(
                    "get_universe",
                    lambda: self._func(date_str),
                    extra={"attempt": attempt + 1, "date": date_str},
                )
            except Exception as exc:
                if attempt == self.attempts - 1 or not self._is_retryable_universe_error(
                    exc
                ):
                    raise
                delay = min(self.max_delay, self.base_delay * (self.factor**attempt))
                logging.warning(
                    "get_universe failed for date %s (attempt %s/%s): %s. "
                    "Retrying in %.1fs.",
                    date_str,
                    attempt + 1,
                    self.attempts,
                    exc,
                    delay,
                )
                time.sleep(delay)


def _derive_tables_to_load(
    pass_config,
    user_tables,
    known_context_sources: list[str] | None = None,
):
    from .analytics_registry import get_registered_entries
    from .tables import ALL_TABLES

    entries = get_registered_entries()
    tables = []
    known_context = set(known_context_sources or [])

    def add_table(name):
        if name not in tables:
            tables.append(name)

    if user_tables:
        for name in user_tables:
            add_table(name)

    module_inputs = pass_config.module_inputs or {}
    table_names = set(ALL_TABLES.keys())
    for module in _resolve_module_names(pass_config):
        entry = entries.get(module)
        if not entry:
            logging.warning("Module '%s' not found in analytics registry.", module)
            continue
        requires = list(getattr(entry.cls, "REQUIRES", []) or [])
        overrides = module_inputs.get(module)
        if overrides is None:
            for name in requires:
                if name in table_names:
                    add_table(name)
            continue

        if isinstance(overrides, str):
            if len(requires) != 1:
                raise ValueError(
                    f"module_inputs for '{module}' must be a mapping when REQUIRES has multiple entries."
                )
            if overrides in table_names:
                add_table(overrides)
            elif overrides in known_context:
                continue
            continue

        if isinstance(overrides, dict):
            for name in requires:
                source = overrides.get(name, name)
                if source in table_names:
                    add_table(source)
                elif source in known_context:
                    continue
            continue

        raise ValueError(
            f"module_inputs for '{module}' must be a string or mapping."
        )

    return tables


def _side_output_context_keys(config) -> list[str]:
    keys: list[str] = []
    for pass_cfg in config.PASSES or []:
        side_outputs = getattr(pass_cfg, "side_outputs", {}) or {}
        for name in side_outputs.keys():
            side_cfg = side_outputs.get(name)
            if side_cfg is not None and getattr(side_cfg, "context_key", None):
                keys.append(str(side_cfg.context_key))
                continue
            namespace = getattr(pass_cfg, "side_output_namespace", None)
            if namespace:
                keys.append(f"{namespace}:{name}")
            else:
                keys.append(f"{pass_cfg.name}:{name}")
    return keys


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
        side_writers: dict[str, BatchWriter] = {}

        def _side_write(name: str, df: pl.DataFrame, listing_id: str | None = None):
            side_path = os.path.join(
                temp_dir, f"batch-side-{name}-{i}-{current_date.date()}.parquet"
            )
            if name not in side_writers:
                side_writers[name] = BatchWriter(side_path)
            side_writers[name].write(df, listing_id)

        runner = AnalyticsRunner(pipe, writer.write, config, side_writer=_side_write)
        result = runner.run_batch(batch_data)
        writer.close()
        for w in side_writers.values():
            w.close()
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

    def update_and_persist_context(self, pipe, final_path, side_paths: dict[str, str]):
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

        for side_name, side_path in (side_paths or {}).items():
            if not side_path:
                continue
            try:
                side_cfg = (self.pass_config.side_outputs or {}).get(side_name)
                if side_cfg is not None and getattr(side_cfg, "context_key", None):
                    key = str(side_cfg.context_key)
                else:
                    namespace = getattr(self.pass_config, "side_output_namespace", None)
                    key = f"{namespace}:{side_name}" if namespace else f"{self.pass_config.name}:{side_name}"
                pipe.context[key] = pl.scan_parquet(side_path)
            except Exception as e:
                logging.warning(
                    "Could not load side output %s for pass %s: %s",
                    side_name,
                    self.pass_config.name,
                    e,
                )

        # After the pass is complete, save the context
        with open(self.context_path, "wb") as f:
            pickle.dump(pipe.context, f)

    def run_naive(self, pipe, nanoseconds, ref, current_date):
        logging.info("🚚 Creating batch files (Streaming)...")

        # 1. Identify input files and create LazyFrames
        lf_dict = {}
        for table_name in self.config.TABLES_TO_LOAD:
            lf, source = _load_table_lazy_with_fallback(
                table_name=table_name,
                config=self.config,
                pass_config=self.pass_config,
                current_date=current_date,
                ref=ref,
                nanoseconds=nanoseconds,
            )
            if lf is not None:
                logging.info(
                    "Loaded table '%s' via source '%s' for %s.",
                    table_name,
                    source,
                    current_date.date(),
                )
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
        if "bmll" not in _normalized_data_source_path(self.config):
            logging.warning(
                "PREPARE_DATA_MODE=s3_shredding requires 'bmll' in DATA_SOURCE_PATH; "
                "falling back to naive mode for %s.",
                current_date.date(),
            )
            return self.run_naive(pipe, nanoseconds, ref, current_date)

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
                context["__batch_date__"] = current_date.date().isoformat()
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

            final_path, side_paths = aggregate_and_write_final_output(
                self.sd,
                self.ed,
                self.config,
                self.pass_config,
                TEMP_DIR,
                sort_keys=sort_keys,
            )

            # Update context with the result of this pass
            self.update_and_persist_context(pipe, final_path, side_paths)

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


def _find_git_root(candidates: list[str]) -> Path | None:
    for raw in candidates:
        if not raw:
            continue
        path = Path(raw).expanduser().resolve()
        if path.is_file():
            path = path.parent
        for parent in [path, *path.parents]:
            if (parent / ".git").exists():
                return parent
    return None


def _collect_git_metadata(git_root: Path | None) -> dict:
    if git_root is None:
        return {}
    data: dict = {"root": str(git_root)}
    try:
        head = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=git_root, text=True
        ).strip()
        data["commit"] = head
    except Exception:
        pass
    try:
        status = subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=git_root, text=True
        )
        data["dirty"] = bool(status.strip())
    except Exception:
        pass
    try:
        describe = subprocess.check_output(
            ["git", "describe", "--tags", "--always", "--dirty"],
            cwd=git_root,
            text=True,
        ).strip()
        data["describe"] = describe
    except Exception:
        pass
    return data


def _collect_package_versions(prefix: str = "bmll-basalt") -> dict:
    versions: dict = {}
    try:
        import importlib.metadata as _metadata

        for dist in _metadata.distributions():
            name = dist.metadata.get("Name") if dist and dist.metadata else None
            if not name:
                continue
            if name == prefix or name.startswith(f"{prefix}-"):
                versions[name] = dist.version
    except Exception:
        return versions
    return versions


def _write_run_artifact(config, temp_dir: str) -> None:
    if not getattr(config, "RUN_ARTIFACTS_ENABLED", False):
        return
    artifact_dir = getattr(config, "RUN_ARTIFACTS_DIR", None) or os.path.join(
        temp_dir, "run_artifacts"
    )
    os.makedirs(artifact_dir, exist_ok=True)
    timestamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    name = getattr(config, "RUN_ARTIFACTS_NAME", None)
    filename = name or f"run_metadata_{timestamp}.json"
    path = os.path.join(artifact_dir, filename)
    candidates = [
        os.getenv("BASALT_CONFIG_FILE", ""),
        os.getenv("BASALT_PROJECT_ROOT", ""),
        os.getcwd(),
        __file__,
    ]
    git_root = _find_git_root(candidates)
    payload = {
        "timestamp_utc": timestamp,
        "config": config.model_dump(),
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
        },
        "git": _collect_git_metadata(git_root),
        "packages": _collect_package_versions(),
    }
    try:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        logging.info("Run metadata artifact written to %s", path)
    except Exception as exc:
        logging.warning("Failed to write run metadata artifact: %s", exc)


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
            if not pass_config.modules:
                raise ValueError(
                    f"Pass '{pass_config.name}' has no modules configured."
                )

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
                    pass_config,
                    config.TABLES_TO_LOAD,
                    known_context_sources=[p.name for p in config.PASSES]
                    + _side_output_context_keys(config),
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
