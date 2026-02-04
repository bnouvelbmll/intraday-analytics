import os
import shutil
import glob
import multiprocessing
from pathlib import Path
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Callable, Any
import logging

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds

from intraday_analytics.utils import (
    SYMBOL_COL,
    get_total_system_memory_gb,
    filter_existing_s3_files,
)


class SymbolBatcherStreaming:
    """
    Builds and streams batches of symbols based on memory constraints.

    This class dynamically groups symbols into batches, ensuring that the total number
    of rows for each data table (e.g., 'trades', 'l2') within a batch does not
    exceed a specified maximum. It is designed to process large datasets that
    cannot fit into memory at once by streaming data for each batch.
    """

    def __init__(
        self,
        lf_dict: Dict[str, pl.LazyFrame],
        max_rows_per_table: Dict[str, int],
    ):
        self.lf_dict = lf_dict
        self.max_rows_per_table = max_rows_per_table
        self.symbol_counts = self._compute_symbol_counts()
        self.symbols_sorted = sorted(
            set(
                sym
                for counts in self.symbol_counts.values()
                for sym in counts[SYMBOL_COL].to_list()
            )
        )

        self.batches = self._define_batches()

    # ----------------------------------------
    def _compute_symbol_counts(self) -> Dict[str, pl.DataFrame]:
        """
        Compute row counts per symbol for each table. This is done once during
        initialization.
        """
        counts = {}
        for name, lf in self.lf_dict.items():
            counts[name] = lf.group_by(SYMBOL_COL).agg(pl.len()).collect()
        return counts

    # ----------------------------------------
    def _define_batches(self) -> List[List[str]]:
        """
        Dynamically group symbols into batches without exceeding max_rows_per_table.
        """
        logging.info("Defining batches...")
        batches = []
        current_batch = []
        current_rows = {name: 0 for name in self.lf_dict}

        for sym in self.symbols_sorted:
            # check if adding this symbol exceeds max_rows for any table
            exceeds = False
            for name in self.lf_dict:
                row_count_df = self.symbol_counts[name].filter(
                    pl.col(SYMBOL_COL) == sym
                )
                rows = row_count_df["len"][0] if len(row_count_df) > 0 else 0
                if current_rows[name] + rows > self.max_rows_per_table[name]:
                    exceeds = True
                    break

            if exceeds and current_batch:
                batches.append(current_batch)
                current_batch = []
                current_rows = {name: 0 for name in self.lf_dict}

            current_batch.append(sym)
            for name in self.lf_dict:
                row_count_df = self.symbol_counts[name].filter(
                    pl.col(SYMBOL_COL) == sym
                )
                rows = row_count_df["len"][0] if len(row_count_df) > 0 else 0
                current_rows[name] += rows

        if current_batch:
            batches.append(current_batch)
        logging.info(f"Defined {len(batches)} batches.")

        return batches

    def stream_batches(self):
        """
        A generator that yields data for each batch.

        This method streams data from the underlying LazyFrames for each batch,
        avoiding loading the entire dataset into memory. It uses `collect_batches`
        to efficiently scan each table only once.
        """
        liter = {name: iter(lf.collect_batches()) for name, lf in self.lf_dict.items()}
        citer = {name: next(liter[name]) for name, lf in self.lf_dict.items()}
        last_symbol_seen: Dict[str, str] = {name: "" for name in self.lf_dict.keys()}

        def _check_sorted_by_symbol(name: str, df: pl.DataFrame, last_symbol: str) -> None:
            if df.is_empty():
                return
            if not df[SYMBOL_COL].is_sorted():
                raise ValueError(
                    f"{name} data must be sorted by {SYMBOL_COL} for streaming batches."
                )
            first_symbol = df[SYMBOL_COL][0]
            if last_symbol and first_symbol < last_symbol:
                raise ValueError(
                    f"{name} data is not globally sorted by {SYMBOL_COL}."
                )

        for name, df in citer.items():
            _check_sorted_by_symbol(name, df, last_symbol_seen[name])
            if not df.is_empty():
                last_symbol_seen[name] = df[SYMBOL_COL][-1]

        for batch_symbols in self.batches:
            batch_data: Dict[str, pl.DataFrame] = {}
            for name in self.lf_dict.keys():
                # Streaming collect_batches to avoid filtering multiple times
                dfs = []
                b = citer[name]
                mask = pl.Series(SYMBOL_COL, b[SYMBOL_COL]).is_in(list(batch_symbols))
                if mask.any():
                    dfs.append(b.filter(mask))

                while b[SYMBOL_COL].last() <= batch_symbols[-1]:
                    if b[SYMBOL_COL].last() <= batch_symbols[-1]:
                        # we haven't necessarily reached the end of the batch symbol group we need nore data
                        # and all the data from this block has been integrated
                        try:
                            citer[name] = next(liter[name])
                            b = citer[name]
                            _check_sorted_by_symbol(name, b, last_symbol_seen[name])
                            if not b.is_empty():
                                last_symbol_seen[name] = b[SYMBOL_COL][-1]
                            mask = pl.Series(SYMBOL_COL, b[SYMBOL_COL]).is_in(
                                list(batch_symbols)
                            )
                            if mask.any():
                                dfs.append(b.filter(mask))
                        except StopIteration:
                            # There is no more data
                            break
                    else:
                        # we have integrated this block data and there may remain data for the rest
                        break
                if len(dfs):
                    batch_data[name] = pl.concat(dfs).sort(
                        [
                            SYMBOL_COL,
                            (
                                "TradeTimestamp"
                                if name == "trades"
                                else "EventTimestamp"
                            ),
                        ]
                    )
                else:
                    batch_data[name] = pl.DataFrame()
            yield batch_data


# --- Batching Strategy Abstraction ---


class BatchingStrategy(ABC):
    """
    Abstract base class for defining symbol batching strategies.

    A batching strategy is responsible for dividing a list of symbols into
    smaller, manageable batches. This is a key component in controlling memory
    usage and parallelism when processing large datasets.
    """

    @abstractmethod
    def create_batches(self, symbols: List[str], **kwargs) -> List[List[str]]:
        """
        Creates batches of symbols.

        Args:
            symbols: A list of all unique symbols to be batched.
            **kwargs: Strategy-specific arguments.

        Returns:
            A list of lists, where each inner list is a batch of symbols.
        """
        pass


class SymbolSizeEstimator:
    """
    Estimates the size of data for each symbol based on historical data.

    This class loads pre-computed size estimates (e.g., median daily trade
    counts) to help in creating balanced batches. It provides a way to
    proactively estimate memory usage without having to scan the entire dataset.
    """

    def __init__(self, date, get_universe):
        self.date = date
        self.get_universe = get_universe
        self._estimates = self._load_estimates()

    def _load_estimates(self) -> pl.DataFrame:
        """Loads size estimates from an external source."""
        import pandas as pd
        import bmll
        from .api_stats import api_call
        from concurrent.futures import ThreadPoolExecutor, as_completed

        try:
            universe = self.get_universe(self.date)
        except Exception as e:
            logging.warning(
                f"Could not load universe for size estimates on {self.date}: {e}"
            )
            return pl.DataFrame(
                {SYMBOL_COL: [], "table_name": [], "estimated_rows": []},
                schema={
                    SYMBOL_COL: pl.Utf8,
                    "table_name": pl.Utf8,
                    "estimated_rows": pl.UInt64,
                },
            )

        if "ListingId" not in universe.columns or "MIC" not in universe.columns:
            logging.warning(
                f"Universe for size estimates on {self.date} is missing required columns "
                f"(ListingId, MIC). Got columns: {universe.columns}"
            )
            return pl.DataFrame(
                {SYMBOL_COL: [], "table_name": [], "estimated_rows": []},
                schema={
                    SYMBOL_COL: pl.Utf8,
                    "table_name": pl.Utf8,
                    "estimated_rows": pl.UInt64,
                },
            )

        universe_pd = universe.to_pandas()
        before_count = len(universe_pd)
        universe_pd = universe_pd.dropna(subset=["ListingId", "MIC"])
        dropped = before_count - len(universe_pd)
        if dropped:
            logging.warning(
                f"Dropped {dropped} universe rows with null ListingId/MIC for size estimates on {self.date}."
            )

        if universe_pd.empty:
            logging.warning(
                f"No valid ListingId/MIC rows available for size estimates on {self.date}."
            )
            return pl.DataFrame(
                {SYMBOL_COL: [], "table_name": [], "estimated_rows": []},
                schema={
                    SYMBOL_COL: pl.Utf8,
                    "table_name": pl.Utf8,
                    "estimated_rows": pl.UInt64,
                },
            )

        end_date_m2w = (
            (pd.Timestamp(self.date) - pd.Timedelta(days=14)).date().isoformat()
        )

        def query_mic(mic, group):
            try:
                logging.debug(f"🔍 Querying size estimates for mic {mic}")
                res = api_call(
                    "bmll.time_series.query",
                    lambda: bmll.time_series.query(
                        object_ids=group["ListingId"].tolist(),
                        metric=["TradeCount"],
                        start_date=end_date_m2w,
                        end_date=self.date,
                    ),
                    extra={"mic": mic},
                )
                res = (
                    res.groupby("ObjectId")["TradeCount|Lit"]
                    .median()
                    .reset_index()
                )
                if not isinstance(res, pd.DataFrame):
                    return None
                return res
            except Exception as e:
                logging.warning(
                    f"Could not load size estimates for mic {mic} "
                    f"(listings={len(group)}, date_range={end_date_m2w}->{self.date}). "
                    f"Error: {e}"
                )
                return None

        all_res = []
        # Assuming 'mic' column exists in the universe dataframe
        mic_groups = list(universe_pd.groupby("MIC"))

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(query_mic, mic, group): mic for mic, group in mic_groups
            }

            for future in as_completed(futures):
                res = future.result()
                if res is not None:
                    all_res.append(res)

        if not all_res:
            logging.warning(
                f"Could not load size estimates for any mic "
                f"(mics={len(mic_groups)}, listings={len(universe_pd)})."
            )
            return pl.DataFrame(
                {SYMBOL_COL: [], "table_name": [], "estimated_rows": []},
                schema={
                    SYMBOL_COL: pl.Utf8,
                    "table_name": pl.Utf8,
                    "estimated_rows": pl.UInt64,
                },
            )

        res = pd.concat(all_res)

        return pl.concat(
            [
                pl.from_pandas(
                    res.assign(
                        table_name="trades",
                        estimated_rows=res["TradeCount|Lit"],
                        **{SYMBOL_COL: res["ObjectId"]},
                    )
                ),
                pl.from_pandas(
                    res.assign(
                        table_name="l2",
                        estimated_rows=res["TradeCount|Lit"] * 20,
                        **{SYMBOL_COL: res["ObjectId"]},
                    )
                ),
                pl.from_pandas(
                    res.assign(
                        table_name="l3",
                        estimated_rows=res["TradeCount|Lit"] * 50,
                        **{SYMBOL_COL: res["ObjectId"]},
                    )
                ),
            ]
        )

    def get_estimates_for_symbols(
        self, symbols: List[str]
    ) -> Dict[str, Dict[str, int]]:
        """
        Gets the estimated row counts for a given list of symbols.
        """
        if self._estimates.is_empty():
            return {}

        symbol_estimates = self._estimates.filter(
            pl.col(SYMBOL_COL).is_in(list(symbols))
        )

        estimates_by_table = {}
        for table_name in symbol_estimates["table_name"].unique().to_list():
            table_df = symbol_estimates.filter(pl.col("table_name") == table_name)
            estimates_by_table[table_name] = dict(
                zip(table_df[SYMBOL_COL], table_df["estimated_rows"])
            )

        return estimates_by_table


class HeuristicBatchingStrategy(BatchingStrategy):
    """
    A batching strategy that uses pre-computed size estimates.

    This strategy creates batches based on estimated row counts provided by a
    `SymbolSizeEstimator`. It aims to create batches of similar total size,
    which can lead to more balanced workloads and efficient memory usage.
    """

    def __init__(
        self, estimator: SymbolSizeEstimator, max_rows_per_table: Dict[str, int]
    ):
        self.estimator = estimator
        self.max_rows_per_table = max_rows_per_table

    def create_batches(self, symbols: List[str], **kwargs) -> List[List[str]]:
        logging.info("Defining batches using HeuristicBatchingStrategy...")

        symbol_estimates = self.estimator.get_estimates_for_symbols(symbols)
        table_names = list(self.max_rows_per_table.keys())

        if not symbol_estimates:
            logging.warning(
                "No size estimates found. Falling back to fixed-size batching."
            )
            fixed_size = 100
            return [
                symbols[i : i + fixed_size] for i in range(0, len(symbols), fixed_size)
            ]

        batches = []
        current_batch = []
        current_rows = {name: 0 for name in table_names}

        for sym in sorted(symbols):
            exceeds = False
            for name in table_names:
                rows = symbol_estimates.get(name, {}).get(sym, 0)
                if rows is None:
                    rows = 0
                if current_rows[name] + rows > self.max_rows_per_table[name]:
                    exceeds = True
                    break

            if exceeds and current_batch:
                batches.append(current_batch)
                current_batch = []
                current_rows = {name: 0 for name in table_names}

            current_batch.append(sym)
            for name in table_names:
                rows = symbol_estimates.get(name, {}).get(sym, 0)
                if rows is None:
                    rows = 0
                current_rows[name] += rows

        if current_batch:
            batches.append(current_batch)

        logging.info(f"Defined {len(batches)} batches using HeuristicBatchingStrategy.")
        return batches


class PolarsScanBatchingStrategy(BatchingStrategy):
    """
    A batching strategy that uses Polars to get exact row counts.

    This strategy scans the dataset using Polars to get the exact number of rows
    for each symbol. While more accurate than heuristic-based methods, this
    approach requires a full scan of the data, which can be time-consuming for
    very large datasets.
    """

    def __init__(
        self, lf_dict: Dict[str, pl.LazyFrame], max_rows_per_table: Dict[str, int]
    ):
        self.lf_dict = lf_dict
        self.max_rows_per_table = max_rows_per_table

    def _compute_symbol_counts(self) -> Dict[str, pl.DataFrame]:
        """Compute row counts per symbol per table."""
        logging.info("Computing exact symbol counts via Polars scan...")
        counts = {}
        for name, lf in self.lf_dict.items():
            counts[name] = lf.group_by(SYMBOL_COL).agg(pl.len()).collect()
        logging.info("Finished computing exact symbol counts.")
        return counts

    def create_batches(self, symbols: List[str], **kwargs) -> List[List[str]]:
        """
        Dynamically group symbols into batches based on exact counts.
        """
        logging.info("Defining batches using PolarsScanBatchingStrategy...")

        symbol_counts = self._compute_symbol_counts()

        batches = []
        current_batch = []
        current_rows = {name: 0 for name in self.lf_dict}

        for sym in sorted(symbols):
            exceeds = False
            for name in self.lf_dict:
                row_count_df = symbol_counts[name].filter(pl.col(SYMBOL_COL) == sym)
                rows = row_count_df["len"][0] if len(row_count_df) > 0 else 0
                if current_rows[name] + rows > self.max_rows_per_table[name]:
                    exceeds = True
                    break

            if exceeds and current_batch:
                batches.append(current_batch)
                current_batch = []
                current_rows = {name: 0 for name in self.lf_dict}

            current_batch.append(sym)
            for name in self.lf_dict:
                row_count_df = symbol_counts[name].filter(pl.col(SYMBOL_COL) == sym)
                rows = row_count_df["len"][0] if len(row_count_df) > 0 else 0
                current_rows[name] += rows

        if current_batch:
            batches.append(current_batch)

        logging.info(
            f"Defined {len(batches)} batches using PolarsScanBatchingStrategy."
        )
        return batches


# --- S3 Processing and Batching ---
# @remote_process_executor_wrapper
def _process_s3_chunk(
    s3_files: List[str],
    transform_fn: Callable[[pl.LazyFrame], pl.LazyFrame],
    batch_map_table: pa.Table,
    output_root: str,
    table_name: str,
    storage_options: Dict[str, Any],
    worker_id: int,
):
    """
    Processes a chunk of S3 files in a worker process.
    This function reads a subset of S3 files, applies a transformation, joins
    the data with a batch ID map, and writes the results to local partitioned
    folders. This is a core part of the distributed data processing workflow.
    """
    s3_files = filter_existing_s3_files(s3_files, storage_options)
    if not s3_files:
        logging.warning(
            f"No readable files found for table {table_name} (worker {worker_id})."
        )
        return
    try:
        # Optimistic path: Try reading all files at once
        lf = pl.scan_parquet(s3_files, storage_options=storage_options)
        lf_filtered = transform_fn(lf)
        df_chunk = lf_filtered.collect()
    except Exception as e:
        logging.warning(
            f"Batch read failed for chunk starting with {s3_files[0] if s3_files else 'N/A'}. Falling back to individual file read. Error: {e}"
        )
        # Fallback: Try reading files one by one
        valid_dfs = []
        for f in s3_files:
            try:
                lf_single = pl.scan_parquet(f, storage_options=storage_options)
                lf_single_filtered = transform_fn(lf_single)
                valid_dfs.append(lf_single_filtered.collect())
            except Exception as inner_e:
                logging.warning(
                    f"Skipping missing or corrupt file: {f}. Error: {inner_e}"
                )

        if not valid_dfs:
            return

        df_chunk = pl.concat(valid_dfs)

    if df_chunk.is_empty():
        return

    # 4. Join with Batch Map
    pl_map = pl.from_arrow(batch_map_table)

    # Inner join attaches 'batch_id' and filters out symbols not in any batch
    shredded = df_chunk.join(pl_map, on="ListingId", how="inner")
    logging.info(
        f"Worker {worker_id}: Read {len(df_chunk)} rows. Shredded {len(shredded)} rows."
    )

    if shredded.is_empty():
        logging.warning(f"Worker {worker_id}: Shredded dataframe is empty.")
        return

    # 5. Write to Local Partitioned Dataset
    ds.write_dataset(
        shredded.to_arrow(),
        base_dir=f"{output_root}/{table_name}",
        partitioning=["batch_id"],
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
        max_rows_per_group=1_000_000,
        basename_template=f"f-{worker_id}-{{i}}.parquet",
    )


class S3SymbolBatcher:
    """
    Orchestrates the batch processing of data from S3.

    This class manages the entire workflow of reading data from S3, batching it
    according to a chosen strategy, processing it in parallel, and writing the
    output. It uses a temporary local directory to stage data during processing.
    """

    def __init__(
        self,
        s3_file_lists: Dict[str, List[str]],
        transform_fns: Dict[str, Callable[[pl.LazyFrame], pl.LazyFrame]],
        batching_strategy: BatchingStrategy,
        temp_dir: str,
        storage_options: Dict[str, str] = None,
        date: str = None,
        get_universe: Callable = None,
        memory_per_worker: int = 20,
    ):
        self.s3_file_lists = s3_file_lists
        self.transform_fns = transform_fns
        self.batching_strategy = batching_strategy
        self.temp_dir = Path(temp_dir)
        self.storage_options = storage_options or {}
        self.date = date
        self.get_universe = get_universe
        self.memory_per_worker = memory_per_worker
        self.last_total_rows = 0

        # 1. Create batches using the chosen strategy and the provided symbols
        logging.info("Computing Batches...")
        universe = self.get_universe(date)
        self.batches = self.batching_strategy.create_batches(
            sorted(universe[SYMBOL_COL].unique())
        )
        logging.info(f"{len(self.batches)} batches created.")

        # 2. Create the map for routing
        self.batch_map = self._create_batch_map_table()

    def _create_batch_map_table(self) -> pa.Table:
        """Creates a PyArrow Table mapping Symbol -> batch_id"""
        rows = []
        for b_id, symbols in enumerate(self.batches):
            for sym in symbols:
                rows.append({SYMBOL_COL: sym, "batch_id": b_id})
        df_map = pl.DataFrame(rows).with_columns(pl.col("batch_id").cast(pl.Int32))
        assert len(df_map[SYMBOL_COL].unique()) == len(
            df_map
        )  # assert symbol only once per batch
        return df_map.to_arrow()

    def process(self, num_workers=-1):
        """
        Starts the parallel processing of S3 data.

        This method sets up a thread pool to process chunks of S3 files in
        parallel. It coordinates the shredding of data into local batch-specific
        folders and then finalizes the batches.

        Args:
            num_workers: The number of worker threads to use. If -1, it defaults
                         to the number of CPU cores.
        """
        # 1. Setup Directories
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        self.temp_dir.mkdir(parents=True)

        if num_workers <= 0:
            # Calculate max workers based on memory
            total_memory_gb = get_total_system_memory_gb()
            max_workers_mem = int(total_memory_gb // self.memory_per_worker)

            # Use the minimum of CPU cores and memory-based limit
            num_workers = max(1, min(multiprocessing.cpu_count(), max_workers_mem))

        logging.info(
            f"--- PHASE 1: PARALLEL S3 STREAM & SHRED ({num_workers} workers) ---"
        )
        # Process
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = []

            for name, files in self.s3_file_lists.items():
                transform_fn = self.transform_fns.get(name, lambda x: x)

                # Split files into chunks for workers
                chunk_size = max(1, len(files) // num_workers)
                file_chunks = [
                    files[i : i + chunk_size] for i in range(0, len(files), chunk_size)
                ]

                for j, chunk in enumerate(file_chunks):
                    futures.append(
                        executor.submit(
                            _process_s3_chunk,
                            s3_files=chunk,
                            transform_fn=transform_fn,
                            batch_map_table=self.batch_map,
                            output_root=str(self.temp_dir),
                            table_name=name,
                            storage_options=self.storage_options,
                            worker_id=j,
                        )
                    )

            # Monitoring
            for i, f in enumerate(futures):
                try:
                    f.result()  # Raises exceptions if worker failed
                    if i % 10 == 0:
                        logging.info(f"Finished chunk {i}/{len(futures)}")
                except Exception as e:
                    logging.error(f"Worker failed: {e}")

        logging.info("--- PHASE 2: LOCAL SORT & FINALIZE ---")
        self._finalize_batches()
        logging.info("--- PHASE 1&2: DONE ---")

    def _finalize_batches(self):
        """
        Finalizes the batches by reading the locally shredded data, sorting it,
        and writing the final output for each batch.
        """
        logging.info(f"Finalizing {len(self.batches)} batches...")
        total_rows = 0
        for b_id in range(len(self.batches)):
            batch_data = {}
            try:
                for name in self.s3_file_lists.keys():
                    # This path contains ONLY the data for this batch, pre-filtered
                    path = self.temp_dir / name / f"{b_id}"
                    # transform_fn = self.transform_fns.get(name, lambda x: x)

                    if path.exists():
                        parquet_files = glob.glob(str(path / "*.parquet"))
                        if not parquet_files:
                            logging.warning(f"{path} has no parquet files...")
                            batch_data[name] = pl.DataFrame()
                            continue

                        # Read the local parquet shards
                        lf = pl.scan_parquet(
                            parquet_files,
                            storage_options=self.storage_options,
                        )

                        # Sort - This is now fast because dataset is small & local
                        sort_col = (
                            "TradeTimestamp" if name == "trades" else "EventTimestamp"
                        )
                        batch_data[name] = lf.sort([SYMBOL_COL, sort_col]).collect()
                    else:
                        logging.warning(f"{path} does not exist...")
                        batch_data[name] = pl.DataFrame()

                # Write final output
                self._write_output(b_id, batch_data)
                batch_rows = sum(len(df) for df in batch_data.values())
                total_rows += batch_rows
                logging.info(f"Batch {b_id}: wrote {batch_rows} rows.")
            finally:
                for name in self.s3_file_lists.keys():
                    path = self.temp_dir / name / f"{b_id}"
                    if path.exists():
                        for g in glob.glob(str(path / "*.parquet")):
                            os.unlink(g)
        self.last_total_rows = total_rows

    def _write_output(self, b_id, batch_data):
        """Writes the data for a single batch to a Parquet file."""
        for name, df in batch_data.items():
            if not df.is_empty():
                out_path = self.temp_dir / f"batch-{name}-{b_id}.parquet"
                df.write_parquet(out_path)
            else:
                logging.warning(f"Batch {b_id} for table {name} is empty.")
