# Polars Memory Optimization Strategies

This document details the specific strategies ("tricks") employed in this project to limit memory usage when processing large financial datasets with Polars. These strategies are critical for stability; if any are removed or disabled, the process is likely to fail with Out-Of-Memory (OOM) errors.

## 1. Temporal Partitioning (Day-by-Day Processing)

**The Trick:**
Instead of loading the entire date range (e.g., a month or year) into a single Polars LazyFrame graph, we iterate through the date range one day at a time.

**Implementation:**
-   **Location:** `intraday_analytics.execution.ProcessInterval.run`
-   **Mechanism:** A simple Python `for` loop iterates over `pd.date_range(start, end)`.
-   **Why it works:** Polars' query optimizer is powerful, but a query graph covering 365 days of high-frequency data can become too large to optimize effectively, and intermediate materializations might exceed RAM. Processing one day at a time ensures the "working set" of data is bounded by the volume of a single trading day.

## 2. Spatial Partitioning (S3 Symbol Batching & Shredding)

**The Trick:**
We partition the universe of symbols into "batches" such that the data for each batch fits comfortably in RAM. We then "shred" the monolithic S3 files into local, per-batch parquet files.

**Implementation:**
-   **Location:** `intraday_analytics.batching.S3SymbolBatcher`
-   **Symbol Size Estimation:** We use `SymbolSizeEstimator` to query historical trade counts. This allows us to estimate the memory footprint of each symbol.
-   **Bin Packing:** `HeuristicBatchingStrategy` groups symbols into batches, ensuring the sum of estimated rows < `MAX_ROWS_PER_TABLE`.
-   **Shredding:** We read S3 files in chunks, filter them, join with a `batch_id` map, and write to local partitioned datasets (`/tmp/.../batch_id=X/`).
-   **Why it works:** This converts the data layout from "Time-partitioned S3 files" (where reading one symbol requires reading the whole file) to "Symbol-batch-partitioned local files". This allows us to load *only* the data needed for the current batch into memory.

## 3. Process Isolation for Computation (The "Spawn" Trick)

**The Trick:**
We execute the metric computation pipeline for each batch in a **separate, freshly spawned process**.

**Implementation:**
-   **Location:** `intraday_analytics.execution.ProcessInterval.run` and `process_batch_task`
-   **Mechanism:**
    ```python
    with ProcessPoolExecutor(max_workers=..., mp_context=get_context('spawn')) as executor:
        executor.submit(process_batch_task, ...)
    ```
-   **Why it works:**
    1.  **Memory Reclaiming:** Polars (and the underlying Arrow/jemalloc allocators) may not immediately return memory to the OS after a DataFrame is dropped, leading to "memory bloat" over time in a long-running process. By running each batch in a short-lived process, we rely on the OS to reclaim *all* resources (RAM, file descriptors) when the process terminates. This is the only way to guarantee 100% memory cleanup.
    2.  **Deadlock Prevention:** Using `mp_context='spawn'` (instead of the default `fork` on Linux) ensures that the worker process starts with a clean slate. Forking a process that has already initialized a multi-threaded library like Polars (which uses OpenMP or Rust thread pools) can lead to deadlocks. `spawn` avoids this completely.

## 4. Immediate Cleanup

**The Trick:**
We delete the intermediate "shredded" batch files immediately after they are processed.

**Implementation:**
-   **Location:** `intraday_analytics.execution.process_batch_task`
-   **Mechanism:** `os.remove(path)` is called on the input batch files as soon as the pipeline finishes for that batch.
-   **Why it works:** This keeps the disk usage footprint low (bounded by ~1 day of data), preventing the local disk from filling up during long backtests.
