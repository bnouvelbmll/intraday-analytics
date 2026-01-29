# High-Level Overview of the Data Shredding and Batching Process

## 1. Introduction

This document outlines the data processing pipeline for the `s3_shredding` mode, which is designed to handle large datasets from S3 that cannot fit into a single machine's memory.

The core problem is to efficiently process terabytes of financial data without running out of memory. The solution is a multi-stage process that involves:
1.  **Strategic Planning:** Intelligently dividing the workload into memory-manageable batches *before* reading any data.
2.  **Parallel Execution:** Shredding the raw S3 data into smaller, organized local files in parallel.
3.  **Day-by-Day Processing:** Processing data one day at a time to minimize memory footprint and ensure metric safety.
4.  **Finalization:** Consolidating the shredded data into clean, batch-specific files ready for analysis.

---

## 2. How the Shredding Logic Works (`S3SymbolBatcher`)

The entire workflow is orchestrated by the `S3SymbolBatcher` class.

### Step 1: Initialization and Batching (The Plan)

Before any data is touched, the system first defines a plan.

-   **Batch Definition:** It takes the complete list of symbols (`ListingId`s) to be processed and uses a `BatchingStrategy` to group them into smaller "batches." The primary goal is to create batches that are small enough to be processed without exceeding memory limits.
-   **Batch Map Creation:** This plan is then converted into a simple lookup table called a "batch map." This map assigns a unique `batch_id` to every symbol and is the critical link between the plan and the data.
-   **Parallel Size Estimation:** The `SymbolSizeEstimator` now uses a thread pool to query size estimates for multiple markets (MICs) in parallel, significantly speeding up the initialization phase.

### Step 2: Parallel Shredding (`_process_s3_chunk`)

This is the core of the process, where the heavy lifting happens in parallel across multiple worker processes.

-   **File Chunking:** The list of S3 files for a given table (e.g., `trades`) is divided into smaller chunks. Each worker is assigned one chunk.
-   **Lazy Reading and Filtering:** The worker reads its assigned files using `polars.scan_parquet`. This is a "lazy" operation, meaning data is not loaded into memory yet. It then applies transformations and filters, which drastically reduces the data size before it's materialized.
-   **Materialization:** The worker calls `.collect()` to load the now-filtered (and much smaller) data chunk into an in-memory DataFrame.
-   **Routing with Batch Map:** The worker joins this DataFrame with the "batch map." This adds the correct `batch_id` to every row.
-   **Writing to Local Partitions (The "Shred"):** The worker writes the DataFrame to a temporary local directory using `pyarrow.dataset.write_dataset`, partitioning the data by the `batch_id` column. All rows for `batch_id=0` go into a folder named `0`, rows for `batch_id=1` go into a folder named `1`, and so on.

This "shreds" the large, mixed S3 files into small, organized files on the local disk, neatly sorted by the batch they belong to.

### Step 3: Finalization

-   **Consolidation:** After all workers are finished, the temporary directory contains a set of partitioned folders.
-   **Final Batch Creation:** The system iterates through each `batch_id`, reads all the corresponding local files, sorts them, and writes them into a single, final file (e.g., `batch-trades-0.parquet`). This file is now a clean, memory-manageable subset of the original data, ready for the analytics pipeline.

---

## 3. Day-by-Day Processing Strategy

To further optimize memory usage and ensure the correctness of metrics that should not span across days (e.g., daily VWAP), the pipeline now operates on a **day-by-day** basis.

1.  **Daily Loop:** The `ProcessInterval` class iterates through each date in the configured range.
2.  **Daily Shredding:** For each day, the `S3SymbolBatcher` (or `SymbolBatcherStreaming` in naive mode) is initialized and run only for that specific date's data.
3.  **Immediate Computation:** Once the data for a day is shredded into batches, the analytics pipeline is immediately run on those batches.
4.  **Cleanup:** After metrics are computed for a day, the raw shredded batch files are deleted to free up disk space.
5.  **Aggregation:** Daily metric results are saved to temporary files and then aggregated into the final output at the end of the processing loop.

This approach ensures that the system's memory and disk footprint remains constant regardless of the total duration of the analysis, allowing for scalable processing of long historical periods.

---

## 4. The Role of Symbol Batching

Symbol batching is the strategic "brain" behind the entire operation.

1.  **The Blueprint:** The `BatchingStrategy` creates the high-level plan by dividing the master list of symbols into smaller batches based on estimated data size.
2.  **The Bridge (Batch Map):** This plan is converted into a simple `(ListingId, batch_id)` lookup table that is shared with all workers.
3.  **The Execution:** The shredding process executes the plan. During the shred, each worker uses the batch map to tag every single row of data with its correct `batch_id` and route it to the right destination folder on disk.

This ensures that data for memory-intensive symbols is intelligently distributed, preventing any single worker or subsequent step from running out of memory.

---

## 5. Memory Management and Chunking

The system is designed to process data in a chunk-by-chunk manner to keep memory usage low.

-   **Direct PyArrow Usage:** The `pyarrow.dataset.write_dataset` function is key to this. It allows workers to write processed data directly to partitioned files on disk without accumulating large amounts of data in memory.
-   **Controlled `.collect()`:** While `polars.collect()` is used to materialize data, it is only called on small, pre-filtered chunks of the S3 files, never on the entire dataset at once. This is a controlled and essential step to perform the join with the batch map.
-   **`collect_batches`:** This Polars method, which processes a `LazyFrame` in smaller chunks, is **not** used in the `s3_shredding` mode. It is, however, used in the alternative `"naive"` processing mode for streaming data from already-defined lazyframes.


---

## 6. Summary

The `s3_shredding` mode transforms a massive, unmanageable S3 dataset into a series of small, organized local files. It achieves this by:
1.  **Planning:** Creating a batching strategy upfront, now accelerated with parallel size estimation.
2.  **Shredding:** Using parallel workers to filter, tag, and route data to local partitions based on that plan.
3.  **Iterating:** Processing data day-by-day to maintain a low memory profile.
4.  **Finalizing:** Consolidating the shredded data into clean batch files.

This approach allows the system to process datasets far larger than available RAM, ensuring scalability and robustness.