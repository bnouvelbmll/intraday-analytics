# Architecture Overview

This document provides a high-level overview of the Intraday Analytics Pipeline's architecture, data flow, and key components.

## Core Philosophy

The pipeline is designed around a few core principles:

1.  **Modularity**: Components for data loading (`tables`), metric calculation (`metrics`), and execution logic (`execution`) are kept separate. This makes it easy to add new data sources or analytics without impacting the rest of the system.
2.  **Performance**: The pipeline leverages modern, high-performance libraries like Polars and uses process-based parallelism to scale computations across multiple CPU cores.
3.  **Memory Safety**: By processing data in batches and using spawned processes for heavy lifting, the pipeline ensures that memory is reliably reclaimed, preventing common issues in long-running data processing tasks.

## Data Flow

The pipeline executes in the following stages:

1.  **Initialization**:
    *   The `main.py` script loads the default and user configurations into a single `AnalyticsConfig` object.
    *   Logging is configured.

2.  **Date Batching**:
    *   The total date range (`START_DATE` to `END_DATE`) is divided into smaller batches based on the `BATCH_FREQ` setting (e.g., weekly). This is handled by `create_date_batches`.

3.  **Processing per Date Batch**:
    *   For each date batch, a `ProcessInterval` is started. This runs the main data preparation and computation in an isolated process.
    *   **Inside `ProcessInterval`**:
        1.  **Universe Retrieval**: The list of symbols to process for the batch is fetched by the `get_universe` function.
        2.  **Data Shredding (`s3_shredding` mode)**:
            *   The `S3SymbolBatcher` identifies all required S3 files for the current date range.
            *   It reads these files in parallel, transforms the data on the fly, and "shreds" them into smaller Parquet files in a local temporary directory, partitioned by symbol batch. This is the key to avoiding out-of-memory errors.
        3.  **Metric Computation**:
            *   A `ProcessPoolExecutor` is used to process the local, shredded batch files in parallel.
            *   For each batch, the `AnalyticsPipeline` runs a series of analytics modules (e.g., `TradeAnalytics`, `L2Analytics`).
            *   The results from each module are joined together, and the output is written to an intermediate Parquet file in the temporary directory.

4.  **Aggregation**:
    *   After all date batches are processed, the `aggregate_and_write_final_output` function reads all the intermediate results from the temporary directory.
    *   It combines them into a single DataFrame, sorts the data, and writes the final output to the S3 location specified in the configuration.

5.  **Cleanup**:
    *   The temporary directory and all intermediate files are deleted.

## Key Components

*   **`main.py`**: The entry point of the application. It handles configuration, defines the `get_universe` and `get_pipeline` functions, and orchestrates the overall execution flow.
*   **`intraday_analytics/configuration.py`**: Defines the `AnalyticsConfig` dataclass, which provides a strongly-typed structure for all pipeline settings.
*   **`intraday_analytics/execution.py`**: Contains the core orchestration logic, including the `run_metrics_pipeline` function and the `ProcessInterval` class that manages the work for each date batch.
*   **`intraday_analytics/pipeline.py`**: Defines the `AnalyticsPipeline` and `AnalyticsRunner` classes, which are responsible for executing the sequence of analytics modules on a given batch of data.
*   **`intraday_analytics/batching.py`**: Contains the logic for creating symbol-based batches from the source data. The `S3SymbolBatcher` is the primary component for the efficient `s3_shredding` mode.
*   **`intraday_analytics/tables.py`**: Defines the data sources. Each table (e.g., `TradesPlusTable`) is represented by a class that knows how to load and pre-process its specific data.
*   **`intraday_analytics/metrics/`**: This directory contains the individual analytics modules. Each module is responsible for calculating a specific set of metrics (e.g., `dense.py` for dense analytics, `trade.py` for trade-based metrics).
