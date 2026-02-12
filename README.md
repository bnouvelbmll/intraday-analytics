# Basalt Pipeline

## Project Description
This project implements a robust and extensible basalt analytics pipeline designed to process high-frequency financial data. It allows for the computation of various metrics on a configurable universe of symbols and date ranges, with a focus on modularity, performance, and ease of extension.

The pipeline is designed to handle large volumes of data by shredding S3 data into smaller, symbol-based batches and processing them in parallel across multiple processes. This approach ensures efficient memory usage and scalable performance.

## Features
-   **Modular Architecture**: Easily define and integrate new input data tables and analytics modules without modifying core pipeline logic.
-   **Configurable Date Batching**: Process data in intelligently aligned date batches (e.g., weekly, monthly) to optimize resource usage.
-   **Performant by Default**: Uses modern, high-performance libraries like Polars and leverages process-based parallelism to scale across multiple CPU cores.
-   **Memory Safety**: Employs a "spawn" multiprocessing context and process isolation to ensure robust memory cleanup between tasks, preventing common memory leak issues in long-running data processing jobs.
-   **Standardized Logging**: Comprehensive and configurable logging using Python's standard `logging` module.

## Setup

### Prerequisites
-   Python 3.9+
-   Access to the BMLL data platform (assumed to be configured in your environment).

### Installation
1.  Clone this repository:
    ```bash
    git clone <repository_url>
    cd bmll-basalt # Or your project directory name
    ```
2.  Install the required Python packages:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

The pipeline's behavior is controlled by the `AnalyticsConfig` dataclass, which is initialized in `main.py` by merging a `USER_CONFIG` dictionary with the `DEFAULT_CONFIG`.

### `USER_CONFIG` (in `main.py`)
This dictionary allows you to override default settings for a specific run. Key options include:

-   `START_DATE`, `END_DATE`: The date range for data processing.
-   `TIME_BUCKET_SECONDS`: Granularity for time-series aggregations.
-   `BATCH_FREQ`: The frequency for creating date batches (e.g., "W" for weekly, "M" for monthly). If `None`, it's auto-detected.
-   `TABLES_TO_LOAD`: List of table names to load (e.g., `["trades", "l2", "l3"]`).
-   `LOGGING_LEVEL`: Set the logging verbosity (e.g., "INFO", "DEBUG").

Example `USER_CONFIG`:
```python
USER_CONFIG = {
    "START_DATE": "2025-11-01",
    "END_DATE": "2025-12-31",
    "TIME_BUCKET_SECONDS": 60,
    "BATCH_FREQ": "W", # Force weekly batches
    "TABLES_TO_LOAD": ["trades", "l2"], # Only load trades and L2 data
}
```

For a complete list of all configuration options and their descriptions, see the comments in `basalt/configuration.py`.

## Usage

To run the pipeline, execute the `main.py` script:

```bash
python3 main.py
```

The script will process the data for the configured date range and universe, writing the final aggregated output to the S3 location specified by `OUTPUT_TARGET.path_template` in the configuration.

## Dagster Compatibility

The framework includes a Dagster compatibility layer that models the run as a
cartesian product of universe partitions and date partitions. This allows
Dagster to materialize a single (universe, date-range) partition or fan out
across multiple MICs.

See `basalt/dagster/docs/dagster.md` for a full walkthrough, including:
- single MIC + single date
- multi-MIC fan-out with `build_partition_runs`
- `on_result` callback for artifact handling

There is also a self-contained setup script:
`basalt/dagster/demo/setup_dagster_demo.sh`

## Extending the Pipeline

The pipeline is designed for easy extension:

### Adding New Data Tables
1.  Create a new class in `basalt/tables.py` that inherits from `DataTable`.
2.  Implement its `load` and `post_load_process` methods.
3.  Add an instance of your new table class to the `ALL_TABLES` dictionary in the same file.
4.  Add the new table's name to the `TABLES_TO_LOAD` list in your `USER_CONFIG`.

### Adding New Analytics
1.  Create or extend an analytics module in `basalt/analytics/`.
2.  Implement your analytics logic within a class that follows the pattern of the existing analytics modules (e.g., `TradeAnalytics`).
3.  Add your new analytics class to the `modules` list within the `get_pipeline` function in `main.py`.

Analytics modules can also use `@analytic_expression(...)` to register documentation from expression docstrings.
