# Intraday Analytics Pipeline

## Project Description
This project implements a robust and extensible intraday analytics pipeline designed to process high-frequency financial data. It allows for the computation of various metrics on a configurable universe of symbols and date ranges, with a focus on modularity, performance, and ease of extension.

A significant aspect of the project's complexity stems from the current limitations with Polars regarding batch sizing. As of now, Polars may require explicit guidance to maintain reasonable batch sizes, and direct streaming cannot be fully utilized due to this constraint. The current implementation explicitly overcomes these limitations, meaning some of this code may become unnecessary as Polars evolves. This necessitates careful management of data processing to avoid excessive memory consumption and ensure efficient execution.

## Features
-   **Modular Data Loading**: Easily define and integrate new input data tables without modifying core pipeline logic.
-   **Configurable Date Batching**: Process data in intelligently aligned date batches (weekly, monthly, yearly) to optimize resource usage and ensure consistent partitioning.
-   **Standardized Logging**: Comprehensive and configurable logging using Python's standard `logging` module, with improved messages and visual cues (emojis). When profiling is enabled, `VizLoggingHandler` is integrated to capture log messages within the VizTracer trace.
-   **Distributed Profiling**: Integrated support for global and distributed profiling across multiple processes using `VizTracer`, with output compatible with Perfetto for detailed performance analysis.
-   **Process Management**: Robust handling of multi-process execution using `joblib` with `max_tasks_per_child=1` for clean worker processes, including lock file management and graceful shutdown on `KeyboardInterrupt`.

## Setup

### Prerequisites
-   Python 3.9+
-   Access to BMLL data platform (assumed to be configured in your environment).

### Installation
1.  Clone this repository:
    ```bash
    git clone <repository_url>
    cd odo4
    ```
2.  Install the required Python packages:
    ```bash
    pip install -r requirements.txt
    ```

### Environment Variables
Ensure the following environment variables are set (these are typically handled by your BMLL environment setup):
-   `POLARS_MAX_THREADS`
-   `AWS_DEFAULT_REGION`

## Configuration

The pipeline's behavior is controlled by a `CONFIG` dictionary, which is a merge of `DEFAULT_CONFIG` (defined in `intraday_analytics/config.py`) and `USER_CONFIG` (defined in `main.py`).

### `USER_CONFIG` (in `main.py`)
This dictionary in `main.py` allows you to override default settings and specify parameters specific to your current run, such as:

-   `START_DATE`, `END_DATE`: The date range for data processing.
-   `UNIVERSE`: Defines the set of symbols to analyze.
-   `TIME_BUCKET_SECONDS`: Granularity for time-series data.
-   `L2_LEVELS`: Number of L2 order book levels for analytics.
-   `DENSE_OUTPUT`: Whether to enable dense output for certain analytics.
-   `ENABLE_PROFILER_TOOL`: Set to `True` to enable distributed profiling using `VizTracer`.
-   `ENABLE_PERFORMANCE_LOGS`: Set to `True` to enable custom performance logging (e.g., in `pipeline.py`).
-   `ENABLE_POLARS_PROFILING`: Set to `True` to enable Polars query profiling (e.g., in `pipeline.py`'s `save` method).
-   `PROFILING_OUTPUT_DIR`: Directory where Perfetto trace files will be saved.
-   `LOGGING_LEVEL`: Set the logging verbosity (e.g., "INFO", "DEBUG", "WARNING").
-   `DEFAULT_FREQ`: Override auto-detected date batching frequency (e.g., "W", "2W", "M", "A").
-   `TABLES_TO_LOAD`: List of table names to load (e.g., `["trades", "l2", "l3"]`).

Example `USER_CONFIG`:
```python
USER_CONFIG = {
    "START_DATE": "2025-11-01",
    "END_DATE": "2025-12-31",
    "UNIVERSE": {"Index": "bepacp"},
    "TIME_BUCKET_SECONDS": 60,
    "L2_LEVELS": 10,
    "DENSE_OUTPUT": True,
    "ENABLE_PROFILER_TOOL": True, # Enable profiling with VizTracer
    "ENABLE_PERFORMANCE_LOGS": True, # Enable custom performance logging
    "ENABLE_POLARS_PROFILING": True, # Enable Polars query profiling
    "PROFILING_OUTPUT_DIR": "/tmp/my_perf_traces",
    "LOGGING_LEVEL": "DEBUG",
    "DEFAULT_FREQ": "W", # Force weekly batches
    "TABLES_TO_LOAD": ["trades", "l2"], # Only load trades and L2 data
}
```

## Usage

To run the pipeline, simply execute the `main.py` script:

```bash
python main.py
```

The pipeline will process data for the configured date range and universe, outputting results as specified in the configuration.

## Profiling

The pipeline includes integrated support for distributed profiling using `VizTracer`, with output compatible with [Perfetto](https://ui.perfetto.dev/). `VizTracer` automatically traces `joblib` worker processes, providing a comprehensive view of multi-process execution.

1.  **Enable Profiling**: Set `"ENABLE_PROFILER_TOOL": True` in your `USER_CONFIG` in `main.py` to enable VizTracer profiling.
2.  **Run the Pipeline**: Execute `python main.py`. Trace files (`.json.gz`) will be generated in the directory specified by `PROFILING_OUTPUT_DIR` (default `/tmp/perf_traces`).
3.  **Analyze Traces**:
    -   Open your web browser and navigate to [ui.perfetto.dev](https://ui.perfetto.dev/).
    -   Click on the "Open trace file" button (or drag and drop the files).
    -   Select all the `.json.gz` trace files generated by your pipeline.

Perfetto will load and visualize the combined traces from all processes, allowing you to analyze performance bottlenecks, CPU usage, and inter-process communication.

## Extending the Pipeline

The pipeline is designed for easy extension:

### Adding New Data Tables
To add a new input data table:
1.  Create a new class in `intraday_analytics/tables.py` that inherits from `DataTable`.
2.  Implement its `load` method with specific logic for fetching and transforming that table's data.
3.  Add an instance of your new table class to the `ALL_TABLES` dictionary in `intraday_analytics/tables.py`.
4.  Update the `TABLES_TO_LOAD` list in your `USER_CONFIG` in `main.py` to include the name of your new table.

### Adding New Metrics
To add new analytics metrics:
1.  Create a new analytics module (e.g., in `intraday_analytics/metrics/`).
2.  Implement your analytics logic within a class that conforms to the `AnalyticsPipeline`'s expected module interface.
3.  Add an instance of your new analytics class to the `modules` list within the `get_pipeline` function in `main.py`.

This modular approach minimizes changes to existing code when extending functionality.
