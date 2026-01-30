from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional
from enum import Enum
from .metrics.dense import DenseAnalyticsConfig
from .metrics.l2 import L2AnalyticsConfig
from .metrics.l3 import L3AnalyticsConfig
from .metrics.trade import TradeAnalyticsConfig
from .metrics.execution import ExecutionAnalyticsConfig


class PrepareDataMode(str, Enum):
    NAIVE = "naive"
    S3_SHREDDING = "s3_shredding"


class BatchingStrategyType(str, Enum):
    HEURISTIC = "heuristic"
    POLARS_SCAN = "polars_scan"


class DenseOutputMode(str, Enum):
    ADAPTATIVE = "adaptative"
    UNIFORM = "uniform"


@dataclass
class AnalyticsConfig:
    # --- Date & Scope ---
    START_DATE: Optional[str] = None
    """The start date for the analytics pipeline in 'YYYY-MM-DD' format."""

    END_DATE: Optional[str] = None
    """The end date for the analytics pipeline in 'YYYY-MM-DD' format."""

    EXCLUDE_WEEKENDS: bool = True
    """If True, dates falling on weekends will be excluded from processing."""

    DATASETNAME: str = "sample2d"
    """A name for the dataset being processed, used in output paths."""

    UNIVERSE: Optional[Dict[str, str]] = None
    """
    A dictionary specifying the universe of symbols to process.
    Example: {"Index": "bepacp"}
    """

    # --- Analytics Parameters ---
    TIME_BUCKET_SECONDS: float = 60
    """The size of the time buckets in seconds for time-based aggregations."""

    dense_analytics: DenseAnalyticsConfig = field(default_factory=DenseAnalyticsConfig)
    """Configuration for dense analytics metrics."""

    l2_analytics: L2AnalyticsConfig = field(default_factory=L2AnalyticsConfig)
    """Configuration for L2 order book analytics."""

    l3_analytics: L3AnalyticsConfig = field(default_factory=L3AnalyticsConfig)
    """Configuration for L3 order book analytics."""

    trade_analytics: TradeAnalyticsConfig = field(default_factory=TradeAnalyticsConfig)
    """Configuration for trade analytics."""

    execution_analytics: ExecutionAnalyticsConfig = field(
        default_factory=ExecutionAnalyticsConfig
    )
    """Configuration for execution analytics."""

    # --- Batching & Performance ---
    BATCHING_STRATEGY: str = BatchingStrategyType.HEURISTIC.value
    """
    The strategy for batching symbols.
    'heuristic': Groups symbols based on estimated memory usage.
    'polars_scan': A strategy that may be implemented for more precise batching.
    """

    NUM_WORKERS: int = -1
    """
    The number of worker processes to use for parallel computation.
    -1 uses all available CPU cores.
    """

    MAX_ROWS_PER_TABLE: Dict[str, int] = field(
        default_factory=lambda: {"trades": 500_000, "l2": 2_000_000, "l3": 10_000_000}
    )
    """
    The maximum number of rows per table to load into memory for a single batch.
    This is a key parameter for controlling memory usage.
    """

    # --- File Paths ---
    HEURISTIC_SIZES_PATH: str = "/tmp/symbol_sizes/latest.parquet"
    """
    Path to the Parquet file containing symbol size estimates for the heuristic
    batching strategy.
    """

    TEMP_DIR: str = "/tmp/temp_ian3"
    """A temporary directory for storing intermediate files."""

    AREA: str = "user"
    """
    The bmll2 storage area to use for writing the final output.
    Determines the S3 bucket and prefix.
    """

    # --- Execution ---
    PREPARE_DATA_MODE: str = PrepareDataMode.S3_SHREDDING.value
    """
    The mode for preparing data.
    's3_shredding': Efficiently reads and partitions data from S3.
    'naive': A simpler, less memory-efficient mode for local testing.
    """

    DEFAULT_FFILL: bool = False
    """
    If True, forward-fills missing values in the base analytics data.
    """

    DENSE_OUTPUT: bool = True
    """
    If True, generates a dense time-series output by filling in time buckets
    with no activity.
    """

    MEMORY_PER_WORKER: int = 20
    """The estimated memory in GiB to allocate per worker for S3 shredding."""

    RUN_ONE_SYMBOL_AT_A_TIME: bool = False
    """
    If True, processes symbols one by one. Useful for debugging but very slow.
    """

    EAGER_EXECUTION: bool = False
    """
    If True, Polars queries are executed eagerly. Can be useful for debugging
    but may increase memory usage.
    """

    BATCH_FREQ: Optional[str] = "W"
    """
    The frequency for creating date batches (e.g., 'W' for weekly, 'M' for monthly).
    If None, the frequency is auto-detected based on the date range.
    """

    LOGGING_LEVEL: str = "INFO"
    """The logging level (e.g., 'DEBUG', 'INFO', 'WARNING')."""

    TABLES_TO_LOAD: List[str] = field(
        default_factory=lambda: ["trades", "l2", "l3", "marketstate"]
    )
    """A list of the data tables to load for the analytics pipeline."""

    # --- Profiling ---
    ENABLE_PERFORMANCE_LOGS: bool = True
    """If True, logs performance metrics such as execution time and memory usage."""

    ENABLE_POLARS_PROFILING: bool = False
    """If True, enables Polars' built-in query profiling."""

    # --- Output ---
    FINAL_OUTPUT_PATH_TEMPLATE: str = (
        "s3://{bucket}/{prefix}/data/{datasetname}/{start_date}_{end_date}.parquet"
    )
    """
    A template for the final output path. Can be customized to change the
    S3 bucket, prefix, and filename format.
    """

    S3_STORAGE_OPTIONS: Dict[str, str] = field(default_factory=dict)
    """
    A dictionary of options to pass to the S3 filesystem driver, such as
    {'region': 'us-east-1'}.
    """

    CLEAN_UP_BATCH_FILES: bool = True
    """If True, intermediate batch files are deleted after they are processed."""

    CLEAN_UP_TEMP_DIR: bool = True
    """If True, the entire temporary directory is deleted at the end of the run."""

    OVERWRITE_TEMP_DIR: bool = False
    """
    If True, an existing TEMP_DIR will be deleted and recreated. If False (default),
    the pipeline will raise an error if TEMP_DIR already exists.
    """

    SKIP_EXISTING_OUTPUT: bool = False
    """
    If True, the pipeline will check if the final output file for a date batch
    already exists and skip processing if it does. Useful for resuming runs.
    """

    def __post_init__(self):
        """Propagate global settings and ensure sub-configs are dataclass instances."""
        # Ensure nested configs are dataclass objects, not dicts from USER_CONFIG
        if isinstance(self.dense_analytics, dict):
            self.dense_analytics = DenseAnalyticsConfig(**self.dense_analytics)
        if isinstance(self.l2_analytics, dict):
            self.l2_analytics = L2AnalyticsConfig(**self.l2_analytics)
        if isinstance(self.l3_analytics, dict):
            self.l3_analytics = L3AnalyticsConfig(**self.l3_analytics)
        if isinstance(self.trade_analytics, dict):
            self.trade_analytics = TradeAnalyticsConfig(**self.trade_analytics)
        if isinstance(self.execution_analytics, dict):
            self.execution_analytics = ExecutionAnalyticsConfig(
                **self.execution_analytics
            )

        # Propagate global settings
        self.dense_analytics.time_bucket_seconds = self.TIME_BUCKET_SECONDS
        self.l2_analytics.time_bucket_seconds = self.TIME_BUCKET_SECONDS

    def to_dict(self):
        return asdict(self)

    def validate(self):
        # Manual validation since we don't have Pydantic
        valid_modes = [m.value for m in PrepareDataMode]
        if self.PREPARE_DATA_MODE not in valid_modes:
            raise ValueError(
                f"Invalid PREPARE_DATA_MODE: {self.PREPARE_DATA_MODE}. Must be one of {valid_modes}"
            )

        valid_strategies = [s.value for s in BatchingStrategyType]
        if self.BATCHING_STRATEGY not in valid_strategies:
            raise ValueError(
                f"Invalid BATCHING_STRATEGY: {self.BATCHING_STRATEGY}. Must be one of {valid_strategies}"
            )

        if not isinstance(self.MAX_ROWS_PER_TABLE, dict):
            raise ValueError("MAX_ROWS_PER_TABLE must be a dictionary")
