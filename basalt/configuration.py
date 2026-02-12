from pydantic import BaseModel, Field, model_validator
from typing import List, Dict, Optional, Literal, Union
from enum import Enum
from .dense_analytics import DenseAnalyticsConfig
from .analytics.l2 import L2AnalyticsConfig
from .analytics.l3 import L3AnalyticsConfig
from .analytics.trade import TradeAnalyticsConfig
from .analytics.execution import ExecutionAnalyticsConfig
from .analytics.generic import GenericAnalyticsConfig
from .analytics.reaggregate import ReaggregateAnalyticsConfig
from .preprocessors.iceberg import IcebergAnalyticsConfig
from .analytics.cbbo import CBBOAnalyticsConfig
from .preprocessors.cbbo_preprocess import CBBOPreprocessConfig
from .analytics.characteristics.l3_characteristics import L3CharacteristicsConfig
from .analytics.characteristics.trade_characteristics import TradeCharacteristicsConfig
from .analytics.alpha101.alpha101 import Alpha101AnalyticsConfig
from .analytics.events import EventAnalyticsConfig
from .analytics.correlation import CorrelationAnalyticsConfig


class QualityCheckConfig(BaseModel):
    """
    Post-processing quality checks for metric outputs.
    """

    ENABLED: bool = False
    null_rate_max: float = Field(
        0.2,
        ge=0.0,
        le=1.0,
        description="Maximum allowed null rate per metric (0-1).",
        json_schema_extra={"section": "Advanced"},
    )
    ranges: Dict[str, List[float]] = Field(
        default_factory=dict,
        description="Optional per-metric bounds: {column: [min, max]}.",
        json_schema_extra={"section": "Advanced"},
    )
    action: Literal["warn", "raise"] = Field(
        "warn",
        description="Whether to warn or raise on failed checks.",
        json_schema_extra={"section": "Advanced"},
    )


class PrepareDataMode(str, Enum):
    NAIVE = "naive"
    S3_SHREDDING = "s3_shredding"


class BatchingStrategyType(str, Enum):
    HEURISTIC = "heuristic"
    POLARS_SCAN = "polars_scan"


class DenseOutputMode(str, Enum):
    ADAPTATIVE = "adaptative"
    UNIFORM = "uniform"


class OutputType(str, Enum):
    PARQUET = "parquet"
    DELTA = "delta"
    SQL = "sql"


class OutputTarget(BaseModel):
    type: OutputType = OutputType.PARQUET
    path_template: Optional[str] = Field(
        "s3://{bucket}/{prefix}/data/{datasetname}/{pass}/{universe}/{start_date}_{end_date}.parquet",
        description="Path template for parquet/delta outputs (supports {bucket}, {prefix}, {datasetname}, {universe}, {start_date}, {end_date}).",
        json_schema_extra={"depends_on": {"type": ["parquet", "delta"]}},
    )
    area: str = Field(
        "user",
        description="Bucket area to use for S3-backed outputs.",
        json_schema_extra={"depends_on": {"type": ["parquet", "delta"]}},
    )
    delta_mode: Literal["append", "overwrite"] = Field(
        "append",
        description="Write mode for delta outputs.",
        json_schema_extra={"depends_on": {"type": "delta"}},
    )
    sql_connection: Optional[str] = Field(
        None,
        description="SQLAlchemy connection string for SQL outputs.",
        json_schema_extra={"depends_on": {"type": "sql"}},
    )
    sql_table: Optional[str] = Field(
        None,
        description="SQL table name for SQL outputs.",
        json_schema_extra={"depends_on": {"type": "sql"}},
    )
    sql_if_exists: Literal["fail", "replace", "append"] = Field(
        "append",
        description="Behavior when SQL table exists.",
        json_schema_extra={"depends_on": {"type": "sql"}},
    )
    sql_use_pandas: bool = Field(
        True,
        description="Use pandas to_sql for SQL outputs (set False to use SQLAlchemy core inserts).",
        json_schema_extra={"section": "Advanced", "depends_on": {"type": "sql"}},
    )
    sql_batch_size: Optional[int] = Field(
        50000,
        description="Chunk size for SQL writes (applies to pandas/core inserts).",
        json_schema_extra={"section": "Advanced", "depends_on": {"type": "sql"}},
    )
    partition_columns: Optional[List[str]] = Field(
        None,
        description="Optional partition/primary key columns for delta/sql writes.",
        json_schema_extra={
            "section": "Advanced",
            "depends_on": {"type": ["delta", "sql"]},
        },
    )
    dedupe_on_partition: bool = Field(
        True,
        description="Remove duplicate rows based on partition columns before append.",
        json_schema_extra={
            "section": "Advanced",
            "depends_on": {"type": ["delta", "sql"]},
        },
    )
    preserve_index: bool = Field(
        False,
        description="Preserve pandas index as columns when writing outputs.",
        json_schema_extra={
            "section": "Advanced",
            "depends_on": {"type": ["parquet", "delta", "sql"]},
        },
    )
    index_name: Optional[str] = Field(
        None,
        description="Optional index column name when preserving index.",
        json_schema_extra={
            "section": "Advanced",
            "depends_on": {"type": ["parquet", "delta", "sql"]},
        },
    )


class PassConfig(BaseModel):
    """
    Configuration for a single analytics pass.

    A pass bundles a time bucket definition, selected modules, and per-module
    sub-configs. Passes are executed in order and can feed subsequent passes.
    """

    name: str
    time_bucket_seconds: float = Field(60, gt=0)
    time_bucket_anchor: Literal["end", "start"] = "end"
    time_bucket_closed: Literal["right", "left"] = "right"
    sort_keys: Optional[List[str]] = None
    batch_freq: Optional[str] = Field(
        None, description="Override global BATCH_FREQ for this pass."
    )
    modules: List[str] = Field(default_factory=list)
    output: Optional[OutputTarget] = Field(
        None,
        description="Optional per-pass output target override.",
    )
    module_inputs: Dict[str, Union[str, Dict[str, str]]] = Field(
        default_factory=dict,
        description=(
            "Optional module input overrides. Example: "
            "{'l2': 'cbbo_pass'} or {'execution': {'trades': 'pass1'}}."
        ),
        json_schema_extra={"section": "Advanced"},
    )
    extra_outputs: List[OutputTarget] = Field(
        default_factory=list,
        description="Optional additional output targets for this pass.",
    )

    ## Module specific config
    dense_analytics: DenseAnalyticsConfig = Field(default_factory=DenseAnalyticsConfig)
    l2_analytics: L2AnalyticsConfig = Field(default_factory=L2AnalyticsConfig)
    l3_analytics: L3AnalyticsConfig = Field(default_factory=L3AnalyticsConfig)
    trade_analytics: TradeAnalyticsConfig = Field(default_factory=TradeAnalyticsConfig)
    execution_analytics: ExecutionAnalyticsConfig = Field(
        default_factory=ExecutionAnalyticsConfig
    )
    iceberg_analytics: IcebergAnalyticsConfig = Field(
        default_factory=IcebergAnalyticsConfig
    )
    cbbo_analytics: CBBOAnalyticsConfig = Field(default_factory=CBBOAnalyticsConfig)
    cbbo_preprocess: CBBOPreprocessConfig = Field(
        default_factory=CBBOPreprocessConfig
    )
    l3_characteristics_analytics: L3CharacteristicsConfig = Field(
        default_factory=L3CharacteristicsConfig
    )
    trade_characteristics_analytics: TradeCharacteristicsConfig = Field(
        default_factory=TradeCharacteristicsConfig
    )
    generic_analytics: GenericAnalyticsConfig = Field(
        default_factory=GenericAnalyticsConfig
    )
    reaggregate_analytics: ReaggregateAnalyticsConfig = Field(
        default_factory=ReaggregateAnalyticsConfig
    )
    alpha101_analytics: Alpha101AnalyticsConfig = Field(
        default_factory=Alpha101AnalyticsConfig
    )
    event_analytics: EventAnalyticsConfig = Field(default_factory=EventAnalyticsConfig)
    correlation_analytics: CorrelationAnalyticsConfig = Field(
        default_factory=CorrelationAnalyticsConfig
    )
    quality_checks: QualityCheckConfig = Field(default_factory=QualityCheckConfig)

    @model_validator(mode="after")
    def propagate_pass_settings(self) -> "PassConfig":
        """Propagate pass-level settings to sub-configs."""
        self.dense_analytics.time_bucket_seconds = self.time_bucket_seconds
        self.dense_analytics.time_bucket_anchor = self.time_bucket_anchor
        self.dense_analytics.time_bucket_closed = self.time_bucket_closed
        self.l2_analytics.time_bucket_seconds = self.time_bucket_seconds
        self.l2_analytics.time_bucket_anchor = self.time_bucket_anchor
        self.l2_analytics.time_bucket_closed = self.time_bucket_closed
        self.l3_characteristics_analytics.time_bucket_seconds = self.time_bucket_seconds
        self.trade_characteristics_analytics.time_bucket_seconds = (
            self.time_bucket_seconds
        )
        if self.sort_keys:
            if "TimeBucket" not in self.sort_keys:
                self.sort_keys.append("TimeBucket")
        return self


class SchedulePartitionSelector(BaseModel):
    date: Optional[str] = Field(
        None,
        description="Partition date key (YYYY-MM-DD, range, or 'yesterday').",
    )
    universe: Optional[str] = Field(
        None,
        description="Universe partition spec (e.g. mic=XLON).",
    )


class ScheduleConfig(BaseModel):
    """
    Configuration for a Dagster schedule.

    Defines cron, timezone, and optional partition selectors to trigger runs.
    """

    name: str = Field("schedule", description="Schedule name.")
    enabled: bool = Field(False, description="Enable schedule.")
    cron: str = Field("0 2 * * *", description="Cron expression.")
    timezone: str = Field("UTC", description="Timezone for cron.")
    partitions: List[SchedulePartitionSelector] = Field(
        default_factory=list,
        description="Optional list of partition selectors (defaults to yesterday + first universe).",
    )


class BMLLJobConfig(BaseModel):
    """
    Defaults for submitting analytics runs via BMLL compute instance jobs.

    Controls instance size, bootstraps, and script locations for remote execution.
    """

    enabled: bool = Field(
        False,
        description="Enable BMLL job submissions from the CLI.",
        json_schema_extra={"section": "Automation"},
    )
    default_instance_size: int = Field(
        16,
        description="Default EC2 instance size for BMLL jobs (GB).",
        json_schema_extra={"section": "Automation"},
    )
    max_concurrent_instances: Optional[int] = Field(
        None,
        description="Optional cap on concurrent BMLL job runs.",
        json_schema_extra={"section": "Automation"},
    )
    default_conda_env: Literal["py311-stable", "py311-latest"] = Field(
        "py311-stable",
        description="Default conda environment for BMLL jobs.",
        json_schema_extra={"section": "Automation"},
    )
    max_runtime_hours: int = Field(
        1,
        description="Maximum runtime hours for BMLL instance jobs.",
        json_schema_extra={"section": "Automation"},
    )
    default_bootstrap: Optional[str] = Field(
        None,
        description="Path to a bootstrap script (defaults to auto-generated).",
        json_schema_extra={"section": "Automation"},
    )
    default_bootstrap_args: List[Union[str, int]] = Field(
        default_factory=list,
        description="Arguments to pass to the bootstrap script.",
        json_schema_extra={"section": "Automation"},
    )
    jobs_dir: str = Field(
        "/home/bmll/user/basalt/_bmll_jobs",
        description="Directory where job scripts are staged.",
        json_schema_extra={"section": "Automation"},
    )
    project_root: str = Field(
        "/home/bmll/user/basalt",
        description="Project root used in the bootstrap for PYTHONPATH and installs.",
        json_schema_extra={"section": "Automation"},
    )
    pythonpath_prefixes: List[str] = Field(
        default_factory=list,
        description="Extra PYTHONPATH entries exported by the bootstrap script.",
        json_schema_extra={"section": "Automation"},
    )
    delete_job_after: bool = Field(
        True,
        description="Delete one-off jobs after execution completes.",
        json_schema_extra={"section": "Automation"},
    )
    log_area: Literal["user", "organisation"] = Field(
        "user",
        description="Area for job logs.",
        json_schema_extra={"section": "Automation"},
    )
    log_path: str = Field(
        "job_run_logs",
        description="Path for job logs within the area (overridden per job by default).",
        json_schema_extra={"section": "Automation"},
    )
    cron_format: Literal["dagster", "bmll"] = Field(
        "dagster",
        description="Cron format for job schedules (dagster=5-field, bmll=AWS 6-field).",
        json_schema_extra={"section": "Automation"},
    )
    scheduler_days: str = Field(
        "MON-SAT",
        description="Default day-of-week for scheduler cron (e.g. MON-FRI, MON-SAT).",
        json_schema_extra={"section": "Advanced"},
    )
    visibility: Literal["private", "public"] = Field(
        "private",
        description="Visibility of submitted jobs.",
        json_schema_extra={"section": "Automation"},
    )


class AnalyticsConfig(BaseModel):
    """
    Top-level configuration for the analytics pipeline.

    Defines date range, passes, batching, I/O targets, and automation settings.
    """

    # --- Date & Scope ---
    START_DATE: Optional[str] = Field(
        None,
        description="Start date for analytics (YYYY-MM-DD).",
        json_schema_extra={"section": "Core"},
    )
    END_DATE: Optional[str] = Field(
        None,
        description="End date for analytics (YYYY-MM-DD).",
        json_schema_extra={"section": "Core"},
    )
    EXCLUDE_WEEKENDS: bool = Field(
        True,
        description="Skip weekends when building date ranges.",
        json_schema_extra={"section": "PerformanceAndExecutionEnvironment"},
    )
    DATASETNAME: str = Field(
        "sample2d",
        description="Logical dataset name used in output paths.",
        json_schema_extra={"section": "Core"},
    )
    UNIVERSE: Optional[str] = Field(
        None,
        description="Universe spec (e.g. mic=XLON). Used for output paths and metadata.",
        json_schema_extra={"section": "Core"},
    )

    # --- Analytics Passes ---
    PASSES: List[PassConfig] = Field(
        default_factory=list,
        description="Analytics passes to execute.",
        json_schema_extra={"section": "Advanced"},
    )

    # --- Automation ---
    AUTO_MATERIALIZE_ENABLED: bool = Field(
        False,
        description="Enable auto-materialization for assets.",
        json_schema_extra={"section": "Automation"},
    )
    AUTO_MATERIALIZE_LATEST_DAYS: Optional[int] = Field(
        7,
        description="If set, limit auto-materialization to the last N days.",
        json_schema_extra={"section": "Automation"},
    )
    SCHEDULES: List[ScheduleConfig] = Field(
        default_factory=list,
        description="Optional schedules for asset materialization.",
        json_schema_extra={"section": "Automation"},
    )
    BMLL_JOBS: BMLLJobConfig = Field(
        default_factory=BMLLJobConfig,
        description="Defaults for running scripts on BMLL compute instances.",
        json_schema_extra={"section": "Automation"},
    )

    # --- Batching & Performance ---
    BATCHING_STRATEGY: BatchingStrategyType = Field(
        BatchingStrategyType.HEURISTIC,
        description="Controls how input data is batched.",
        json_schema_extra={"section": "PerformanceAndExecutionEnvironment"},
    )
    NUM_WORKERS: int = Field(
        -1,
        description="Number of worker processes (-1 uses all cores).",
        json_schema_extra={"section": "PerformanceAndExecutionEnvironment"},
    )
    """
    The number of worker processes to use for parallel computation.
    -1 uses all available CPU cores.
    """

    MAX_ROWS_PER_TABLE: Dict[str, int] = Field(
        default_factory=lambda: {
            "trades": 250_000,
            "l2": 1_000_000,
            "l3": 5_000_000,
            "cbbo": 1_000_000,
            "marketstate": 200_000,
        },
        description="Row caps per table for memory control.",
        json_schema_extra={"section": "PerformanceAndExecutionEnvironment"},
    )

    # --- File Paths ---
    HEURISTIC_SIZES_PATH: str = Field(
        "/tmp/symbol_sizes/latest.parquet",
        description="Path to size heuristics parquet.",
        json_schema_extra={"section": "PerformanceAndExecutionEnvironment"},
    )
    TEMP_DIR: Optional[str] = Field(
        None,
        description="Local temp directory used during processing (auto-generated if empty).",
        json_schema_extra={"section": "Advanced"},
    )
    DAGSTER_IO_MANAGER_KEY: Optional[str] = Field(
        None,
        description="Dagster IO manager key to use when running in Dagster.",
        json_schema_extra={"section": "Outputs"},
    )

    # --- Execution ---
    PREPARE_DATA_MODE: PrepareDataMode = Field(
        PrepareDataMode.S3_SHREDDING,
        description="How input data is prepared (naive or s3_shredding).",
        json_schema_extra={"section": "PerformanceAndExecutionEnvironment"},
    )
    DEFAULT_FFILL: bool = Field(
        False,
        description="Forward-fill missing values when needed.",
        json_schema_extra={"section": "Core"},
    )
    DENSE_OUTPUT: bool = Field(
        True,
        description="Emit dense output tables.",
        json_schema_extra={"section": "Core"},
    )
    MEMORY_PER_WORKER: int = Field(
        20,
        gt=0,
        description="Memory budget per worker (GB).",
        json_schema_extra={"section": "PerformanceAndExecutionEnvironment"},
    )
    RUN_ONE_SYMBOL_AT_A_TIME: bool = Field(
        False,
        description="Serializes execution per symbol.",
        json_schema_extra={"section": "Advanced"},
    )
    EAGER_EXECUTION: bool = Field(
        False,
        description="Eagerly evaluate lazy frames.",
        json_schema_extra={"section": "Advanced"},
    )
    BATCH_FREQ: Optional[str] = Field(
        "W",
        description="Batch frequency for date partitions (e.g. D, W, M).",
        json_schema_extra={"section": "PerformanceAndExecutionEnvironment"},
    )
    LOGGING_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        "INFO",
        description="Logging verbosity.",
        json_schema_extra={"section": "Advanced"},
    )
    TABLES_TO_LOAD: Optional[List[str]] = Field(
        None,
        description="Restrict which tables are loaded.",
        json_schema_extra={"section": "Advanced"},
    )

    # --- Profiling ---
    ENABLE_PERFORMANCE_LOGS: bool = Field(
        True,
        description="Emit performance logs per stage.",
        json_schema_extra={"section": "PerformanceAndExecutionEnvironment"},
    )
    ENABLE_POLARS_PROFILING: bool = Field(
        False,
        description="Enable Polars profiling.",
        json_schema_extra={"section": "PerformanceAndExecutionEnvironment"},
    )

    # --- Output ---
    OUTPUT_TARGET: OutputTarget = Field(
        default_factory=OutputTarget,
        description="Default output target configuration.",
        json_schema_extra={"section": "Outputs"},
    )
    S3_STORAGE_OPTIONS: Dict[str, str] = Field(
        default_factory=dict,
        description="Extra options for S3 storage.",
        json_schema_extra={"section": "Advanced"},
    )
    DEFAULT_S3_REGION: str = Field(
        "us-east-1",
        description="Default S3 region for IO.",
        json_schema_extra={"section": "Advanced"},
    )
    CLEAN_UP_BATCH_FILES: bool = Field(
        True,
        description="Delete batch files after use.",
        json_schema_extra={"section": "Advanced"},
    )
    CLEAN_UP_TEMP_DIR: bool = Field(
        True,
        description="Delete temp dir after run.",
        json_schema_extra={"section": "Advanced"},
    )
    OVERWRITE_TEMP_DIR: bool = Field(
        False,
        description="Overwrite temp dir if it exists.",
        json_schema_extra={"section": "Advanced"},
    )
    SKIP_EXISTING_OUTPUT: bool = Field(
        False,
        description="Skip outputs if they already exist.",
        json_schema_extra={"section": "Outputs"},
    )
    QUALITY_CHECKS: QualityCheckConfig = Field(
        default_factory=QualityCheckConfig,
        description="Default quality checks for outputs.",
        json_schema_extra={"section": "Advanced"},
    )
    SCHEMA_LOCK_MODE: Literal["off", "warn", "raise", "update"] = Field(
        "off",
        description="Schema lock behavior when output columns change.",
        json_schema_extra={"section": "Advanced"},
    )
    SCHEMA_LOCK_PATH: Optional[str] = Field(
        None,
        description="Path to schema lock file (JSON). If unset, schema lock is skipped.",
        json_schema_extra={"section": "Advanced"},
    )

    def to_dict(self):
        return self.model_dump()
