from pydantic import BaseModel, Field, model_validator
from typing import List, Dict, Optional, Literal, Union
from enum import Enum
from .time.dense import DenseAnalyticsConfig
from .analytics.l2 import L2AnalyticsConfig
from .analytics.l3 import L3AnalyticsConfig
from .analytics.trade import TradeAnalyticsConfig
from .analytics.execution import ExecutionAnalyticsConfig
from .analytics.generic import GenericAnalyticsConfig
from .analytics.reaggregate import ReaggregateAnalyticsConfig
from .analytics.cbbo import CBBOAnalyticsConfig
from .time.external_events import ExternalEventsAnalyticsConfig
from .analytics.observed_events import ObservedEventsAnalyticsConfig
from .analytics.correlation import CorrelationAnalyticsConfig
from .analytics.hurst import HurstAnalyticsConfig
from .analytics.predictor import PredictorAnalyticsConfig
from .plugins import get_plugin_module_config_models



class QualityCheckConfig(BaseModel):
    """
    Post-processing quality checks for metric outputs.
    """

    ENABLED: bool = Field(
        False,
        description="Enable postprocessing data-quality checks on pass outputs.",
        json_schema_extra={"section": "Advanced"},
    )
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


class PassExpectationConfig(BaseModel):
    """
    Invariants expected from a pass output relative to its main input.

    These checks are optional and run after pass execution. They are useful for
    regression detection when a pass is expected to preserve structural properties
    (entity coverage, row count, column continuity, missingness guarantees).
    """

    preserve_listing_id_count: bool = Field(
        False,
        description=(
            "Require output to keep the same number of distinct ListingId values as "
            "the reference input."
        ),
        json_schema_extra={"section": "Advanced"},
    )
    preserve_listing_ids: bool = Field(
        False,
        description=(
            "Require output to keep exactly the same ListingId set as the reference input."
        ),
        json_schema_extra={"section": "Advanced"},
    )
    preserve_rows: bool = Field(
        False,
        description=(
            "Require output row count to equal the reference input row count."
        ),
        json_schema_extra={"section": "Advanced"},
    )
    preserve_existing_columns: bool = Field(
        False,
        description=(
            "Require output columns to include all columns present in the reference input."
        ),
        json_schema_extra={"section": "Advanced"},
    )
    all_non_nans: bool = Field(
        False,
        description=(
            "Require all output values to be non-null and non-NaN, except exempt columns."
        ),
        json_schema_extra={"section": "Advanced"},
    )
    non_nan_exempt_columns: List[str] = Field(
        default_factory=lambda: ["ListingId", "TimeBucket"],
        description=(
            "Columns excluded from the all_non_nans check (for example keys/index columns)."
        ),
        json_schema_extra={"section": "Advanced"},
    )
    action: Literal["warn", "raise"] = Field(
        "raise",
        description="Action when a pass expectation fails.",
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


class DataSourceMechanism(str, Enum):
    BMLL = "bmll"
    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"


class PassTimelineMode(str, Enum):
    SPARSE_ORIGINAL = "sparse_original"
    SPARSE_DIGITISED = "sparse_digitised"
    DAILY_ANALYTICS = "daily_analytics"
    DENSE = "dense"
    EVENT = "event"


class OutputType(str, Enum):
    PARQUET = "parquet"
    DELTA = "delta"
    SQL = "sql"


class OutputTarget(BaseModel):
    type: OutputType = Field(
        OutputType.PARQUET,
        description=(
            "Output backend type. Use 'parquet' for file output, 'delta' for "
            "delta tables, or 'sql' for relational databases."
        ),
    )
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

    name: str = Field(
        ...,
        description="Unique pass identifier used for dependencies and outputs.",
    )
    time_bucket_seconds: float = Field(
        60,
        gt=0,
        description="Base bucket size (seconds) used by time-aware modules in this pass.",
    )
    time_bucket_anchor: Literal["end", "start"] = Field(
        "end",
        description=(
            "Bucket labeling anchor: 'end' labels buckets by right boundary; "
            "'start' labels by left boundary."
        ),
    )
    time_bucket_closed: Literal["right", "left"] = Field(
        "right",
        description=(
            "Bucket boundary convention: 'right' means (start, end], 'left' means "
            "[start, end)."
        ),
    )
    sort_keys: Optional[List[str]] = Field(
        None,
        description="Optional explicit output sort keys for this pass.",
    )
    batch_freq: Optional[str] = Field(
        None, description="Override global BATCH_FREQ for this pass."
    )
    modules: List[str] = Field(
        default_factory=list,
        description="Ordered module names enabled for this pass.",
    )
    output: Optional[OutputTarget] = Field(
        None,
        description="Optional per-pass output target override.",
    )
    timeline_mode: Optional[PassTimelineMode] = Field(
        None,
        description=(
            "Output timeline mode: sparse_original, sparse_digitised, "
            "daily_analytics, dense, or event."
        ),
        json_schema_extra={"section": "Core"},
    )
    module_inputs: Dict[str, Union[str, Dict[str, str]]] = Field(
        default_factory=dict,
        description=(
            "Optional module input overrides. Example: "
            "{'l2': 'cbbo_pass'} or {'execution': {'trades': 'pass1'}}."
        ),
        json_schema_extra={"section": "Advanced"},
    )
    extension_configs: Dict[str, Dict] = Field(
        default_factory=dict,
        description=(
            "Plugin-provided module configs keyed by config key or module name "
            "(e.g. {'iceberg_analytics': {...}})."
        ),
        json_schema_extra={"section": "Advanced"},
    )
    extra_outputs: List[OutputTarget] = Field(
        default_factory=list,
        description="Optional additional output targets for this pass.",
    )

    ## Module specific config
    dense_analytics: DenseAnalyticsConfig = Field(
        default_factory=DenseAnalyticsConfig,
        description="Configuration for the dense timeline module.",
    )
    l2_analytics: L2AnalyticsConfig = Field(
        default_factory=L2AnalyticsConfig,
        description="Configuration for core L2 analytics.",
    )
    l3_analytics: L3AnalyticsConfig = Field(
        default_factory=L3AnalyticsConfig,
        description="Configuration for core L3 analytics.",
    )
    trade_analytics: TradeAnalyticsConfig = Field(
        default_factory=TradeAnalyticsConfig,
        description="Configuration for trade analytics.",
    )
    execution_analytics: ExecutionAnalyticsConfig = Field(
        default_factory=ExecutionAnalyticsConfig,
        description="Configuration for execution analytics.",
    )
    cbbo_analytics: CBBOAnalyticsConfig = Field(
        default_factory=CBBOAnalyticsConfig,
        description="Configuration for CBBO analytics.",
    )
    generic_analytics: GenericAnalyticsConfig = Field(
        default_factory=GenericAnalyticsConfig,
        description="Configuration for generic postprocessing analytics.",
    )
    reaggregate_analytics: ReaggregateAnalyticsConfig = Field(
        default_factory=ReaggregateAnalyticsConfig,
        description="Configuration for pass-output reaggregation.",
    )
    external_event_analytics: ExternalEventsAnalyticsConfig = Field(
        default_factory=ExternalEventsAnalyticsConfig,
        description="Configuration for external-events timeline mode.",
    )
    observed_events_analytics: ObservedEventsAnalyticsConfig = Field(
        default_factory=ObservedEventsAnalyticsConfig,
        description="Configuration for observed-events detection analytics.",
    )
    correlation_analytics: CorrelationAnalyticsConfig = Field(
        default_factory=CorrelationAnalyticsConfig,
        description="Configuration for correlation analytics.",
    )
    hurst_analytics: HurstAnalyticsConfig = Field(
        default_factory=HurstAnalyticsConfig,
        description="Configuration for Hurst exponent analytics.",
    )
    predictor_analytics: PredictorAnalyticsConfig = Field(
        default_factory=PredictorAnalyticsConfig,
        description="Configuration for predictor analytics (lagged correlations).",
    )
    quality_checks: QualityCheckConfig = Field(
        default_factory=QualityCheckConfig,
        description="Postprocessing quality-check rules for this pass output.",
    )
    pass_expectations: PassExpectationConfig = Field(
        default_factory=PassExpectationConfig,
        description="Optional structural invariants expected from this pass output.",
    )

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_event_keys(cls, data):
        if not isinstance(data, dict):
            return data
        out = dict(data)
        known_fields = set(cls.model_fields.keys())  # type: ignore[attr-defined]
        modules = out.get("modules")
        if isinstance(modules, list):
            out["modules"] = [
                "external_events" if m == "events" else m for m in modules
            ]
        if "event_analytics" in out and "external_event_analytics" not in out:
            out["external_event_analytics"] = out.get("event_analytics")
        extension_configs = dict(out.get("extension_configs") or {})
        for key in list(out.keys()):
            if key in known_fields:
                continue
            value = out.get(key)
            if not isinstance(value, dict):
                continue
            if key.endswith("_analytics") or key.endswith("_preprocess"):
                extension_configs.setdefault(key, value)
                out.pop(key, None)
        if extension_configs:
            out["extension_configs"] = extension_configs
        return out

    @model_validator(mode="after")
    def propagate_pass_settings(self) -> "PassConfig":
        """Propagate pass-level settings to sub-configs."""
        modules = list(dict.fromkeys(self.modules or []))
        mode = self.timeline_mode
        if mode is None:
            if "external_events" in modules:
                mode = PassTimelineMode.EVENT
            elif "dense" in modules:
                mode = PassTimelineMode.DENSE
            else:
                mode = PassTimelineMode.DENSE
            self.timeline_mode = mode

        if mode == PassTimelineMode.DENSE:
            modules = [m for m in modules if m != "external_events"]
            modules = [m for m in modules if m != "dense"]
            modules.insert(0, "dense")
            self.dense_analytics.ENABLED = True
            self.external_event_analytics.ENABLED = False
        elif mode == PassTimelineMode.EVENT:
            modules = [m for m in modules if m != "dense"]
            modules = [m for m in modules if m != "external_events"]
            modules.insert(0, "external_events")
            self.dense_analytics.ENABLED = False
            self.external_event_analytics.ENABLED = True
        else:
            modules = [m for m in modules if m not in {"dense", "external_events"}]
            self.dense_analytics.ENABLED = False
            self.external_event_analytics.ENABLED = False
        self.modules = modules

        self.dense_analytics.time_bucket_seconds = self.time_bucket_seconds
        self.dense_analytics.time_bucket_anchor = self.time_bucket_anchor
        self.dense_analytics.time_bucket_closed = self.time_bucket_closed
        self.l2_analytics.time_bucket_seconds = self.time_bucket_seconds
        self.l2_analytics.time_bucket_anchor = self.time_bucket_anchor
        self.l2_analytics.time_bucket_closed = self.time_bucket_closed
        self.trade_analytics.time_bucket_seconds = self.time_bucket_seconds
        model_map = get_plugin_module_config_models()
        updated_extensions: Dict[str, Dict] = {}
        for key, cfg in (self.extension_configs or {}).items():
            if not isinstance(cfg, dict):
                continue
            model = model_map.get(key)
            if model is None:
                updated_extensions[key] = dict(cfg)
                continue
            patched = dict(cfg)
            model_fields = getattr(model, "model_fields", {})
            if "time_bucket_seconds" in model_fields:
                patched["time_bucket_seconds"] = patched.get(
                    "time_bucket_seconds",
                    int(self.time_bucket_seconds),
                )
            if "time_bucket_anchor" in model_fields:
                patched["time_bucket_anchor"] = patched.get(
                    "time_bucket_anchor",
                    self.time_bucket_anchor,
                )
            if "time_bucket_closed" in model_fields:
                patched["time_bucket_closed"] = patched.get(
                    "time_bucket_closed",
                    self.time_bucket_closed,
                )
            try:
                patched = model.model_validate(patched).model_dump()
            except Exception:
                pass
            updated_extensions[key] = patched
        self.extension_configs = updated_extensions
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
    jobs_area: Literal["user", "organisation"] = Field(
        "user",
        description="Default area for jobs_dir/project_root when auto-resolving paths.",
        json_schema_extra={
            "section": "Automation",
            "long_doc": "When set to 'organisation', default jobs_dir and project_root\n"
            "are resolved under /home/bmll/organisation instead of /home/bmll/user.\n"
            "Explicit jobs_dir/project_root values still take precedence.\n",
        },
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
    DATA_SOURCE_PATH: List[DataSourceMechanism] = Field(
        default_factory=lambda: [DataSourceMechanism.BMLL],
        description=(
            "Ordered data-source fallback path for table loading "
            "(bmll, snowflake, databricks)."
        ),
        json_schema_extra={"section": "PerformanceAndExecutionEnvironment"},
    )
    DEFAULT_FFILL: bool = Field(
        False,
        description="Forward-fill missing values when needed.",
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
    POLARS_GPU_ENABLED: Optional[bool] = Field(
        None,
        description=(
            "Enable Polars GPU engine for LazyFrame collect. "
            "True=force GPU, False=force CPU, None=auto-detect NVIDIA host."
        ),
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
