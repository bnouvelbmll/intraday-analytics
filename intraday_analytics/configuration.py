from pydantic import BaseModel, Field, model_validator
from typing import List, Dict, Optional, Literal
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

class AnalyticsConfig(BaseModel):
    # --- Date & Scope ---
    START_DATE: Optional[str] = None
    END_DATE: Optional[str] = None
    EXCLUDE_WEEKENDS: bool = True
    DATASETNAME: str = "sample2d"
    UNIVERSE: Optional[Dict[str, str]] = None

    # --- Analytics Parameters ---
    TIME_BUCKET_SECONDS: float = Field(60, gt=0)
    dense_analytics: DenseAnalyticsConfig = Field(default_factory=DenseAnalyticsConfig)
    l2_analytics: L2AnalyticsConfig = Field(default_factory=L2AnalyticsConfig)
    l3_analytics: L3AnalyticsConfig = Field(default_factory=L3AnalyticsConfig)
    trade_analytics: TradeAnalyticsConfig = Field(default_factory=TradeAnalyticsConfig)
    execution_analytics: ExecutionAnalyticsConfig = Field(default_factory=ExecutionAnalyticsConfig)

    # --- Batching & Performance ---
    BATCHING_STRATEGY: BatchingStrategyType = BatchingStrategyType.HEURISTIC
    NUM_WORKERS: int = -1
    """
    The number of worker processes to use for parallel computation.
    -1 uses all available CPU cores.
    """

    MAX_ROWS_PER_TABLE: Dict[str, int] = dict(
        default_factory=lambda: {"trades": 250_000, "l2": 1_000_000, "l3": 5_000_000}
    )

    # --- File Paths ---
    HEURISTIC_SIZES_PATH: str = "/tmp/symbol_sizes/latest.parquet"
    TEMP_DIR: str = "/tmp/temp_ian3"
    AREA: str = "user"

    # --- Execution ---
    PREPARE_DATA_MODE: PrepareDataMode = PrepareDataMode.S3_SHREDDING
    DEFAULT_FFILL: bool = False
    DENSE_OUTPUT: bool = True
    MEMORY_PER_WORKER: int = Field(20, gt=0)
    RUN_ONE_SYMBOL_AT_A_TIME: bool = False
    EAGER_EXECUTION: bool = False
    BATCH_FREQ: Optional[str] = "W"
    LOGGING_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    TABLES_TO_LOAD: List[str] = Field(default_factory=lambda: ["trades", "l2", "l3", "marketstate"], min_length=1)
    
    # --- Profiling ---
    ENABLE_PERFORMANCE_LOGS: bool = True
    ENABLE_POLARS_PROFILING: bool = False
    
    # --- Output ---
    FINAL_OUTPUT_PATH_TEMPLATE: str = "s3://{bucket}/{prefix}/data/{datasetname}/{start_date}_{end_date}.parquet"
    S3_STORAGE_OPTIONS: Dict[str, str] = Field(default_factory=dict)
    CLEAN_UP_BATCH_FILES: bool = True
    CLEAN_UP_TEMP_DIR: bool = True
    OVERWRITE_TEMP_DIR: bool = False
    SKIP_EXISTING_OUTPUT: bool = False

    @model_validator(mode='after')
    def propagate_global_settings(self) -> 'AnalyticsConfig':
        """Propagate global settings to sub-configs."""
        self.dense_analytics.time_bucket_seconds = self.TIME_BUCKET_SECONDS
        self.l2_analytics.time_bucket_seconds = self.TIME_BUCKET_SECONDS
        return self

    def to_dict(self):
        return self.model_dump()
