from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional
from enum import Enum

class PrepareDataMode(str, Enum):
    NAIVE = "naive"
    S3_SHREDDING = "s3_shredding"
    S3_SYMB = "s3symb"

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
    END_DATE: Optional[str] = None
    PART_DAYS: int = 7
    EXCLUDE_WEEKENDS: bool = True
    DATASETNAME: str = "sample2d"
    UNIVERSE: Optional[Dict[str, str]] = None

    # --- Analytics Parameters ---
    TIME_BUCKET_SECONDS: float = 60
    L2_LEVELS: int = 10

    # --- Batching & Performance ---
    BATCHING_STRATEGY: str = BatchingStrategyType.HEURISTIC.value
    NUM_WORKERS: int = -1
    MAX_ROWS_PER_TABLE: Dict[str, int] = field(
        default_factory=lambda: {"trades": 500_000, "l2": 2_000_000, "l3": 10_000_000}
    )

    # --- File Paths ---
    HEURISTIC_SIZES_PATH: str = "/tmp/symbol_sizes/latest.parquet"
    TEMP_DIR: str = "/tmp/temp_ian3"
    AREA: str = "user"

    # --- Execution ---
    PREPARE_DATA_MODE: str = PrepareDataMode.S3_SHREDDING.value
    DEFAULT_FFILL: bool = False
    DENSE_OUTPUT: bool = True
    DENSE_OUTPUT_MODE: str = DenseOutputMode.ADAPTATIVE.value
    DENSE_OUTPUT_TIME_INTERVAL: List[str] = field(default_factory=lambda: ["08:00", "15:30"])
    MEMORY_PER_WORKER: int = 20
    METRIC_COMPUTATION: str = "parallel"
    SEPARATE_METRIC_PROCESS: bool = True
    RUN_ONE_SYMBOL_AT_A_TIME: bool = False
    DEFAULT_FREQ: Optional[str] = None
    LOGGING_LEVEL: str = "INFO"
    TABLES_TO_LOAD: List[str] = field(default_factory=lambda: ["trades", "l2", "l3", "marketstate"])
    
    # --- Profiling ---
    ENABLE_PROFILER_TOOL: bool = False
    ENABLE_PERFORMANCE_LOGS: bool = True
    ENABLE_POLARS_PROFILING: bool = False
    PROFILING_OUTPUT_DIR: str = "/tmp/perf_traces"
    
    # --- Output ---
    FINAL_OUTPUT_PATH_TEMPLATE: str = "s3://{bucket}/{prefix}/data/{datasetname}/{start_date}_{end_date}.parquet"
    S3_STORAGE_OPTIONS: Dict[str, str] = field(default_factory=dict)
    CLEAN_UP_BATCH_FILES: bool = True
    CLEAN_UP_TEMP_DIR: bool = True

    def to_dict(self):
        return asdict(self)

    def validate(self):
        # Manual validation since we don't have Pydantic
        valid_modes = [m.value for m in PrepareDataMode]
        if self.PREPARE_DATA_MODE not in valid_modes:
            raise ValueError(f"Invalid PREPARE_DATA_MODE: {self.PREPARE_DATA_MODE}. Must be one of {valid_modes}")
        
        valid_strategies = [s.value for s in BatchingStrategyType]
        if self.BATCHING_STRATEGY not in valid_strategies:
            raise ValueError(f"Invalid BATCHING_STRATEGY: {self.BATCHING_STRATEGY}. Must be one of {valid_strategies}")

        if not isinstance(self.MAX_ROWS_PER_TABLE, dict):
             raise ValueError("MAX_ROWS_PER_TABLE must be a dictionary")