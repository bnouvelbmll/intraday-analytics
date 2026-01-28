DEFAULT_CONFIG = {
    # --- Date & Scope ---
    "START_DATE": None,
    "END_DATE": None,
    "PART_DAYS": 7,
    "DATASETNAME": "sample2d",
    "UNIVERSE": None,
    # --- Analytics Parameters ---
    "TIME_BUCKET_SECONDS": 60,
    "L2_LEVELS": 10,
    # --- Batching & Performance ---
    "BATCHING_STRATEGY": "heuristic",
    "NUM_WORKERS": -1,
    "MAX_ROWS_PER_TABLE": {"trades": 500_000, "l2": 2_000_000, "l3": 10_000_000},
    # --- File Paths ---
    "HEURISTIC_SIZES_PATH": "/tmp/symbol_sizes/latest.parquet",
    "TEMP_DIR": "/tmp/temp_ian3",
    "AREA": "user",
    # --- Execution ---
    "PREPARE_DATA_MODE": "s3symb",
    "DEFAULT_FFILL": False,
    "DENSE_OUTPUT": True,
    "DENSE_OUTPUT_MODE": "adaptative",
    "DENSE_OUTPUT_TIME_INTERVAL": ["08:00", "15:30"],
    "MEMORY_PER_WORKER": 20,
    "METRIC_COMPUTATION": "parallel",
    "SEPARATE_METRIC_PROCESS": True,
    "RUN_ONE_SYMBOL_AT_A_TIME": False,
    "PROFILE": False,
    "DEFAULT_FREQ": None,
    "LOGGING_LEVEL": "INFO",
    "TABLES_TO_LOAD": ["trades", "l2", "l3"],
}
