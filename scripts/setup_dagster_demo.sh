#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="intraday_dagster_demo"
PROJECT_DIR="${PROJECT_NAME}"
DEFINITIONS_FILE="${PROJECT_DIR}/definitions.py"

python3 -m pip install --upgrade pip
python3 -m pip install dagster dagster-webserver

dagster project scaffold --name "${PROJECT_NAME}"

cat > "${DEFINITIONS_FILE}" <<'PY'
from dagster import (
    Definitions,
    StaticPartitionsDefinition,
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    define_asset_job,
    asset,
)
from intraday_analytics.dagster_compat import (
    UniversePartition,
    DatePartition,
    PartitionRun,
    parse_universe_spec,
    run_partition,
)

BASE_CONFIG = {
    "DATASETNAME": "dagster_demo",
    "PASSES": [
        {
            "name": "pass1",
            "time_bucket_seconds": 3600,
            "modules": ["l3_characteristics", "trade_characteristics"],
        }
    ],
}

def default_get_universe(date):
    import bmll.reference
    import polars as pl
    q = bmll.reference.query(Index="bezacp", object_type="Instrument", start_date=date).query("IsAlive")
    return pl.DataFrame(q)

universe_partitions = StaticPartitionsDefinition(["mic=XLON", "mic=XPAR"])
date_partitions = DailyPartitionsDefinition(start_date="2025-01-02", end_date="2025-01-03")
partitions_def = MultiPartitionsDefinition(
    {"universe": universe_partitions, "date": date_partitions}
)

@asset(name="demo06_characteristics", partitions_def=partitions_def)
def demo06_characteristics(context):
    keys = context.partition_key.keys_by_dimension
    universe_spec = keys["universe"]
    date_key = keys["date"]
    partition = PartitionRun(
        universe=parse_universe_spec(universe_spec),
        dates=DatePartition(start_date=date_key, end_date=date_key),
    )
    run_partition(
        base_config=BASE_CONFIG,
        default_get_universe=default_get_universe,
        partition=partition,
    )

demo_job = define_asset_job(
    name="demo06_characteristics",
    selection=["demo06_characteristics"],
    partitions_def=partitions_def,
)

defs = Definitions(assets=[demo06_characteristics], jobs=[demo_job])
PY

echo "Starting Dagster UI..."
dagster dev -f "${DEFINITIONS_FILE}"
