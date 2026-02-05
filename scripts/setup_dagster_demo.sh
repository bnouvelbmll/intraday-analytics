#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="intraday_dagster_demo"
PROJECT_DIR="${PROJECT_NAME}"
DEFINITIONS_FILE="${PROJECT_DIR}/definitions.py"

python3 -m pip install --upgrade pip
python3 -m pip install dagster dagster-webserver

dagster project scaffold --name "${PROJECT_NAME}"

cat > "${DEFINITIONS_FILE}" <<'PY'
from dagster import Definitions, asset
from intraday_analytics.dagster_compat import (
    UniversePartition,
    DatePartition,
    PartitionRun,
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

@asset
def intraday_partition():
    partition = PartitionRun(
        universe=UniversePartition(name="mic", value="XLON"),
        dates=DatePartition(start_date="2025-01-02", end_date="2025-01-02"),
    )

    run_partition(
        base_config=BASE_CONFIG,
        default_get_universe=default_get_universe,
        partition=partition,
    )

defs = Definitions(assets=[intraday_partition])
PY

echo "Starting Dagster UI..."
dagster dev -f "${DEFINITIONS_FILE}"
