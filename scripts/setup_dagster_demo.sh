#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="intraday_dagster_demo"
PROJECT_DIR="${PROJECT_NAME}"
DEFINITIONS_FILE="${PROJECT_DIR}/definitions.py"

export DAGSTER_HOME="/home/bmll/user/my-dagster"
mkdir -p "${DAGSTER_HOME}"

python3 -m pip install --upgrade pip
python3 -m pip install dagster dagster-webserver

dagster project scaffold --name "${PROJECT_NAME}"

cat > "${DEFINITIONS_FILE}" <<'PY'
import datetime as dt
import os

from dagster import (
    Definitions,
    StaticPartitionsDefinition,
    MultiPartitionsDefinition,
)
from intraday_analytics.dagster_compat import (
    build_demo_assets,
)
from intraday_analytics.utils import create_date_batches

def _available_mics():
    import bmll.reference
    return (
        bmll.reference.available_markets()
        .query("Schema=='Equity'")
        .query("IsAlive")
        ["MIC"]
        .tolist()
    )

def _build_partitions():
    start_date = "2015-01-01"
    end_date = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    batch_freq = os.getenv("BATCH_FREQ")
    batches = create_date_batches(start_date, end_date, batch_freq)
    date_keys = [
        b[0].date().isoformat()
        if b[0].date() == b[1].date()
        else f"{b[0].date().isoformat()}_{b[1].date().isoformat()}"
        for b in batches
    ]
    date_partitions = StaticPartitionsDefinition(date_keys)
    universe_partitions = StaticPartitionsDefinition([f"mic={mic}" for mic in _available_mics()])
    return MultiPartitionsDefinition({"universe": universe_partitions, "date": date_partitions})

partitions_def = _build_partitions()
defs = Definitions(assets=build_demo_assets(partitions_def=partitions_def))
PY

echo "Starting Dagster UI..."
dagster dev -f "${DEFINITIONS_FILE}"
