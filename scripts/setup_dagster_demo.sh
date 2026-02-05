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
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    TimeWindowPartitionsDefinition,
)
from intraday_analytics.dagster_compat import (
    build_demo_assets,
)

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
    part_days = 1
    part_days = int(os.getenv("PART_DAYS", part_days))
    if part_days == 1:
        date_partitions = DailyPartitionsDefinition(start_date=start_date, end_date=end_date)
    else:
        date_partitions = TimeWindowPartitionsDefinition(
            start=start_date,
            cron_schedule=f"0 0 */{part_days} * *",
            fmt="%Y-%m-%d",
        )
    universe_partitions = StaticPartitionsDefinition([f"mic={mic}" for mic in _available_mics()])
    return MultiPartitionsDefinition({"universe": universe_partitions, "date": date_partitions})

partitions_def = _build_partitions()
defs = Definitions(assets=build_demo_assets(partitions_def=partitions_def))
PY

echo "Starting Dagster UI..."
dagster dev -f "${DEFINITIONS_FILE}"
