#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="intraday_dagster_demo"
PROJECT_DIR="${PROJECT_NAME}"
DEFINITIONS_FILE="${PROJECT_DIR}/definitions.py"

export DAGSTER_HOME="$HOME/user/my-dagster"
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
    build_demo_materialization_checks,
    build_input_source_assets,
    build_s3_input_asset_checks,
    build_s3_input_observation_sensor,
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

def _build_date_partitions():
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

def _build_daily_partitions():
    start_date = "2015-01-01"
    end_date = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    start = dt.date.fromisoformat(start_date)
    end = dt.date.fromisoformat(end_date)
    date_keys = []
    current = start
    while current <= end:
        date_keys.append(current.isoformat())
        current += dt.timedelta(days=1)
    return StaticPartitionsDefinition(date_keys)

def _cbbo_partitions():
    cbbo = os.getenv("CBBO_PARTITIONS", "cbbo")
    values = [v.strip() for v in cbbo.split(",") if v.strip()]
    return StaticPartitionsDefinition(values)

partitions_def = _build_date_partitions()
daily_partitions = _build_daily_partitions()
mic_partitions = StaticPartitionsDefinition(_available_mics())
cbbo_partitions = _cbbo_partitions()

input_assets, input_table_map = build_input_source_assets(
    date_partitions_def=daily_partitions,
    mic_partitions_def=mic_partitions,
    cbbo_partitions_def=cbbo_partitions,
    asset_key_prefix=["bmll"],
    date_dim="date",
    mic_dim="mic",
    cbbo_dim="cbbo",
    group_name="BMLL",
)

demo_assets = build_demo_assets(
    partitions_def=partitions_def,
    split_passes=True,
    input_asset_keys=[asset.key for asset in input_assets],
)
demo_checks = build_demo_materialization_checks(
    check_mode=os.getenv("S3_CHECK_MODE", "head"),
    split_passes=True,
)
input_checks = build_s3_input_asset_checks(
    assets=input_assets,
    table_map=input_table_map,
    check_mode=os.getenv("S3_CHECK_MODE", "head"),
)
input_sensor = build_s3_input_observation_sensor(
    name="s3_input_observation",
    assets=input_assets,
    table_map=input_table_map,
    check_mode=os.getenv("S3_CHECK_MODE", "head"),
    mics=_available_mics()[:5],
)

defs = Definitions(
    assets=[*demo_assets, *input_assets],
    asset_checks=[*demo_checks, *input_checks],
    sensors=[input_sensor],
)
PY

echo "Starting Dagster UI..."
dagster dev -f "${DEFINITIONS_FILE}"
