#!/usr/bin/env bash
set -euo pipefail

PROJECT_NAME="intraday_dagster_demo"
PROJECT_DIR="${PROJECT_NAME}"
DEFINITIONS_FILE="${PROJECT_DIR}/definitions.py"

export DAGSTER_HOME="$HOME/user/my-dagster"
export BMLL_REPO_ROOT="$(pwd)"
mkdir -p "${DAGSTER_HOME}"

python3 -m pip install --upgrade pip
python3 -m pip install dagster dagster-webserver

dagster project scaffold --name "${PROJECT_NAME}"

cat > "${DEFINITIONS_FILE}" <<'PY'
import datetime as dt
import os

from dagster import (
    Definitions,
    DailyPartitionsDefinition,
    StaticPartitionsDefinition,
    TimeWindowPartitionsDefinition,
    MultiPartitionsDefinition,
    schedule,
)
from intraday_analytics.dagster_compat import (
    build_assets,
    build_materialization_checks,
    build_schedules,
    build_input_source_assets,
    build_s3_input_asset_checks,
    build_s3_input_observation_sensor,
    build_s3_input_sync_job,
    build_s3_input_sync_asset,
    list_cbbo_partitions_from_s3,
    MICUniverse,
    IndexUniverse,
    OPOLUniverse,
    CustomUniverse,
    CartProdUniverse,
    build_universe_partitions,
    default_universes,
)
import yaml
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
    if batch_freq == "D":
        date_partitions = DailyPartitionsDefinition(start_date=start_date, end_date=end_date)
    else:
        cron = "0 0 * * *"
        if batch_freq == "W":
            cron = "0 0 * * 1"
        elif batch_freq == "2W":
            cron = "0 0 */14 * *"
        elif batch_freq == "M":
            cron = "0 0 1 * *"
        elif batch_freq == "A":
            cron = "0 0 1 1 *"
        date_partitions = TimeWindowPartitionsDefinition(
            start=start_date,
            end=end_date,
            fmt="%Y-%m-%d",
            cron_schedule=cron,
        )
    universes = default_universes()
    universe_partitions = StaticPartitionsDefinition(build_universe_partitions(universes))
    return MultiPartitionsDefinition({"universe": universe_partitions, "date": date_partitions})

def _build_daily_partitions():
    start_date = "2015-01-01"
    end_date = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    return DailyPartitionsDefinition(start_date=start_date, end_date=end_date)

def _cbbo_partitions():
    cbbo = os.getenv("CBBO_PARTITIONS", "cbbo")
    values = [v.strip() for v in cbbo.split(",") if v.strip()]
    if values != ["cbbo"]:
        return StaticPartitionsDefinition(values)
    return StaticPartitionsDefinition(list_cbbo_partitions_from_s3())

partitions_def = _build_date_partitions()
daily_partitions = _build_daily_partitions()
mic_partitions = StaticPartitionsDefinition(_available_mics())
cbbo_partitions = _cbbo_partitions()

input_assets, input_table_map = build_input_source_assets(
    date_partitions_def=daily_partitions,
    mic_partitions_def=mic_partitions,
    cbbo_partitions_def=cbbo_partitions,
    asset_key_prefix=["BMLL"],
    date_dim="date",
    mic_dim="mic",
    cbbo_dim="cbbo",
    group_name="BMLL",
)

demo_assets = build_assets(
    partitions_def=partitions_def,
    split_passes=True,
    input_asset_keys=[asset.key for asset in input_assets],
)
demo_schedules = build_schedules(
    partitions_def=partitions_def,
    split_passes=True,
)
demo_checks = build_materialization_checks(
    check_mode=os.getenv("S3_CHECK_MODE", "recursive"),
    split_passes=True,
)
input_checks = build_s3_input_asset_checks(
    assets=input_assets,
    table_map=input_table_map,
    check_mode=os.getenv("S3_CHECK_MODE", "recursive"),
)
input_sensor = build_s3_input_observation_sensor(
    name="s3_input_observation",
    assets=input_assets,
    table_map=input_table_map,
    check_mode=os.getenv("S3_CHECK_MODE", "recursive"),
    mics=_available_mics()[:5],
)
input_sync_job = build_s3_input_sync_job(
    name="s3_input_sync",
    assets=input_assets,
    table_map=input_table_map,
    check_mode=os.getenv("S3_CHECK_MODE", "recursive"),
)
input_sync_asset = build_s3_input_sync_asset(
    name="s3_input_sync_by_date",
    assets=input_assets,
    table_map=input_table_map,
    date_partitions_def=daily_partitions,
    check_mode=os.getenv("S3_CHECK_MODE", "recursive"),
    group_name="BMLL",
    use_db_bulk=True,
    repo_root=os.getenv("BMLL_REPO_ROOT"),
    asset_key_prefix=["BMLL"],
)

def _load_demo_config():
    cfg_path = os.path.join(os.path.dirname(__file__), "definitions.yaml")
    if not os.path.exists(cfg_path):
        return {}
    with open(cfg_path, "r") as fh:
        return yaml.safe_load(fh) or {}

_cfg = _load_demo_config()
_cron = _cfg.get("S3_SYNC_CRON")

if _cron:
    @schedule(cron_schedule=_cron, job=input_sync_job, execution_timezone="UTC")
    def s3_sync_schedule():
        return {}
    sync_schedule = s3_sync_schedule
else:
    sync_schedule = None

defs = Definitions(
    assets=[*demo_assets, *input_assets, input_sync_asset],
    asset_checks=[*demo_checks, *input_checks],
    sensors=[input_sensor],
    jobs=[input_sync_job],
    schedules=[*demo_schedules, *([sync_schedule] if sync_schedule else [])],
)
PY

if [ ! -f "${PROJECT_DIR}/definitions.yaml" ]; then
cat > "${PROJECT_DIR}/definitions.yaml" <<'YAML'
# Optional config for dagster demo definitions
# Example:
# S3_SYNC_CRON: "0 2 * * *"
YAML
fi

echo "Starting Dagster UI..."
dagster dev -f "${DEFINITIONS_FILE}"
