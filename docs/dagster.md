# Dagster Compatibility

This project includes a lightweight Dagster compatibility layer in
`intraday_analytics/dagster_compat.py`. It provides a partition model
and a `run_partition` helper that can be wrapped by Dagster assets/jobs.

## What's new (highlights)

- New `beaf` CLI (`beaf pipeline run`, `beaf pipeline config`, `beaf job run`, `beaf dagster run`).
- BMLL instance jobs for remote execution (with automatic bootstrap).
- Optional Dagster run launcher backed by BMLL instances.
- Per-run tags for instance size/conda env/concurrency.
- YAML + schema-driven config UI for any demo or pipeline.

## Concepts

- **Universe partition**: a selector that resolves to a `get_universe` function.
- **Date partition**: a day or range of days.
- **Partition run**: the product of (universe x date range).

The framework exposes:

- `UniversePartition(name, value)`
- `DatePartition(start_date, end_date)`
- `PartitionRun(universe, dates)`
- `build_partition_runs(universes, dates)`
- `run_partition(...)`

New universe helpers:
- `MICUniverse(whitelist=..., blacklist=...)`
- `IndexUniverse(values=[...])`
- `OPOLUniverse(values=[...])`
- `CustomUniverse(get_universe, name=..., value=...)`
- `CartProdUniverse(left, right)`
- `build_universe_partitions(universes)`
- `default_universes()`

## How Dagster Works Here (Short Version)

- `build_assets(pkg=...)` discovers pipeline modules and exposes them as Dagster assets.
- Each asset runs `run_partition(...)` under the hood.
- Partitions are typically `(universe x date)`, with partition keys like `mic=XLON`.
- Schedules come from `AnalyticsConfig.SCHEDULES` (cron + timezone + optional partitions).
- Optional: `BMLLRunLauncher` runs each Dagster run on a BMLL EC2 instance.

## Example: Single MIC, Single Day

```python
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

    def on_result(partition, config):
        # Hook to upload to the Dagster data layer or attach metadata.
        # Example: record the output path per partition.
        pass

    run_partition(
        base_config=BASE_CONFIG,
        default_get_universe=default_get_universe,
        partition=partition,
        on_result=on_result,
    )

defs = Definitions(assets=[intraday_partition])
```

## Example: Multiple MICs

`UniversePartition` can be used to fan out across multiple MICs. Use
`build_partition_runs` to get all (MIC x date) pairs.

```python
from intraday_analytics.dagster_compat import (
    UniversePartition,
    DatePartition,
    build_partition_runs,
    run_partition,
)

universes = [
    UniversePartition(name="mic", value="XLON"),
    UniversePartition(name="mic", value="XPAR"),
]
dates = [DatePartition("2025-01-02", "2025-01-02")]
partition_runs = build_partition_runs(universes, dates)

for partition in partition_runs:
    run_partition(base_config=BASE_CONFIG, default_get_universe=default_get_universe, partition=partition)
```

## Universe Overrides

`UniversePartition(name="mic", value="XLON")` maps to the CLI-style
`--universe mic=XLON` and will call
`intraday_analytics/universes/mic.py:get_universe(date, "XLON")`.

Available universe modules can be added under `intraday_analytics/universes/`.
Each module must expose:

```python
def get_universe(date, value):
    ...
```

## Structured Universes (Preferred)

Instead of hand-assembling `UniversePartition` lists, you can use structured
universe classes:

```python
from dagster import StaticPartitionsDefinition, MultiPartitionsDefinition
from intraday_analytics.dagster_compat import (
    MICUniverse, IndexUniverse, CustomUniverse, CartProdUniverse,
    build_universe_partitions,
)

def my_custom_universe(date):
    ...

universes = [
    MICUniverse(blacklist=["BATE"]),
    IndexUniverse(["STOXX600"]),
    CartProdUniverse(CustomUniverse(my_custom_universe), MICUniverse()),
]
universe_partitions = StaticPartitionsDefinition(build_universe_partitions(universes))
partitions_def = MultiPartitionsDefinition({"universe": universe_partitions, "date": date_partitions})
```

`CartProdUniverse` produces partitions like `mic=XLON+custom` and intersects the
universe frames at runtime.

## YAML Config Support

Every `demo/*.py` and `main.py` can now read an optional YAML file with the same
base name. The merge precedence is controlled inside the Python module using:

YAML can be either:
- A flat dict of config keys
- Or wrapped as `USER_CONFIG: {...}`

## Config UI (Terminal/Web)

Launch a schema-driven UI editor for YAML configs:

```bash
beaf pipeline config demo/01_ohlcv_bars.py
beaf pipeline config --web demo/01_ohlcv_bars.py
```

This creates or edits `demo/01_ohlcv_bars.yaml`. The UI mirrors the
`AnalyticsConfig` / `PassConfig` Pydantic schema and constrains enums and
types where possible.

## DB Sync and UI Cache

The DB bulk sync path now clears cached asset status after each table sync so
the UI recomputes partition counts and materialization badges. If you bulk
insert events outside the API, you must either:
- Call `wipe_asset_cached_status(...)`
- Or rebuild the cached status via `get_and_update_asset_status_cache_value(...)`

## Scheduler + UI Separation (Remote Daemon)

For production-like setups, run the **Dagster daemon** on a remote BMLL instance
and keep the **webserver/UI** local. The daemon is responsible for schedules and
sensors; the UI can be started independently.

Requirements:
- Provide either a Dagster **workspace** (`workspace.yaml`) or a **pipeline file**
  to the daemon and webserver.
- Ensure `DAGSTER_HOME` is set consistently for both (shared event log storage).

Example: schedule daemon every 6 hours on a 16GB instance for 1 hour:

```bash
beaf dagster scheduler install --pipeline demo/01_ohlcv_bars.py --interval_hours 6 --instance_size 16 --max_runtime_hours 1
```

Start the UI without the daemon:

```bash
beaf dagster ui --pipeline demo/01_ohlcv_bars.py --port 3000
```

## Bulk Sync (Materializations/Observations)

You can force Dagster to ingest materializations/observations from S3 using the
bulk sync job (direct event-log insertions):

```bash
beaf dagster sync --tables l2,l3,trades --start_date 2025-01-01 --end_date 2025-01-31
```

To sync a **specific S3 path** (e.g. after a manual write):

```bash
beaf dagster sync --paths s3://bucket/prefix/data/.../2025-01-05.parquet
```

## Artifact Handling

Use the `on_result` callback in `run_partition` to integrate with Dagster's
asset store or metadata. The callback receives the partition and the resolved
`AnalyticsConfig`, so it can compute output paths and register artifacts
per partition.

## OutputTarget IO Manager (Default)

The compatibility layer now defaults assets to the `output_target_io_manager`
so Dagster can transparently read/write outputs using `OutputTarget`
(parquet/delta/sql).

Add the IO manager to your Definitions:

```python
from dagster import Definitions
from intraday_analytics.dagster_io import output_target_io_manager

defs = Definitions(
    assets=build_assets(...),
    resources={
        "output_target_io_manager": output_target_io_manager.configured(
            {
                "base_config": USER_CONFIG,
                # optional override:
                # "output_target": {"type": "delta", "path_template": "..."},
            }
        )
    },
)
```

If you want to use a different IO manager, set `OUTPUT_TARGET.io_manager_key`
in your config.

## Demo Discovery

If you want to expose all demo scripts as Dagster assets, use
`build_assets`:

```python
from dagster import Definitions
from intraday_analytics.dagster_compat import build_assets

defs = Definitions(assets=build_assets(pkg="demo"))
```

## Schedules

Demo schedules are driven by `AnalyticsConfig.SCHEDULES`. Each demo module can
enable one or more schedules with cron, timezone, and partition selectors. If
no partitions are provided, the schedule defaults to yesterday plus the first
universe in `UNIVERSES`.

Example (`demo/01_ohlcv_bars.yaml`):
```yaml
USER_CONFIG:
  SCHEDULES:
    - name: daily_demo
      enabled: true
      cron: "0 2 * * *"
      timezone: "Europe/London"
      partitions:
        - date: yesterday
          universe: mic=XLON
```

To register schedules in Dagster, include them in your Definitions:
```python
from dagster import Definitions
from intraday_analytics.dagster_compat import build_assets, build_schedules

defs = Definitions(
    assets=build_assets(pkg="demo", split_passes=True),
    schedules=build_schedules(pkg="demo", split_passes=True),
)
```

## New CLI: beaf

The `beaf` CLI provides a consistent interface for analytics, config, jobs, and Dagster:

```bash
beaf analytics list
beaf pipeline config demo/01_ohlcv_bars.py
beaf pipeline run --pipeline demo/01_ohlcv_bars.py --date 2026-02-01
```

### Remote BMLL Jobs

Run a pipeline on a dedicated BMLL instance:

```bash
beaf job run --pipeline demo/01_ohlcv_bars.py --date 2026-02-01 --instance_size 64
```

Install a cron-triggered job:

```bash
beaf job install --pipeline demo/01_ohlcv_bars.py --cron "0 6 ? * MON-FRI *"
```

Jobs default their logs under:
`/home/bmll/user/intraday-metrics/_bmll_jobs/<job_id>/logs`

Cron notes:
- Dagster uses 5-field cron (`min hour dom mon dow`).
- BMLL/AWS uses 6-field cron (`min hour dom mon dow year`) with `?` for dom/dow.
- We accept Dagster cron in config and convert automatically for BMLL jobs.

### Dagster Run (CLI)

Run a pipeline directly via Dagster helpers:

```bash
beaf dagster run --pipeline demo/01_ohlcv_bars.py \
  --job demo_ohlcv_1min__job \
  --partition "date=2026-02-01,universe=mic=XLON"
```

If no partition is provided, defaults come from `START_DATE` and the first `UNIVERSE`.

### Dagster Schedule Install

Update the pipeline YAML to enable schedules and auto-materialization:

```bash
beaf dagster install --pipeline demo/01_ohlcv_bars.py \
  --cron "0 2 * * *" --timezone "Europe/London" --auto_materialize_latest_days 7
```

Remove the schedule (and optionally disable auto-materialization):

```bash
beaf dagster uninstall --pipeline demo/01_ohlcv_bars.py
```

## Dagster Run Launcher (BMLL Instances)

To run Dagster jobs on BMLL EC2 instances, configure the run launcher:

```yaml
run_launcher:
  module: intraday_analytics.dagster_bmll
  class: BMLLRunLauncher
```

Default instance settings come from `AnalyticsConfig.BMLL_JOBS`. You can override
them per-run using Dagster run tags:

- `bmll/instance_size`
- `bmll/conda_env`
- `bmll/max_runtime_hours`
- `bmll/max_concurrent_instances`
- `bmll/log_path`

Disable by removing the launcher configuration.

## Enable / Disable Dagster Integration

Enable:
- Define `Definitions` with `build_assets(...)` (and optionally `build_schedules(...)`).
- Start Dagster with that code location.

Disable:
- Don’t register the assets/schedules, or remove the `run_launcher` block from `dagster.yaml`.

## Dagster UI Partitions

To see partitions in the UI, define a Dagster partitions definition and attach
it to your asset/job. For example:

```python
from dagster import (
    StaticPartitionsDefinition,
    MultiPartitionsDefinition,
    asset,
)
from intraday_analytics.dagster_compat import (
    DatePartition,
    PartitionRun,
    parse_universe_spec,
    run_partition,
)

universe_partitions = StaticPartitionsDefinition(["mic=XLON", "mic=XPAR"])
date_partitions = StaticPartitionsDefinition(["2025-01-02", "2025-01-03_2025-01-09"])
partitions_def = MultiPartitionsDefinition({"universe": universe_partitions, "date": date_partitions})

@asset(name="demo06_characteristics", partitions_def=partitions_def)
def demo06_characteristics(context):
    keys = context.partition_key.keys_by_dimension
    partition = PartitionRun(
        universe=parse_universe_spec(keys["universe"]),
        dates=DatePartition(start_date=keys["date"], end_date=keys["date"]),
    )
    run_partition(base_config=BASE_CONFIG, default_get_universe=default_get_universe, partition=partition)

defs = Definitions(assets=[demo06_characteristics])
```

## Setup Script

The helper script `scripts/setup_dagster_demo.sh`:
- sets `DAGSTER_HOME=/home/bmll/user/my-dagster`
- builds universe partitions from `bmll.reference.available_markets()` (Equity + IsAlive)
- builds date partitions from `2015-01-01` to yesterday
- honors `BATCH_FREQ` (environment variable) to align with CLI batching

Example:
```bash
BATCH_FREQ=W ./scripts/setup_dagster_demo.sh
```
