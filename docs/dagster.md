# Dagster Compatibility

This project includes a lightweight Dagster compatibility layer in
`intraday_analytics/dagster_compat.py`. It provides a partition model
and a `run_partition` helper that can be wrapped by Dagster assets/jobs.

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

```python
CONFIG_YAML_PRECEDENCE = "yaml_overrides"  # or "python_overrides"
```

YAML can be either:
- A flat dict of config keys
- Or wrapped as `USER_CONFIG: {...}`

## Config UI (Terminal/Web)

Launch a schema-driven UI editor for YAML configs:

```bash
python -m intraday_analytics.config_ui demo/01_ohlcv_bars.py
python -m intraday_analytics.config_ui --web demo/01_ohlcv_bars.py
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

## Artifact Handling

Use the `on_result` callback in `run_partition` to integrate with Dagster's
asset store or metadata. The callback receives the partition and the resolved
`AnalyticsConfig`, so it can compute output paths and register artifacts
per partition.

## Demo Discovery

If you want to expose all demo scripts as Dagster assets, use
`build_demo_assets`:

```python
from dagster import Definitions
from intraday_analytics.dagster_compat import build_demo_assets

defs = Definitions(assets=build_demo_assets())
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
from intraday_analytics.dagster_compat import build_demo_assets, build_demo_schedules

defs = Definitions(
    assets=build_demo_assets(split_passes=True),
    schedules=build_demo_schedules(split_passes=True),
)
```

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
