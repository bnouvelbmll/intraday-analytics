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

## Dagster UI Partitions

To see partitions in the UI, define a Dagster partitions definition and attach
it to your asset/job. For example:

```python
from dagster import (
    StaticPartitionsDefinition,
    DailyPartitionsDefinition,
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
date_partitions = DailyPartitionsDefinition(start_date="2025-01-02", end_date="2025-01-03")
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
