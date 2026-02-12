# Basalt

Basalt is a modular, multi-pass analytics framework for high-frequency market
data (Trades, L2, L3, MarketState). It is optimized for large universes and
long date ranges via batching, shredding, and process isolation.

`BASALT` stands for `BMLL Advanced Statistical Analytics & Layered Transformations`.

## What It Does

- Runs one or more analytics passes (`PassConfig`) over configurable time buckets.
- Supports preprocessors (for example `cbbo_preprocess`) and postprocessors
  (for example `reaggregate`, `alpha101`).
- Supports local runs, BMLL remote job runs, and Dagster integration.
- Writes to Parquet, Delta, or SQL via output targets.

## Project Layout

- `basalt/`: core framework package and CLI.
- `basalt/dagster/`: Dagster integration and Dagster-specific plotting helpers.
- `basalt/analytics/`: analytics implementations (trade, l2, l3, execution, etc).
- `basalt/preprocessors/`: preprocessors (iceberg, cbbo preprocess, aggressive trades).
- `demo/`: runnable pipeline examples.
- `docs/`: architecture, batching, multi-pass, and reference docs.

## Install

From source (recommended for development):

```bash
pip install -e .
```

Optional extras:

```bash
pip install -e '.[dagster]'
pip install -e '.[all]'
```

## CLI Quick Start

Run a pipeline module:

```bash
basalt pipeline run --pipeline demo/01_ohlcv_bars.py --date 2026-02-01
```

Open schema-driven config UI:

```bash
basalt pipeline config demo/01_ohlcv_bars.py
```

List available analytics columns:

```bash
basalt analytics list --pipeline demo/01_ohlcv_bars.py
```

Explain one analytic column:

```bash
basalt analytics explain --pipeline demo/01_ohlcv_bars.py --column TradeTotalVolume
```

Run on BMLL instance:

```bash
basalt job run --pipeline demo/01_ohlcv_bars.py --instance_size 128
```

## Configuration Model

The main schema is in `basalt/configuration.py`:

- `AnalyticsConfig`: run-level settings.
- `PassConfig`: pass-level settings and module list.
- Module configs under each pass:
  - `trade_analytics`, `l2_analytics`, `l3_analytics`, `execution_analytics`
  - `iceberg_analytics`, `cbbo_analytics`, `cbbo_preprocess`
  - `generic_analytics`, `reaggregate_analytics`
  - `alpha101_analytics`, `event_analytics`, `correlation_analytics`
  - `l3_characteristics_analytics`, `trade_characteristics_analytics`

## Testing

Run tests:

```bash
python3 -m pytest
```

Run scoped coverage (project modules only):

```bash
coverage run --source=basalt,demo,main -m pytest && coverage report -m
```

## Packaging

The default build (`BASALT_DIST=core`) produces `bmll-basalt`.

Build commands (`bdist_wheel`, `bdist`, `sdist`) automatically rotate through:

- `core` -> `bmll-basalt`
- `dagster` -> `bmll-basalt-dagster`
- `preprocessors` -> `bmll-basalt-preprocessors`
- `characteristics` -> `bmll-basalt-characteristics`
- `alpha101` -> `bmll-basalt-alpha101`

These non-core packages are thin metapackages on top of `bmll-basalt`.

## Dagster

Dagster docs and demo are under:

- `basalt/dagster/docs/dagster.md`
- `basalt/dagster/demo/setup_dagster_demo.py`

Dagster plotting utilities are in `basalt.dagster.plotting`.
