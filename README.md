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
- `basalt/mcp/`: MCP server and shared run-configuration/monitoring APIs.
- `basalt/optimize/`: experiment search and parameter optimization helpers.
- `basalt/objective_functions/`: reusable model-evaluation objective functions.
- `basalt/models/`: model adapters/training utilities (sklearn/autogluon/pymc).
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
pip install -e '.[optimize]'
pip install -e '.[objective_functions]'
pip install -e '.[models]'
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
  - `iceberg_analytics`, `cbbo_analytics`, `cbbo_preprocess`, `cbbofroml3_preprocess`
  - `generic_analytics`, `reaggregate_analytics`
  - `alpha101_analytics`, `observed_events_analytics`, `external_event_analytics`, `correlation_analytics`
  - `l3_characteristics_analytics`, `trade_characteristics_analytics`

`cbbofroml3_preprocess` time settings:
- `time_index="event"`: event-indexed output; `time_bucket_seconds` is ignored.
- `time_index="timebucket"`: bucketed output; `time_bucket_seconds` is required and used.

## Testing

Run tests:

```bash
python3 -m pytest
```

Run scoped coverage (project modules only):

```bash
coverage run --source=basalt,demo,main -m pytest && coverage report -m
```

Run coverage by package/subpackage:

```bash
python3 scripts/coverage_by_package.py
```

Reuse existing `.coverage` data (no test rerun):

```bash
python3 scripts/coverage_by_package.py --skip-tests
```

Benchmark run performance (single executor per run):

```bash
python3 scripts/performance_benchmark.py \
  --pipeline demo/01_ohlcv_bars.py \
  --executors bmll \
  --instance-sizes 16,32,64 \
  --repeats 2 \
  --output-dir benchmark_results
```

Executors: `direct`, `dagster`, `bmll`, `ec2`, `kubernetes`.
This writes per-run logs (`jsonl`, `csv`) and an aggregated summary (`csv`) with
duration percentiles and success rates.

Visualize latest benchmark summary:

```bash
python3 scripts/visualize_benchmark_results.py --results-dir benchmark_results
```

Validate DB<->Pascal name conversion against an external schema catalog:

```bash
python3 scripts/validate_schema_name_mapping.py --schema-dir /path/to/schema-catalog
```

## Packaging

The default build (`BASALT_DIST=core`) produces `bmll-basalt`.

Build commands (`bdist_wheel`, `bdist`, `sdist`) automatically rotate through:

- `core` -> `bmll-basalt`
- `dagster` -> `bmll-basalt-dagster`
- `preprocessors` -> `bmll-basalt-preprocessors`
- `characteristics` -> `bmll-basalt-characteristics`
- `alpha101` -> `bmll-basalt-alpha101`
- `talib` -> `bmll-basalt-talib`
- `mcp` -> `bmll-basalt-mcp`
- `optimize` -> `bmll-basalt-optimize`
- `objective_functions` -> `bmll-basalt-objective-functions`
- `models` -> `bmll-basalt-models`

These non-core packages are standalone subpackage wheels that depend on
`bmll-basalt`.

MCP package:

- Install `bmll-basalt-mcp` to enable `basalt mcp ...` CLI commands and MCP server tools.
- Start server (stdio transport by default): `basalt mcp serve`.
- The same capabilities are available via CLI: `configure`, `run`, `recent_runs`, `success_rate`, `materialized_partitions`.

TA-Lib support is modular:

- Core package excludes `basalt.analytics.talib`.
- Install `bmll-basalt-talib` to enable TA-Lib indicator metadata wiring in the config UI.
- TA-Lib indicator parameters are configured with `talib_indicators[].parameters` (JSON object).

Optimize package is modular:

- Install `bmll-basalt-optimize` to enable `basalt optimize run ...`.
- `basalt optimize run` performs search-space driven configuration optimization and writes `trials.jsonl` + `summary.json`.
- Executor mode: `--executor direct|bmll|ec2` currently supports trial-specific config dispatch.
- `direct` computes scores (`--score_fn module:function` required).
- `bmll`/`ec2` submit distributed trials and log submission ids; scoring is not computed in-process.

Objective-functions package is modular:

- Install `bmll-basalt-objective-functions` for reusable evaluation utilities.
- Main classes: `ModelObjectiveEvaluator`, `DatasetSplit`, and objectives such as `DirectionalAccuracyObjective`, `MeanAbsoluteErrorObjective`, `MeanSquaredErrorObjective`.
- Use `make_optimization_score_fn(...)` to plug evaluator outputs into `basalt optimize` score functions.
- Uncertainty-aware models are supported through:
  - `predict_interval(X) -> (pred, lower, upper)` or dict payload.
  - `predict_with_confidence(X) -> (pred, confidence)` or dict payload.
  - `predict(X)` returning dict with `pred`/`predictions` plus optional `lower`, `upper`, `confidence`.

Models package is modular:

- Install `bmll-basalt-models` for training/serialization adapters.
- Included adapters:
  - `SklearnModel` (generic sklearn-style estimator wrapper)
  - `AutoGluonTabularModel`
  - `PyMCModel` (hook-based adapter for user-defined PyMC training/predict functions)
- Use `train_model(...)` and `evaluate_model_with_objectives(...)` for the direct bridge to objective functions.
- Use `make_model_objective_score_fn(...)` to connect models + objective-functions into `basalt optimize`.

Integration checks for packaging are in `integrations/package/README.md`.

## Dagster

Dagster docs and demo are under:

- `basalt/dagster/docs/dagster.md`
- `basalt/dagster/demo/setup_dagster_demo.py`

Dagster plotting utilities are in `basalt.dagster.plotting`.
