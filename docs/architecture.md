# Basalt Architecture

This document describes the current architecture of Basalt: execution flow,
module boundaries, and integration points.

`BASALT` stands for `BMLL Advanced Statistical Analytics & Layered Transformations`.

## Core Principles

1. Modularity: analytics/preprocessors are pluggable units selected per pass.
2. Determinism: pass order and bucket semantics are explicit in config.
3. Throughput with bounded memory: batching + shredding + process isolation.
4. Operational flexibility: same pipeline can run local, remote BMLL, or Dagster.

## Main Components

- `basalt/configuration.py`
  - Pydantic config schema (`AnalyticsConfig`, `PassConfig`, module configs).
- `basalt/execution.py`
  - Orchestration of date batches, process pools, and final aggregation.
- `basalt/pipeline.py`
  - Pass/module runner and context propagation across passes.
- `basalt/batching.py`
  - Batching strategies and shredding orchestration.
- `basalt/tables.py`
  - Table abstraction and loaders (`trades`, `l2`, `l3`, `marketstate`, ...).
- `basalt/analytics/` and `basalt/preprocessors/`
  - Analytics/preprocessor implementations.
- `basalt/analytics_base.py`
  - Analytic base abstractions, analytic docs/hints registry, combinatorial config helpers.
- `basalt/basalt.py`
  - CLI entrypoint (`basalt analytics|pipeline|job ...`).
- `basalt/dagster/`
  - Dagster compatibility layer and Dagster-specific tooling.

## Execution Flow

1. Config load + validation
   - Build `AnalyticsConfig`, normalize pass/module settings.
2. Universe resolution
   - `get_universe(date)` resolves listing scope and reference columns.
3. Batching/data preparation
   - `s3_shredding` mode: shred source files into local batch files.
   - `naive` mode: stream directly from scans.
4. Pass execution
   - For each pass, modules run in configured order.
   - Outputs can be consumed by later passes through context keys.
5. Output write
   - Per-pass/global output targets (Parquet/Delta/SQL).
6. Final aggregation + cleanup
   - Aggregate temporary outputs and cleanup according to config.

## Multi-Pass Model

- Each pass has:
  - its own bucket semantics (`time_bucket_seconds`, `time_bucket_anchor`, `time_bucket_closed`)
  - module selection (`modules`)
  - module configs
  - optional pass-specific output target
- Pass outputs are available to downstream passes via context, enabling:
  - preprocess -> analytic chains (for example CBBO preprocess then L2-style analytics)
  - re-aggregation by alternate keys
  - derived indicators over prior outputs

## CLI Surface

- `basalt pipeline run --pipeline <module_or_path> ...`
- `basalt pipeline config <pipeline.py>`
- `basalt analytics list|explain ...`
- `basalt job run|install ...`

CLI extensions are discoverable through entry point group `basalt.cli`.

## Packaging Model

- Core package: `bmll-basalt`
- Optional add-on distributions:
  - `bmll-basalt-dagster`
  - `bmll-basalt-preprocessors`
  - `bmll-basalt-characteristics`
  - `bmll-basalt-alpha101`

Build rotation is controlled by `BASALT_DIST` in `setup.py`.

## Dagster Integration

Dagster support is in `basalt/dagster/` and includes:

- asset generation and partition handling
- BMLL-compatible job launch integration
- visualization helpers in `basalt.dagster.plotting`

See `basalt/dagster/docs/dagster.md` for operational details.
