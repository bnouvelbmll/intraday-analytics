# BMLL Basalt MCP App Menu

This file summarizes what the MCP server currently provides.

## Manage Runs
- `run_job`: run a pipeline through `direct`, `bmll`, `ec2`, `k8s`, or `dagster`.
- `recent_runs`: list recent runs by executor.
- `success_rate`: compute recent run success/failure rates.
- `materialized_partitions`: inspect Dagster asset materialization partitions.

## Manage Experiments
- `optimize_run`: launch optimization sweeps and trials.
- `optimize_summary`: read optimization summaries and trial metadata.

## Adapt Pipeline Config
- `configure_job`: update executor defaults/scheduling config (BMLL jobs, Dagster).

## Manage Pipelines
- `capabilities`: discover installed runtime capabilities and optional integrations.
- `list_plugins`: inspect discovered Basalt plugins and their exposed interfaces.

## Describe Analytics
- `list_metrics`: list known analytics/metrics docs (module, pattern, unit, definition).
- `inspect_metric_source`: inspect source snippets for metric implementations/docs.
