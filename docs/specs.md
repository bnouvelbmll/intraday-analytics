# Framework Specs

This document summarizes the key features of the framework and the current test coverage status. “Tested” means there is automated test coverage in `basalt/tests` or `basalt/analytics/tests`. “Partial” means some behavior is covered but not all variants. “Not tested” means no automated coverage yet.

## Core Pipeline

| Feature | Status | Tests / Notes |
| --- | --- | --- |
| Multi-pass pipeline execution | Tested | `test_multi_pass_pipeline.py`, `test_pipeline_integration.py` |
| Per-pass time bucket options | Tested | `test_timebucket_options.py` |
| Config propagation to modules | Tested | `test_config_propagation.py` |
| Schema lock (warn/raise/update) | Tested | `test_schema_lock.py` |
| Quality checks (null rate/range) | Not tested | Manual validation recommended |
| Dense analytics mode | Tested | `test_dense_analytics.py` |

## Analytics Modules

Module-level tests live in `basalt/analytics/tests/` and focus on quant correctness. Framework/pipeline tests live in `basalt/tests/`.

| Module | Status | Tests / Notes |
| --- | --- | --- |
| L2 analytics | Tested | `test_l2_ohlc.py`, `test_l2_volatility.py` |
| L3 analytics | Tested | `test_l3_analytics.py` |
| Trade analytics (TCA) | Tested | `test_tca_metrics.py` |
| Execution analytics | Tested | `test_pass2_analytics.py` |
| Iceberg detection | Tested | `test_components.py` |
| CBBO analytics | Partial | Basic wiring tested; no dedicated module test |
| Characteristics (L3/Trade) | Partial | Basic pipeline tests cover configs |
| Generic analytics | Partial | Covered indirectly via pipeline tests |
| Reaggregate module | Tested | `test_reaggregate_module.py` |
| Alpha101 subset | Partial | Schema coverage only, no numerical validation tests |
| Events module | Tested | `test_events_module.py` |
| Correlation module | Tested | `test_correlation_module.py` |

## IO / Outputs

| Feature | Status | Tests / Notes |
| --- | --- | --- |
| Parquet output | Tested | `test_output_targets.py` |
| Delta output | Partial | Requires `deltalake` installed; not always exercised in CI |
| SQL output (sqlite) | Tested | `test_output_targets.py` |
| Output target path template | Tested | `test_output_targets.py` |
| Partition dedupe on write | Partial | Covered in output tests, not all edge cases |
| Preserve index option | Not tested | New option; needs coverage |

## Dagster Integration

| Feature | Status | Tests / Notes |
| --- | --- | --- |
| Dagster asset generation | Tested | `test_dagster_compat.py` |
| OutputTarget IO manager | Not tested | Manual validation recommended |
| Dagster CLI helpers (`basalt dagster …`) | Not tested | Manual validation recommended |
| Scheduler install/uninstall | Not tested | Manual validation recommended |

## CLI / Tools

| Feature | Status | Tests / Notes |
| --- | --- | --- |
| `basalt pipeline run` | Partial | Pipeline tests cover logic; CLI parsing not covered |
| `basalt analytics list/explain` | Partial | Schema tests cover data; UI behavior not covered |
| Config UI (Textual) | Not tested | Manual validation recommended |
| Schema browser (Textual) | Partial | Data path tested; UI rendering not covered |

## Data Sources / Transforms

| Feature | Status | Tests / Notes |
| --- | --- | --- |
| Aggressive trades transform | Tested | `test_aggressive_trades_transform.py` |
| Market state transform | Tested | `test_marketstate_transform.py` |
| L3 transforms | Tested | `test_l3_transform.py` |

## Gaps / Recommended Next Tests

- OutputTarget index preservation (`preserve_index`/`index_name`).
- Dagster scheduler install/uninstall workflows.
- End-to-end Dagster run with OutputTarget IO manager.
- UI flows in Config UI and Schema Browser.
- Delta Lake writes with partition dedupe and replace modes.
