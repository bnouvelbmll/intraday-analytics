# Framework Specs And Coverage Snapshot

This file tracks feature status and automated test coverage at a high level.

Legend:
- `Tested`: explicit automated tests exist.
- `Partial`: some behavior covered, variants/edge cases still open.
- `Not tested`: no dedicated automated coverage yet.

## Core Pipeline

| Feature | Status | Tests / Notes |
| --- | --- | --- |
| Multi-pass execution | Tested | `test_multi_pass_pipeline.py`, `test_pipeline_integration.py` |
| Per-pass bucket options | Tested | `test_timebucket_options.py` |
| Config propagation to module configs | Tested | `test_config_propagation.py` |
| Schema lock behavior | Tested | `test_schema_lock.py` |
| Quality checks (null/range + warn/raise) | Tested | `test_quality_checks.py` |
| Dense analytics mode | Tested | `test_dense_analytics.py` |

## Analytics / Preprocessors

| Module | Status | Tests / Notes |
| --- | --- | --- |
| L2 analytics | Tested | `test_l2_ohlc.py`, `test_l2_volatility.py` |
| L3 analytics | Tested | `test_l3_analytics.py` |
| Trade analytics | Tested | `test_tca_metrics.py`, pass integration tests |
| Execution analytics | Tested | `test_pass2_analytics.py` |
| Iceberg preprocess/analytics | Partial | covered by `test_components.py`; deeper numeric tests open |
| CBBO preprocess | Tested | `test_cbbo_preprocess.py` |
| CBBO analytics | Partial | integrated behavior covered, limited dedicated edge tests |
| Reaggregate | Tested | `test_reaggregate_module.py` |
| Events | Tested | `test_events_module.py` |
| Correlation | Tested | `test_correlation_module.py` |
| Characteristics (L3/trade) | Partial | schema/pipeline wiring covered, limited analytic validation |
| Alpha101 subset | Partial | wiring/schema covered, limited numeric validation |
| Aggressive trades transform | Tested | `test_aggressive_trades_transform.py` |
| Market state/L3 transforms | Tested | `test_marketstate_transform.py`, `test_l3_transform.py` |

## IO / Outputs

| Feature | Status | Tests / Notes |
| --- | --- | --- |
| Parquet output | Tested | `test_output_targets.py` |
| SQL output (sqlite path) | Tested | `test_output_targets.py` |
| Delta output | Partial | depends on optional runtime (`deltalake`) |
| Path templating | Tested | `test_output_targets.py` |
| Dedupe on partition write | Partial | covered for common flows, edge cases open |
| Preserve index options | Partial | exercised indirectly, no dedicated matrix |

## CLI / UX

| Feature | Status | Tests / Notes |
| --- | --- | --- |
| `basalt pipeline run` logic | Partial | mostly covered through pipeline tests |
| `basalt analytics list/explain` data path | Partial | schema utilities tested; CLI parsing/UI flow open |
| Config UI helpers | Tested | `test_config_ui_utils.py` |
| Full Textual UI behavior | Not tested | manual validation currently required |

## Dagster

| Feature | Status | Tests / Notes |
| --- | --- | --- |
| Dagster compatibility core | Tested | `basalt/dagster/tests/test_dagster_compat.py` |
| Dagster CLI extension behavior | Partial | no dedicated CLI-level suite |
| Scheduler install/uninstall workflows | Not tested | manual validation recommended |
| Dagster plotting helpers | Not tested | manual checks only |

## Priority Gaps

1. Numeric validation tests for `alpha101` formulas.
2. Expanded `iceberg` and `cbbo` edge-case scenarios.
3. Dedicated CLI argument/UX tests for `basalt` commands.
4. End-to-end Dagster operational tests (scheduler + IO manager).
