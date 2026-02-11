# Analytics Tests

This folder contains module-level analytics tests. These are intended for quantitative validation of metric behavior and correctness for a specific analytics module.

Guidelines:
- Keep tests focused on a single module (L2/L3/Trade/Execution/etc.).
- Use small, explicit fixtures so expected values are easy to verify.
- Avoid end-to-end pipeline concerns; those belong to `intraday_analytics/tests/`.

Framework-level tests (pipeline execution, IO targets, config wiring, Dagster integration, etc.) remain in `intraday_analytics/tests/`.
