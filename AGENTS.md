# AGENTS.md

## Purpose
Operational recommendations for contributors working on `basalt` core and subpackages.

## Key Principles
- Keep core modular: `basalt` should remain functional without optional subpackages.
- Prefer explicit dataflow: pass-to-pass dependencies should be visible through `module_inputs`.
- Treat time shaping as orchestration, not analytics logic.

## Time And Pass Design
- Use `PassConfig.timeline_mode` to control timeline behavior:
  - `dense`: dense timeline module first, regular joins for analytics.
  - `event`: event timeline module first, `join_asof` for downstream analytics.
  - `sparse_original` / `sparse_digitised`: no implicit time module injection.
- Do not expose `dense`/`events` as regular analytics checkboxes in UI.
- Keep dense/event settings editable only when their mode is selected.

## Input Mapping
- Prefer `module_inputs` over hidden defaults when a module consumes previous pass output.
- UI summaries should display inputs as `(REQ:SOURCE)` pairs.
- For modules with `source_pass`-style fields, keep mappings user-visible in pass summary/editor.

## Packaging
- Keep optional execution backends in optional distributions:
  - `basalt.executors.aws_ec2`
  - `basalt.executors.kubernetes`
- Avoid hard dependency from core import path to optional backends.

## Tests And Quality
- Run full suite before commit: `python3 -m pytest -q`.
- Add targeted tests when changing:
  - timeline/join semantics,
  - config UI behavior,
  - pass/module ordering.
- Keep generated artifacts out of commits (`__pycache__`, `build/`, `dist/`, `.coverage`).

## Known Warnings To Track
- Polars deprecation in `basalt/process.py`:
  - replace `collect(streaming=True)` with `collect(engine="streaming")`.
- Polars may warn about sortedness checks for `join_asof` with `by` groups; this is expected unless pre-validated.
