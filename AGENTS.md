# AGENTS.md

## Purpose
Operational recommendations for contributors working on `basalt` core and subpackages.

## Key Principlese
- Keep core modular: `basalt` should remain functional without optional subpackages. Documentations, tests and configs should live with each subpackages - the main package should remain agnostic of plugins.
- Treat time modules (dense, external_events) shaping as orchestration, not analytics logic.
- When structuring the frameworks - try to think in terms of generic concept that could be extended (analytics, executors, config, batcher, tables...)
- The framework is designed to make thinkg simple. Make sure UI and docs allow configuring correctly and simply the modules that you are adding. lWhen you add new config modules make sure that only revelant options are displayed when some parameters depends on previous choices
- Prefer explicit dataflows: make pass-to-pass dependencies should be visible through `module_inputs`.
- Every `*Config` model must include:
  - a class docstring explaining purpose and usage context,
  - field-level `description=` for every field (including toggles and enum/literal choices),
  - guidance text that helps users interpret allowed values and defaults.
- Every analytic exposed to the explorer must publish complete docs metadata:
  - regex pattern, unit, definition template, and non-empty description,
  - use `@analytic_expression(..., unit=...)` (preferred) + docstring or `AnalyticDoc(...)`,
  - avoid undocumented outputs because schema explorer and explain tools rely on this metadata.
  - Keep config data driven.
- Keep coverage above 85% on all core modules
- Document verbosely - Be descriptive, not prescriptive - the doc should give enough information for the user to understand what the code does - do not assume that the reader of the doc can see the docs. Each docstring should be self-sufficient - you don't want to read the entire doc to make sense of the docs. 
- When you do commits remember to update this document if general principle is identifies or general docs and specs.

## Time And Pass Design
- Use `PassConfig.timeline_mode` to control timeline behavior:
  - `dense`: dense timeline module first, regular joins for analytics. Allowing dense time output.
  - `event`: event timeline module first, `join_asof` for downstream analytics.
  - `sparse_original` / `sparse_digitised`: no implicit time module injection.
- Do not expose `dense`/`events` as regular analytics checkboxes in UI.
- Keep dense/event settings editable only when their mode is selected.

## Input Mapping
- Prefer `module_inputs` over hidden defaults when a module consumes previous pass output.
- UI summaries should display inputs as `(REQ:SOURCE)` pairs.
- For modules with `source_pass`-style fields, keep mappings user-visible in pass summary/editor.

## Packaging
- Keep optional execution backends in optional distributions.
- Avoid hard dependency from core import path to optional backends.

## Tests And Quality
- Run full suite before commit: `python3 -m pytest` (use `-q` when you want quieter output).
- Add targeted tests when changing:
  - timeline/join semantics,
  - config UI behavior,
  - pass/module ordering.
- Keep generated artifacts out of commits (`__pycache__`, `build/`, `dist/`, `.coverage`). Add them to .gitgnore

## Known Warnings To Track
- Polars deprecation in `basalt/process.py`:
  - replace `collect(streaming=True)` with `collect(engine="streaming")`.
- Polars may warn about sortedness checks for `join_asof` with `by` groups; this is expected unless pre-validated.
