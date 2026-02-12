# Package Integration Tests

These checks validate install/run behavior from built wheels in isolated virtual
environments.

## Core Only

Build and install only `bmll-basalt`, then verify:

- no optional `bmll-basalt-*` subpackages are installed
- `basalt` imports and CLI help works

Run:

```bash
python3 integrations/package/test_core_only.py
```

## Core + Subpackages

Build all wheel distributions, install core plus selected subpackages, then
verify module/CLI extension loading.

Run:

```bash
python3 integrations/package/test_with_subpackages.py
```
