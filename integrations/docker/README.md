# Docker install test (core only)

This container builds and installs the core package (no extras), then runs the
test suite to verify the project works without optional subpackages like
`dagster`.

Usage:

```bash
docker build -t bmll-basalt-core -f integrations/docker/Dockerfile .
docker run --rm bmll-basalt-core
```
