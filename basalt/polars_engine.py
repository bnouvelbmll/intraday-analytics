from __future__ import annotations

from functools import lru_cache
import logging
import shutil
import subprocess
from typing import Any

import polars as pl


logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def has_nvidia_gpu() -> bool:
    """Best-effort NVIDIA GPU detection via `nvidia-smi`."""
    if shutil.which("nvidia-smi") is None:
        return False
    try:
        proc = subprocess.run(
            ["nvidia-smi", "-L"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except Exception:
        return False
    return proc.returncode == 0 and bool(proc.stdout.strip())


@lru_cache(maxsize=1)
def build_gpu_engine() -> Any | None:
    """
    Build a Polars GPU engine using RMM managed pooled memory.

    Returns None when dependencies are unavailable.
    """
    try:
        import rmm

        mr = rmm.mr.PrefetchResourceAdaptor(
            rmm.mr.PoolMemoryResource(rmm.mr.ManagedMemoryResource())
        )
        return pl.GPUEngine(memory_resource=mr)
    except Exception as exc:
        logger.debug("Failed to initialize Polars GPU engine: %s", exc)
        return None


def should_use_gpu(config) -> bool:
    """
    Resolve whether GPU collect should be used.

    - True: force GPU
    - False: force CPU
    - None: auto-detect NVIDIA host
    """
    pref = getattr(config, "POLARS_GPU_ENABLED", None) if config is not None else None
    if pref is True:
        return True
    if pref is False:
        return False
    return has_nvidia_gpu()


def collect_lazy(
    lf: pl.LazyFrame,
    *,
    config=None,
    streaming: bool | None = None,
) -> pl.DataFrame:
    """
    Collect helper with optional GPU acceleration and CPU fallback.
    """
    if should_use_gpu(config):
        engine = build_gpu_engine()
        if engine is not None:
            return lf.collect(engine=engine)
        logger.warning(
            "POLARS_GPU_ENABLED requested/auto-detected but GPU engine is unavailable; falling back to CPU."
        )
    if streaming is True:
        return lf.collect(engine="streaming")
    if streaming is None or streaming is False:
        return lf.collect()
    return lf.collect()
