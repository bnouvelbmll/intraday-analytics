from __future__ import annotations

import json
import os
import time
import logging
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)

_ENV_PATH = "INTRADAY_API_STATS_PATH"


def init_api_stats(temp_dir: str) -> str | None:
    if not temp_dir:
        return None
    path = os.path.join(temp_dir, "api_stats.jsonl")
    os.environ[_ENV_PATH] = path
    try:
        os.makedirs(temp_dir, exist_ok=True)
    except Exception:
        return None
    return path


def _stats_path() -> str | None:
    return os.environ.get(_ENV_PATH)


@dataclass(frozen=True)
class ApiStat:
    name: str
    duration_s: float
    pid: int
    ts: float
    extra: dict[str, Any] | None = None

    def to_json(self) -> str:
        payload = {
            "name": self.name,
            "duration_s": self.duration_s,
            "pid": self.pid,
            "ts": self.ts,
        }
        if self.extra:
            payload["extra"] = self.extra
        return json.dumps(payload, separators=(",", ":"))


def record_api_timing(
    name: str, duration_s: float, extra: dict[str, Any] | None = None
):
    path = _stats_path()
    if not path:
        return
    try:
        stat = ApiStat(
            name=name,
            duration_s=duration_s,
            pid=os.getpid(),
            ts=time.time(),
            extra=extra,
        )
        with open(path, "a", encoding="utf-8") as f:
            f.write(stat.to_json() + "\n")
    except Exception:
        # Avoid breaking the pipeline if stats logging fails.
        logger.debug("Failed to write api stats", exc_info=True)


def api_call(name: str, fn: Callable[[], Any], extra: dict[str, Any] | None = None):
    start = time.perf_counter()
    try:
        return fn()
    finally:
        record_api_timing(name, time.perf_counter() - start, extra=extra)


def summarize_api_stats(temp_dir: str):
    path = os.path.join(temp_dir, "api_stats.jsonl")
    if not os.path.exists(path):
        return
    totals: dict[str, dict[str, Any]] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                name = row.get("name", "unknown")
                duration = float(row.get("duration_s", 0.0))
                entry = totals.setdefault(
                    name, {"count": 0, "total_s": 0.0, "max_s": 0.0}
                )
                entry["count"] += 1
                entry["total_s"] += duration
                if duration > entry["max_s"]:
                    entry["max_s"] = duration
    except Exception:
        logger.debug("Failed to summarize api stats", exc_info=True)
        return

    if not totals:
        return
    logger.info("API timing summary (all workers):")
    for name, entry in sorted(
        totals.items(), key=lambda kv: kv[1]["total_s"], reverse=True
    ):
        avg = entry["total_s"] / entry["count"] if entry["count"] else 0.0
        logger.info(
            f"- {name}: calls={entry['count']}, total={entry['total_s']:.3f}s, avg={avg:.3f}s, max={entry['max_s']:.3f}s"
        )
