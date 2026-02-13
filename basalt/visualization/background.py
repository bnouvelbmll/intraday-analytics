from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Optional

import polars as pl


LoaderFn = Callable[[dict[str, Any]], pl.DataFrame]


@dataclass
class LoadResult:
    request_id: str
    status: str
    dataframe: pl.DataFrame | None = None
    error: str | None = None


class BackgroundFrameLoader:
    """
    Runs dataframe loading in a background thread.

    Cancellation is best-effort: when a new request is submitted, previous work is
    marked stale and its result is ignored if it eventually completes.
    """

    def __init__(self, loader: LoaderFn):
        self._loader = loader
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="viz-loader")
        self._future: Future | None = None
        self._request_id: str | None = None
        self._result: LoadResult | None = None

    @property
    def request_id(self) -> str | None:
        return self._request_id

    def submit(self, request_id: str, payload: dict[str, Any]) -> None:
        if self._request_id == request_id and self.is_running():
            return
        self.cancel()
        self._result = None
        self._request_id = request_id

        def _run() -> LoadResult:
            try:
                df = self._loader(payload)
                return LoadResult(request_id=request_id, status="ok", dataframe=df)
            except Exception as exc:
                return LoadResult(
                    request_id=request_id,
                    status="error",
                    error=f"{type(exc).__name__}: {exc}",
                )

        self._future = self._executor.submit(_run)

    def is_running(self) -> bool:
        return self._future is not None and not self._future.done()

    def poll(self) -> Optional[LoadResult]:
        if self._future is None:
            return self._result
        if not self._future.done():
            return self._result
        try:
            result = self._future.result()
        finally:
            self._future = None
        # Drop stale completions if a newer request is active.
        if result.request_id != self._request_id:
            return self._result
        self._result = result
        return self._result

    def cancel(self) -> None:
        if self._future is not None:
            self._future.cancel()
            self._future = None

    def consume_dataframe(self) -> pl.DataFrame | None:
        result = self.poll()
        if result is None or result.status != "ok":
            return None
        df = result.dataframe
        self._result = LoadResult(request_id=result.request_id, status="consumed")
        return df

