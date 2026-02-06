from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import os
import polars as pl


@dataclass(frozen=True)
class ArtifactSpec:
    dataset: str
    stage: str
    pass_name: str
    universe: str
    start_date: str
    end_date: str

    @property
    def partition_key(self) -> str:
        if self.start_date == self.end_date:
            return self.start_date
        return f"{self.start_date}_{self.end_date}"


class ArtifactStore:
    """
    Abstract artifact storage interface.
    """

    def build_path(self, spec: ArtifactSpec) -> str:
        raise NotImplementedError

    def write(self, df: pl.DataFrame | pl.LazyFrame, spec: ArtifactSpec) -> str:
        raise NotImplementedError

    def read(self, spec: ArtifactSpec) -> pl.LazyFrame:
        raise NotImplementedError

    def exists(self, spec: ArtifactSpec) -> bool:
        raise NotImplementedError


class PathArtifactStore(ArtifactStore):
    """
    Artifact store backed by a path template.

    If the base_path uses a non-local scheme (e.g., s3a+delta://), write is a no-op
    and only returns the resolved path for external management.
    """

    def __init__(self, base_path: str):
        self.base_path = base_path.rstrip("/")

    def build_path(self, spec: ArtifactSpec) -> str:
        return (
            f"{self.base_path}/dataset={spec.dataset}"
            f"/stage={spec.stage}"
            f"/pass={spec.pass_name}"
            f"/universe={spec.universe}"
            f"/partition={spec.partition_key}"
            f"/data.parquet"
        )

    def _is_external(self) -> bool:
        return "://" in self.base_path and not self.base_path.startswith("file://")

    def write(self, df: pl.DataFrame | pl.LazyFrame, spec: ArtifactSpec) -> str:
        path = self.build_path(spec)
        if self._is_external():
            return path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        df.write_parquet(path)
        return path

    def read(self, spec: ArtifactSpec) -> pl.LazyFrame:
        path = self.build_path(spec)
        return pl.scan_parquet(path)

    def exists(self, spec: ArtifactSpec) -> bool:
        path = self.build_path(spec)
        if self._is_external():
            return False
        return os.path.exists(path)
