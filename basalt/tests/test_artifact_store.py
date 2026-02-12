from __future__ import annotations

import polars as pl

from basalt.artifact_store import ArtifactSpec, PathArtifactStore


def _spec(**kwargs) -> ArtifactSpec:
    base = dict(
        dataset="l2",
        stage="pre",
        pass_name="pass1",
        universe="cac40",
        start_date="2025-01-02",
        end_date="2025-01-02",
    )
    base.update(kwargs)
    return ArtifactSpec(**base)


def test_partition_key_single_date():
    spec = _spec()
    assert spec.partition_key == "2025-01-02"


def test_partition_key_date_range():
    spec = _spec(end_date="2025-01-03")
    assert spec.partition_key == "2025-01-02_2025-01-03"


def test_path_artifact_store_local_roundtrip_dataframe(tmp_path):
    store = PathArtifactStore(str(tmp_path))
    spec = _spec()
    df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]})

    path = store.write(df, spec)

    assert path == store.build_path(spec)
    assert store.exists(spec)
    loaded = store.read(spec).collect()
    assert loaded.to_dict(as_series=False) == df.to_dict(as_series=False)


def test_path_artifact_store_local_roundtrip_lazyframe(tmp_path):
    store = PathArtifactStore(str(tmp_path))
    spec = _spec(pass_name="pass2")
    df = pl.DataFrame({"x": [10]}).lazy()

    path = store.write(df, spec)

    assert path.endswith("/data.parquet")
    loaded = store.read(spec).collect()
    assert loaded.to_dict(as_series=False) == {"x": [10]}


def test_path_artifact_store_external_mode_skips_write_and_exists_false(tmp_path):
    store = PathArtifactStore("s3://bucket/prefix")
    spec = _spec()
    path = store.write(pl.DataFrame({"x": [1]}), spec)

    assert path.startswith("s3://bucket/prefix/")
    assert store.exists(spec) is False
