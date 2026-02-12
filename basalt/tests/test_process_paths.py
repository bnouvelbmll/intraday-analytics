from __future__ import annotations

import datetime as dt
import os
import sys
import types

import polars as pl
import pytest

from basalt.configuration import (
    AnalyticsConfig,
    OutputTarget,
    OutputType,
    PassConfig,
    QualityCheckConfig,
)
from basalt.process import (
    BatchWriter,
    aggregate_and_write_final_output,
    get_final_output_path,
    get_final_s3_path,
)


def _write_batch(temp_dir, *, with_partition_cols=False):
    data = {
        "ListingId": [1],
        "TimeBucket": [dt.datetime(2025, 1, 1, 10, 0)],
        "Metric": [1.23],
    }
    if with_partition_cols:
        data["MIC"] = ["XPAR"]
        data["Date"] = [dt.date(2025, 1, 1)]
    path = os.path.join(temp_dir, "batch-metrics-0001.parquet")
    pl.DataFrame(data).write_parquet(path)
    return path


def _cfg(output_target: OutputTarget) -> AnalyticsConfig:
    return AnalyticsConfig(
        DATASETNAME="ds",
        UNIVERSE="mic=XPAR",
        OUTPUT_TARGET=output_target,
        QUALITY_CHECKS=QualityCheckConfig(ENABLED=False),
        S3_STORAGE_OPTIONS={"region": "us-east-1"},
    )


def _pass(output: OutputTarget | None = None) -> PassConfig:
    return PassConfig(name="pass1", modules=["trade"], output=output)


def test_get_final_output_path_fallbacks_and_normalization(monkeypatch):
    # storage_paths failure should fall back to empty bucket/prefix
    monkeypatch.setitem(
        sys.modules,
        "bmll2",
        types.SimpleNamespace(storage_paths=lambda: (_ for _ in ()).throw(RuntimeError("x"))),
    )

    ot = OutputTarget(
        type=OutputType.PARQUET,
        path_template="s3://{bucket}//{prefix}//data/{datasetname}/{pass}/{start_date}_{end_date}.parquet",
    )
    cfg = _cfg(ot)

    out = get_final_output_path("2025-01-01", "2025-01-02", cfg, "pass1", ot)
    assert out.startswith("s3://")
    assert "ds_pass1/pass1/2025-01-01_2025-01-02.parquet" in out

    monkeypatch.setitem(
        sys.modules,
        "bmll2",
        types.SimpleNamespace(
            storage_paths=lambda: {"user": {"bucket": "bkt", "prefix": "pfx"}}
        ),
    )
    out2 = get_final_output_path(dt.date(2025, 1, 1), dt.date(2025, 1, 2), cfg, "pass1", ot)
    assert "bkt" in out2
    assert "pfx" in out2


def test_get_final_s3_path_wrapper(monkeypatch):
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))
    ot = OutputTarget(type=OutputType.PARQUET, path_template="x/{datasetname}/{pass}")
    cfg = _cfg(ot)
    p1 = get_final_output_path("2025-01-01", "2025-01-01", cfg, "pass1", ot)
    p2 = get_final_s3_path("2025-01-01", "2025-01-01", cfg, "pass1", ot)
    assert p1 == p2


def test_aggregate_no_batch_files_returns_none(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))
    ot = OutputTarget(
        type=OutputType.PARQUET,
        path_template=str(tmp_path / "out_{datasetname}_{start_date}_{end_date}.parquet"),
    )
    out = aggregate_and_write_final_output(
        dt.date(2025, 1, 1),
        dt.date(2025, 1, 1),
        _cfg(ot),
        _pass(),
        str(tmp_path),
    )
    assert out is None


def test_aggregate_guard_missing_partition_columns_raises(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))
    _write_batch(tmp_path, with_partition_cols=False)

    ot = OutputTarget(
        type=OutputType.PARQUET,
        path_template=str(tmp_path / "out.parquet"),
        partition_columns=["MIC"],
    )

    with pytest.raises(RuntimeError, match="Partition columns missing"):
        aggregate_and_write_final_output(
            dt.date(2025, 1, 1),
            dt.date(2025, 1, 1),
            _cfg(ot),
            _pass(),
            str(tmp_path),
        )


def test_aggregate_guard_no_output_target_raises(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))
    _write_batch(tmp_path)
    cfg = _cfg(OutputTarget(type=OutputType.PARQUET, path_template=str(tmp_path / "x.parquet")))
    cfg.OUTPUT_TARGET = None  # exercise runtime guard
    with pytest.raises(RuntimeError, match="No output target configured"):
        aggregate_and_write_final_output(
            dt.date(2025, 1, 1),
            dt.date(2025, 1, 1),
            cfg,
            _pass(output=None),
            str(tmp_path),
        )


def test_aggregate_output_target_dict_is_model_validated(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))
    _write_batch(tmp_path)
    pass_cfg = _pass()
    pass_cfg.output = {
        "type": "parquet",
        "path_template": str(tmp_path / "validated.parquet"),
    }
    out = aggregate_and_write_final_output(
        dt.date(2025, 1, 1),
        dt.date(2025, 1, 1),
        _cfg(OutputTarget(type=OutputType.PARQUET, path_template=str(tmp_path / "unused.parquet"))),
        pass_cfg,
        str(tmp_path),
    )
    assert out.endswith("validated.parquet")


def test_aggregate_falls_back_to_global_output_when_pass_template_empty(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))
    _write_batch(tmp_path)
    cfg = _cfg(
        OutputTarget(type=OutputType.PARQUET, path_template=str(tmp_path / "global.parquet"))
    )
    pass_cfg = _pass(output=OutputTarget(type=OutputType.PARQUET, path_template=None))
    out = aggregate_and_write_final_output(
        dt.date(2025, 1, 1),
        dt.date(2025, 1, 1),
        cfg,
        pass_cfg,
        str(tmp_path),
    )
    assert out.endswith("global.parquet")


def test_aggregate_quality_checks_branch(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))
    _write_batch(tmp_path, with_partition_cols=False)

    called = {"qc": 0, "emit": 0}

    def _qc(df, cfg):
        called["qc"] += 1
        return []

    def _emit(problems, cfg):
        called["emit"] += 1

    import basalt.process as process

    monkeypatch.setattr(process, "run_quality_checks", _qc)
    monkeypatch.setattr(process, "emit_quality_results", _emit)

    pass_cfg = _pass()
    pass_cfg.quality_checks.ENABLED = True

    ot = OutputTarget(
        type=OutputType.PARQUET,
        path_template=str(tmp_path / "out.parquet"),
    )

    out = aggregate_and_write_final_output(
        dt.date(2025, 1, 1),
        dt.date(2025, 1, 1),
        _cfg(ot),
        pass_cfg,
        str(tmp_path),
    )
    assert out and out.endswith("out.parquet")
    assert called["qc"] == 1
    assert called["emit"] == 1


def test_aggregate_parquet_s3_uses_retry_and_cleanup_handles_oserror(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))
    batch_file = _write_batch(tmp_path, with_partition_cols=False)

    import basalt.process as process

    calls = {"retry": 0, "remove": 0}

    def _retry(fn, **kwargs):
        calls["retry"] += 1
        return fn()

    def _remove(path):
        calls["remove"] += 1
        raise OSError("rm failed")

    monkeypatch.setattr(process, "retry_s3", _retry)
    monkeypatch.setattr(process.os, "remove", _remove)
    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", lambda self, path, compression=None: None)

    ot = OutputTarget(
        type=OutputType.PARQUET,
        path_template="s3://bucket/path/{datasetname}/{pass}/{start_date}_{end_date}.parquet",
    )

    out = aggregate_and_write_final_output(
        dt.date(2025, 1, 1),
        dt.date(2025, 1, 1),
        _cfg(ot),
        _pass(),
        str(tmp_path),
    )

    assert out.startswith("s3://")
    assert calls["retry"] == 1
    assert calls["remove"] >= 1
    assert os.path.exists(batch_file)


def test_aggregate_delta_branch_with_dedupe(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))
    _write_batch(tmp_path, with_partition_cols=True)

    calls = {"predicates": []}

    writer_mod = types.ModuleType("deltalake.writer")

    def _write(path, df, mode, storage_options, partition_by):
        calls["path"] = path
        calls["mode"] = mode
        calls["partition_by"] = partition_by
        calls["rows"] = len(df)

    writer_mod.write_deltalake = _write

    delta_mod = types.ModuleType("deltalake")

    class _DeltaTable:
        def __init__(self, path):
            calls["dt_path"] = path

        def delete(self, predicate):
            calls["predicates"].append(predicate)

    delta_mod.DeltaTable = _DeltaTable

    monkeypatch.setitem(sys.modules, "deltalake", delta_mod)
    monkeypatch.setitem(sys.modules, "deltalake.writer", writer_mod)

    out_path = str(tmp_path / "delta_out")
    ot = OutputTarget(
        type=OutputType.DELTA,
        path_template=out_path,
        partition_columns=["MIC", "Date"],
        dedupe_on_partition=True,
        delta_mode="append",
    )

    out = aggregate_and_write_final_output(
        dt.date(2025, 1, 1),
        dt.date(2025, 1, 1),
        _cfg(ot),
        _pass(),
        str(tmp_path),
    )

    assert out == out_path
    assert calls["path"] == out_path
    assert calls["mode"] == "append"
    assert calls["partition_by"] == ["MIC", "Date"]
    assert calls["rows"] == 1
    assert calls["predicates"]


def test_aggregate_delta_missing_dependency_raises(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))
    _write_batch(tmp_path, with_partition_cols=True)

    import builtins

    real_import = builtins.__import__

    def _import(name, *args, **kwargs):
        if name.startswith("deltalake"):
            raise ImportError("missing deltalake")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _import)
    ot = OutputTarget(type=OutputType.DELTA, path_template=str(tmp_path / "delta"))
    with pytest.raises(RuntimeError, match="deltalake is required"):
        aggregate_and_write_final_output(
            dt.date(2025, 1, 1),
            dt.date(2025, 1, 1),
            _cfg(ot),
            _pass(),
            str(tmp_path),
        )


def test_aggregate_sql_pandas_and_core_paths(tmp_path, monkeypatch):
    pytest.importorskip("sqlalchemy")
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))

    db_path = tmp_path / "out.db"
    conn = f"sqlite:///{db_path}"

    # pandas path with preserve_index/index_name and append+dedupe behavior
    _write_batch(tmp_path, with_partition_cols=False)
    ot_pandas = OutputTarget(
        type=OutputType.SQL,
        sql_connection=conn,
        sql_table="metrics_pd",
        sql_if_exists="replace",
        sql_use_pandas=True,
        preserve_index=True,
        index_name="idx",
        partition_columns=["ListingId"],
        dedupe_on_partition=True,
    )
    aggregate_and_write_final_output(
        dt.date(2025, 1, 1),
        dt.date(2025, 1, 1),
        _cfg(ot_pandas),
        _pass(),
        str(tmp_path),
    )

    # second append run with same partition should execute dedupe branch then append
    _write_batch(tmp_path, with_partition_cols=False)
    ot_append = ot_pandas.model_copy(update={"sql_if_exists": "append"})
    aggregate_and_write_final_output(
        dt.date(2025, 1, 1),
        dt.date(2025, 1, 1),
        _cfg(ot_append),
        _pass(),
        str(tmp_path),
    )

    # core insert path (no pandas)
    pl.DataFrame(
        {
            "ListingId": [1],
            "TimeBucket": [dt.datetime(2025, 1, 1, 10, 0)],
            "Metric": [1.23],
            "Txt": ["abc"],
        }
    ).write_parquet(os.path.join(tmp_path, "batch-metrics-0001.parquet"))
    pl.DataFrame(
        {
            "ListingId": [2],
            "TimeBucket": [dt.datetime(2025, 1, 1, 10, 1)],
            "Metric": [2.34],
            "Txt": ["def"],
        }
    ).write_parquet(os.path.join(tmp_path, "batch-metrics-0002.parquet"))
    ot_core = OutputTarget(
        type=OutputType.SQL,
        sql_connection=conn,
        sql_table="metrics_core",
        sql_if_exists="replace",
        sql_use_pandas=False,
        sql_batch_size=1,
    )
    aggregate_and_write_final_output(
        dt.date(2025, 1, 1),
        dt.date(2025, 1, 1),
        _cfg(ot_core),
        _pass(),
        str(tmp_path),
    )

    from sqlalchemy import create_engine, inspect, text

    engine = create_engine(conn)
    tables = inspect(engine).get_table_names()
    assert "metrics_pd" in tables
    assert "metrics_core" in tables
    with engine.connect() as conn_h:
        assert conn_h.execute(text("SELECT COUNT(*) FROM metrics_pd")).fetchone()[0] >= 1
        assert conn_h.execute(text("SELECT COUNT(*) FROM metrics_core")).fetchone()[0] >= 1


def test_aggregate_sql_guard_requires_connection_and_table(tmp_path, monkeypatch):
    pytest.importorskip("sqlalchemy")
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))
    _write_batch(tmp_path)

    ot = OutputTarget(type=OutputType.SQL, sql_connection=None, sql_table=None)
    with pytest.raises(RuntimeError, match="sql_connection and sql_table"):
        aggregate_and_write_final_output(
            dt.date(2025, 1, 1),
            dt.date(2025, 1, 1),
            _cfg(ot),
            _pass(),
            str(tmp_path),
        )


def test_aggregate_unsupported_output_type_raises(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "bmll2", types.SimpleNamespace(storage_paths=lambda: {}))
    _write_batch(tmp_path)

    ot = OutputTarget(type=OutputType.PARQUET, path_template=str(tmp_path / "x.parquet"))
    ot.type = "mystery"  # intentionally bypass enum to hit runtime guard

    with pytest.raises(RuntimeError, match="Unsupported output type"):
        aggregate_and_write_final_output(
            dt.date(2025, 1, 1),
            dt.date(2025, 1, 1),
            _cfg(ot),
            _pass(),
            str(tmp_path),
        )


def test_batch_writer_write_and_close(tmp_path):
    out_path = tmp_path / "dir" / "bw.parquet"
    writer = BatchWriter(str(out_path))
    writer.write(
        pl.DataFrame(
            {"ListingId": [1], "TimeBucket": [dt.datetime(2025, 1, 1, 10, 0)], "x": [1.0]}
        )
    )
    writer.write(
        pl.DataFrame(
            {"ListingId": [1], "TimeBucket": [dt.datetime(2025, 1, 1, 10, 1)], "x": [2.0]}
        )
    )
    assert out_path.exists()
    writer.close()
    writer.close()  # idempotent close path
