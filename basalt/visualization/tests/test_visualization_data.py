from __future__ import annotations

import polars as pl

from basalt.visualization.data import (
    filter_frame,
    infer_dataset_options,
    load_resolved_user_config,
)


def test_infer_dataset_options_defaults_last_pass():
    cfg = {
        "START_DATE": "2026-01-01",
        "END_DATE": "2026-01-02",
        "DATASETNAME": "demo",
        "OUTPUT_TARGET": {
            "path_template": "/tmp/out_{datasetname}_{start_date}_{end_date}.parquet"
        },
        "PASSES": [{"name": "pass1"}, {"name": "pass2"}],
    }
    out = infer_dataset_options(cfg)
    assert len(out) == 2
    assert out[-1].is_default is True
    assert out[-1].pass_name == "pass2"
    assert "demo_pass2" in out[-1].path


def test_filter_frame_by_date_and_instrument():
    df = pl.DataFrame(
        {
            "TimeBucket": [
                "2026-01-01T10:00:00",
                "2026-01-01T10:01:00",
                "2026-01-02T10:00:00",
            ],
            "ListingId": [1, 2, 1],
            "x": [1.0, 2.0, 3.0],
        }
    )
    out, time_col, instrument_col = filter_frame(
        df,
        date_start="2026-01-01",
        date_end="2026-01-01",
        instrument_value="1",
    )
    assert time_col == "TimeBucket"
    assert instrument_col == "ListingId"
    assert out.height == 1
    assert out["ListingId"].to_list() == [1]


def test_load_resolved_user_config_uses_yaml(tmp_path):
    py_file = tmp_path / "demo_pipeline.py"
    py_file.write_text(
        "USER_CONFIG = {'DATASETNAME': 'base', 'PASSES': [{'name': 'p1'}]}\n",
        encoding="utf-8",
    )
    yaml_file = tmp_path / "demo_pipeline.yaml"
    yaml_file.write_text("USER_CONFIG:\n  DATASETNAME: overridden\n", encoding="utf-8")
    cfg = load_resolved_user_config(pipeline=str(py_file))
    assert cfg["DATASETNAME"] == "overridden"
