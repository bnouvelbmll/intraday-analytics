from __future__ import annotations

import polars as pl

from basalt.visualization.data import (
    apply_universe_filter_to_frame,
    available_universe_keys,
    estimate_listing_days,
    filter_frame,
    infer_dataset_options,
    load_resolved_user_config,
    select_subuniverse,
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


def test_universe_selection_and_ld_estimate():
    ref = pl.DataFrame(
        {
            "ListingId": [1, 2, 3],
            "InstrumentId": [10, 20, 20],
            "ISIN": ["A", "B", "C"],
        }
    )
    keys = available_universe_keys(ref)
    assert "ISIN" in keys
    assert "ListingId" in keys

    one = select_subuniverse(ref, mode="ListingId", single_value="2")
    assert one.height == 1
    assert one["ListingId"].to_list() == [2]

    subset = select_subuniverse(
        ref,
        mode="Arbitrary subset",
        subset_column="ISIN",
        subset_values=["A", "C"],
    )
    assert subset["ISIN"].to_list() == ["A", "C"]

    ld = estimate_listing_days(subset, date_start="2026-01-01", date_end="2026-01-03")
    assert ld == 6


def test_apply_universe_filter_to_frame_uses_common_identifier():
    ref = pl.DataFrame({"InstrumentId": [10, 30]})
    frame = pl.DataFrame(
        {
            "InstrumentId": [10, 20, 30],
            "TimeBucket": ["2026-01-01T10:00:00"] * 3,
            "x": [1.0, 2.0, 3.0],
        }
    )
    out = apply_universe_filter_to_frame(frame, ref)
    assert out["InstrumentId"].to_list() == [10, 30]
