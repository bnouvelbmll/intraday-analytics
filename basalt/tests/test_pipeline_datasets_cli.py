from __future__ import annotations

import textwrap

import basalt.basalt as basalt_main


def test_pipeline_datasets_lists_inputs_outputs(tmp_path):
    module_path = tmp_path / "demo_mod.py"
    module_path.write_text(
        textwrap.dedent(
            """
            USER_CONFIG = {
                "START_DATE": "2026-01-01",
                "END_DATE": "2026-01-01",
                "PASSES": [{"name": "p1", "modules": ["l2"]}],
                "OUTPUT_TARGET": {"type": "parquet", "path_template": "/tmp/out_{datasetname}_{start_date}_{end_date}.parquet"},
            }
            def get_universe(date):
                return {"ListingId": [1], "MIC": ["XLON"]}
            """
        ),
        encoding="utf-8",
    )

    out = basalt_main._pipeline_datasets(pipeline=str(module_path))
    assert out["pipeline"].endswith("demo_mod.py")
    assert out["inputs"]
    assert any(row["table"] == "l2" for row in out["inputs"])
    assert out["outputs"]
    assert out["outputs"][0]["pass"] == "p1"
