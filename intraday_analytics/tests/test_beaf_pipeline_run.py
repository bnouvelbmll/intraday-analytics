import textwrap
from pathlib import Path

import intraday_analytics.beaf as beaf


def test_beaf_pipeline_run_loads_module(tmp_path, monkeypatch):
    module_path = tmp_path / "demo_mod.py"
    module_path.write_text(
        textwrap.dedent(
            """
            USER_CONFIG = {"START_DATE": "2025-01-01", "END_DATE": "2025-01-01"}
            def get_universe(date):
                return {"ListingId": [1], "MIC": ["X"]}
            """
        )
    )

    called = {}

    def _fake_run_cli(user_config, default_get_universe, get_pipeline=None, **kwargs):
        called["user_config"] = user_config
        called["get_universe"] = default_get_universe
        called["kwargs"] = kwargs

    monkeypatch.setattr(beaf, "run_cli", _fake_run_cli)

    beaf._pipeline_run(pipeline=str(module_path))

    assert called["user_config"]["START_DATE"] == "2025-01-01"
    assert callable(called["get_universe"])
    assert called["kwargs"]["config_file"].endswith("demo_mod.py")
