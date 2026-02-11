import os
import sys
import types
import polars as pl

from basalt.utils import cache_universe


def test_cache_universe_uses_env_temp_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("INTRADAY_ANALYTICS_TEMP_DIR", str(tmp_path))
    # ensure bmll2 import does not fail
    if "bmll2" not in sys.modules:
        sys.modules["bmll2"] = types.ModuleType("bmll2")

    def _get_universe(date):
        return pl.DataFrame({"ListingId": [1], "MIC": ["X"]})

    wrapped = cache_universe(None)(_get_universe)
    wrapped("2025-01-01")

    expected = tmp_path / "universe_cache" / "universe-2025-01-01.parquet"
    assert expected.exists()
