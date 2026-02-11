import json

import pytest

from intraday_analytics.configuration import AnalyticsConfig, PassConfig
from intraday_analytics.execution import _check_schema_lock


def _make_config(tmp_path, mode: str, locked: dict | None = None) -> AnalyticsConfig:
    config = AnalyticsConfig(
        PASSES=[
            PassConfig(
                name="pass1",
                modules=["l2"],
            )
        ]
    )
    lock_path = tmp_path / "schema_lock.json"
    if locked is not None:
        lock_path.write_text(json.dumps(locked), encoding="utf-8")
    config.SCHEMA_LOCK_PATH = str(lock_path)
    config.SCHEMA_LOCK_MODE = mode
    return config


def test_schema_lock_update_writes_file(tmp_path):
    config = _make_config(tmp_path, "update")
    _check_schema_lock(config)
    lock_path = tmp_path / "schema_lock.json"
    assert lock_path.exists()
    data = json.loads(lock_path.read_text(encoding="utf-8"))
    assert "pass1" in data
    assert isinstance(data["pass1"], list)
    assert data["pass1"]


def test_schema_lock_raise_on_mismatch(tmp_path):
    locked = {"pass1": ["WrongColumn"]}
    config = _make_config(tmp_path, "raise", locked=locked)
    with pytest.raises(RuntimeError, match="Schema lock mismatch"):
        _check_schema_lock(config)
