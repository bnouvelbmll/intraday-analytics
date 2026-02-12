from __future__ import annotations

import pytest

import basalt.cli as cli


def test_run_cli_uses_user_config_json(monkeypatch):
    captured = {}

    class _Cfg:
        def __init__(self, **kwargs):
            captured["config_kwargs"] = kwargs

    def _fake_run_multiday_pipeline(*, config, get_universe, get_pipeline):
        captured["config"] = config
        captured["get_universe"] = get_universe
        captured["get_pipeline"] = get_pipeline

    monkeypatch.setattr(cli, "AnalyticsConfig", _Cfg)
    monkeypatch.setattr(cli, "run_multiday_pipeline", _fake_run_multiday_pipeline)
    monkeypatch.setattr(
        cli,
        "resolve_user_config",
        lambda *args, **kwargs: pytest.fail("resolve_user_config should not be called"),
    )

    cli.run_cli(
        {"SHOULD_NOT": "be used"},
        lambda date: {"ListingId": [1]},
        user_config_json='{"START_DATE":"2025-01-01","END_DATE":"2025-01-01","X":1}',
    )
    assert captured["config_kwargs"]["X"] == 1
    assert captured["config_kwargs"]["START_DATE"] == "2025-01-01"

