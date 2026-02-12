import json
from pathlib import Path

from basalt import schema_utils


def test_build_full_config_and_schema():
    cfg = schema_utils._build_full_config(levels=2, impact_horizons=["1s"])
    assert cfg.PASSES
    out = schema_utils.get_output_schema(cfg.PASSES[0])
    assert isinstance(out, dict)
    assert any(k.startswith("trade") or k.startswith("l2") for k in out.keys())


def test_schema_utils_main_plain_json(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        [
            "schema_utils",
            "--full",
            "--levels",
            "2",
            "--impact-horizons",
            "1s",
            "--json",
            "--plain",
        ],
    )
    schema_utils.main()
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert isinstance(parsed, dict)
    assert parsed


def test_schema_utils_main_from_config_file(monkeypatch, capsys):
    cfg_path = Path("tmp_schema_cfg.json")
    cfg_path.write_text(
        json.dumps(
            {
                "PASSES": [
                    {
                        "name": "pass1",
                        "modules": ["trade"],
                        "trade_analytics": {
                            "generic_metrics": [
                                {"sides": ["Total"], "measures": ["Volume"]}
                            ]
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    try:
        monkeypatch.setattr(
            "sys.argv",
            [
                "schema_utils",
                "--config",
                str(cfg_path),
                "--json",
                "--plain",
                "--no-hints",
            ],
        )
        schema_utils.main()
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert isinstance(parsed, dict)
    finally:
        if cfg_path.exists():
            cfg_path.unlink()
