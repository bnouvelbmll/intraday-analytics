import json
from pathlib import Path

from basalt.analytics_explain import (
    explain_column,
    format_explain_markdown,
    main as analytics_explain_main,
)


def _cfg() -> dict:
    return {
        "PASSES": [
            {
                "name": "pass1",
                "modules": ["trade"],
                "trade_analytics": {
                    "generic_metrics": [{"sides": ["Total"], "measures": ["Volume"]}]
                },
            }
        ]
    }


def test_explain_column_found_and_markdown():
    payload = explain_column(
        pipeline="",
        column="TradeTotalVolume",
        config_dict=_cfg(),
    )
    assert payload["module"] == "trade"
    md = format_explain_markdown(payload)
    assert "TradeTotalVolume" in md
    assert "Module:" in md


def test_explain_column_not_found_message():
    payload = explain_column(
        pipeline="",
        column="DefinitelyMissingColumn",
        config_dict=_cfg(),
    )
    assert payload["found"] is False
    assert "not found" in format_explain_markdown(payload).lower()


def test_explain_column_with_module_hint():
    payload = explain_column(
        pipeline="",
        column="AnyColumn",
        config_dict=_cfg(),
        module_hint="trade",
        pass_name="pass1",
    )
    assert payload["module"] == "trade"
    assert payload["pass_name"] == "pass1"


def test_analytics_explain_main_json(monkeypatch, capsys):
    cfg_file = Path("tmp_explain_cfg.json")
    cfg_file.write_text(json.dumps(_cfg()), encoding="utf-8")
    try:
        monkeypatch.setattr(
            "sys.argv",
            [
                "analytics_explain",
                "--pipeline",
                str(cfg_file),
                "--column",
                "TradeTotalVolume",
                "--json",
            ],
        )
        analytics_explain_main()
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert parsed["column"] == "TradeTotalVolume"
    finally:
        if cfg_file.exists():
            cfg_file.unlink()
