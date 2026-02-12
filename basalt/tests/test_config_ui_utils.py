from typing import Literal

import pytest
from pydantic import BaseModel
from textual.app import App

import basalt.config_ui as config_ui


class DummyModel(BaseModel):
    options: list[Literal["A", "B"]] = ["A"]


def test_module_meta_autodiscovery():
    module_info, field_map, schema_keys = config_ui._module_meta()
    assert "l2" in module_info
    assert "l2_analytics" in field_map.get("l2", [])
    assert "l2_last" in schema_keys.get("l2", [])


def test_field_helpers():
    assert config_ui._field_uses_columns("columns")
    assert config_ui._field_uses_columns("group_by")
    assert config_ui._field_uses_columns("symbol_cols")
    assert not config_ui._field_uses_columns("metric_prefix")

    assert config_ui._is_list_of_str(list[str])
    assert not config_ui._is_list_of_str(list[int])


def test_counts_from_schema_uses_module_meta():
    schema = {"l2_last": ["a", "b"], "l2_tw": ["c"]}
    counts = config_ui._counts_from_schema(schema)
    assert counts.get("l2") == 3


def test_input_source_choices_only_prior_passes():
    config_data = {
        "PASSES": [
            {"name": "pass1"},
            {"name": "pass2"},
            {"name": "pass3"},
        ]
    }
    choices = config_ui._input_source_choices(config_data, pass_index=2)
    assert "pass1" in choices
    assert "pass2" in choices
    assert "pass3" not in choices


def test_build_module_inputs_from_selection():
    selected = {
        ("l2", "l2"): "pass1",
        ("execution", "trades"): "pass1",
        ("execution", "l3"): "l3",
    }
    module_requires = {
        "l2": ["l2"],
        "execution": ["trades", "l3"],
    }
    out = config_ui._build_module_inputs_from_selection(selected, module_requires)
    assert out["l2"] == "pass1"
    assert out["execution"] == {"trades": "pass1", "l3": "l3"}


def test_module_inputs_line_format():
    line = config_ui._module_inputs_line(
        {"module_inputs": {"l2": "pass1"}},
        "l2",
    )
    assert "Inputs = [" in line
    assert "(L2:pass1)" in line


def test_module_inputs_line_uses_source_pass_defaults():
    line = config_ui._module_inputs_line({"modules": ["generic"]}, "generic")
    assert "Inputs = [" in line
    assert "(DF:pass1)" in line


def test_module_inputs_line_for_custom_module_from_module_inputs():
    line = config_ui._module_inputs_line(
        {"module_inputs": {"talib_metrics": "ohlcv_pass"}},
        "talib_metrics",
    )
    assert "Inputs = [" in line
    assert "(DF:ohlcv_pass)" in line


def test_pass_inputs_summary_format():
    summary = config_ui._pass_inputs_summary(
        {"modules": ["l2"], "module_inputs": {"l2": "pass1"}}
    )
    assert "l2:[" in summary
    assert "(L2:pass1)" in summary


def test_pass_inputs_summary_includes_source_pass_modules():
    summary = config_ui._pass_inputs_summary({"modules": ["generic"]})
    assert "generic:[" in summary
    assert "(DF:pass1)" in summary


def test_derive_timeline_mode_from_config_or_modules():
    assert config_ui._derive_timeline_mode({"timeline_mode": "event"}) == "event"
    assert config_ui._derive_timeline_mode({"modules": ["trade", "dense"]}) == "dense"
    assert config_ui._derive_timeline_mode({"modules": ["events"]}) == "event"
    assert config_ui._derive_timeline_mode({"modules": ["trade"]}) == "dense"


class _DummyApp(App):
    pass


@pytest.mark.asyncio
async def test_render_field_handles_list_widgets():
    app = _DummyApp()
    async with app.run_test():
        editor = config_ui.ModelEditor(DummyModel, {}, "Dummy", lambda _: None)
        annotation = DummyModel.model_fields["options"].annotation
        value = ["A"]
        # Ensure it does not crash when widget is a list
        app._compose_stacks.append([])
        app._composed.append([])
        try:
            list(editor._render_field("options", annotation, value))
        finally:
            app._composed.pop()
            app._compose_stacks.pop()
        assert "options" in editor.widgets
