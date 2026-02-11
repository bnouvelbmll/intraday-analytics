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
