from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Literal, Optional, Union

import pytest
import yaml
from pydantic import BaseModel, Field
from textual.widgets import Button, Checkbox, Input, Select, TextArea

import basalt.config_ui as config_ui


class ChildModel(BaseModel):
    """Child model for nested editing."""

    x: int = 1


class Mode(Enum):
    A = "a"
    B = "b"


class RichModel(BaseModel):
    """Rich model.

    Extra details for model docs.
    """

    flag: bool = False
    count: int = 1
    ratio: float = 1.0
    text: str = "abc"
    mode: Literal["m1", "m2"] = "m1"
    single: Literal["only"] = "only"
    list_mode: list[Literal["x", "y"]] = Field(default_factory=lambda: ["x"])
    enum_mode: Mode = Mode.A
    columns: list[str] = Field(
        default_factory=lambda: ["ListingId"],
        description="cols",
        json_schema_extra={"long_doc": "long columns doc"},
    )
    nested: ChildModel = Field(default_factory=ChildModel)
    nested_list: list[ChildModel] = Field(default_factory=list)
    misc: dict = Field(default_factory=dict)
    maybe_mode: Union[Literal["u1", "u2"], None] = "u1"
    maybe_list_lit: Union[list[Literal["lx", "ly"]], None] = None
    optional_num: Optional[int] = None


def _press(button_id: str) -> Button.Pressed:
    return Button.Pressed(Button("x", id=button_id))


def test_helper_functions_and_yaml_io(tmp_path):
    py_path = tmp_path / "demo_cfg.py"
    py_path.write_text("USER_CONFIG = {'A': 1}\n", encoding="utf-8")

    loaded = config_ui._load_module_from_path(py_path)
    assert loaded is not None and loaded.USER_CONFIG["A"] == 1

    assert config_ui._resolve_yaml_path(py_path).suffix == ".yaml"
    assert config_ui._resolve_yaml_path(tmp_path / "a.yaml").suffix == ".yaml"

    text = config_ui._default_yaml_content(py_path)
    assert "USER_CONFIG" in text

    yaml_path = tmp_path / "demo_cfg.yaml"
    config_ui._save_yaml_user_config(yaml_path, {"X": 2})
    assert config_ui._load_yaml_user_config(yaml_path)["X"] == 2

    bad_yaml = tmp_path / "bad.yaml"
    bad_yaml.write_text("- 1\n- 2\n", encoding="utf-8")
    assert config_ui._load_yaml_user_config(bad_yaml) == {}

    assert config_ui._unwrap_optional(Optional[int]) is int
    assert config_ui._literal_from_union(Union[Literal["A", "B"], int]) == ["A", "B"]
    assert config_ui._list_literal_from_union(Union[list[Literal["A", "B"]], int]) == [
        "A",
        "B",
    ]
    assert config_ui._is_literal(Literal["A"]) is True
    assert config_ui._literal_options(Literal["A", "B"]) == ["A", "B"]
    assert config_ui._is_list_of_literal(list[Literal["A", "B"]]) == ["A", "B"]
    assert config_ui._is_enum(Mode) is True
    assert config_ui._enum_options(Mode) == ["a", "b"]
    assert config_ui._is_basemodel(ChildModel) is True
    assert config_ui._is_list_of_basemodel(list[ChildModel]) is ChildModel

    assert "X" in config_ui._yaml_from_value({"X": 1})
    assert config_ui._parse_yaml("a: 1\n")["a"] == 1


def test_module_source_helpers(monkeypatch):
    monkeypatch.setattr(
        config_ui,
        "_module_meta",
        lambda: ({}, {"generic": ["generic_analytics"]}, {}),
    )

    specs = config_ui._module_source_config_specs("generic")
    assert specs

    assert config_ui._module_source_default("generic_analytics", "missing_key") is None

    pass_data = {"generic_analytics": {"source_pass": "pass1"}}
    pairs = config_ui._module_source_config_pairs(pass_data, "generic")
    assert ("df", "pass1") in pairs

    updates = config_ui._module_source_config_updates(
        {("generic", "generic_analytics", "source_pass"): "pass2"}
    )
    assert updates["generic_analytics"]["source_pass"] == "pass2"


def test_counts_metric_helpers_and_output_driver(monkeypatch):
    cfg = {
        "trade_analytics": {"generic_metrics": [1], "retail_imbalance": {"ENABLED": True}},
        "l2_analytics": {"liquidity": [1], "spreads": [1], "imbalances": [], "volatility": [], "ohlc": []},
        "l3_analytics": {"generic_metrics": [1], "advanced_metrics": [1]},
        "execution_analytics": {"l3_execution": [1], "trade_breakdown": [1], "derived_metrics": [1]},
        "iceberg_analytics": {"metrics": [1]},
        "cbbo_analytics": {"measures": [1, 2]},
        "generic_analytics": {"aggregations": {"a": 1}, "talib_indicators": [1]},
    }
    assert config_ui._module_metric_count(cfg, "trade") >= 2
    assert config_ui._module_metric_count(cfg, "l2") == 2
    assert config_ui._module_metric_count(cfg, "unknown") == 0

    monkeypatch.setattr(config_ui, "get_output_schema", lambda _p: {"k1": ["a", "b"], "k2": ["c"]})
    assert config_ui._pass_metric_count({"name": "pass1"}) == 3
    assert len(config_ui._pass_metric_list({"name": "pass1"})) == 3

    assert "sql" in config_ui._format_output_driver({"type": "sql", "sql_table": "t", "sql_connection": "c"}, {})
    assert "parquet" in config_ui._format_output_driver({"type": "parquet", "path_template": "/x"}, {})


@pytest.mark.asyncio
async def test_model_editor_event_paths(monkeypatch):
    saved = {}

    app = config_ui.ConfigEditor(Path("/tmp/nonexistent.yaml"), {"PASSES": []})
    async with app.run_test() as pilot:
        editor = config_ui.ModelEditor(
            RichModel,
            {"columns": ["ListingId"], "mode": "m1", "nested_list": [{"x": 1}]},
            "RichModel",
            lambda d: saved.update(d),
        )
        app.push_screen(editor)
        await pilot.pause()

        # model and field details
        editor._model_doc = "model doc"
        editor._field_docs["columns"] = "columns long doc"
        editor.on_button_pressed(_press("details_model"))
        editor.on_button_pressed(_press("details_columns"))

        # pick columns -> ColumnPicker, then save
        monkeypatch.setattr(editor, "_get_schema_columns", lambda: ["A", "B", "C"])
        editor.on_button_pressed(_press("pick_columns"))
        await pilot.pause()
        assert isinstance(app.screen, config_ui.ColumnPicker)
        cp = app.screen
        original_dump = config_ui.yaml.safe_dump
        monkeypatch.setattr(config_ui.yaml, "safe_dump", lambda *_a, **_k: "[]\n")
        cp.on_button_pressed(_press("save"))
        monkeypatch.setattr(config_ui.yaml, "safe_dump", original_dump)
        await pilot.pause()

        # nested model editor
        editor.on_button_pressed(_press("edit_nested"))
        await pilot.pause()
        assert isinstance(app.screen, config_ui.ModelEditor)
        app.screen.on_button_pressed(_press("cancel"))
        await pilot.pause()

        # list editor
        editor.on_button_pressed(_press("editlist_nested_list"))
        await pilot.pause()
        assert isinstance(app.screen, config_ui.ListEditor)
        lst = app.screen
        lst.on_button_pressed(_press("add"))
        await pilot.pause()
        assert isinstance(app.screen, config_ui.ModelEditor)
        app.screen.on_button_pressed(_press("cancel"))
        await pilot.pause()

        # back to list editor actions edit/remove/back
        app.push_screen(lst)
        await pilot.pause()
        lst = app.screen
        lst.on_button_pressed(_press("edit_0"))
        await pilot.pause()
        app.screen.on_button_pressed(_press("cancel"))
        await pilot.pause()
        app.push_screen(lst)
        await pilot.pause()
        lst = app.screen
        lst.on_button_pressed(_press("remove_0"))
        await pilot.pause()
        assert isinstance(app.screen, config_ui.ListEditor)
        app.screen.on_button_pressed(_press("back"))
        await pilot.pause()

        # invalid yaml path in _save
        w, _ = editor.widgets["misc"]
        assert isinstance(w, TextArea)
        w.text = "a: [1\n"
        editor._save()

        # select changed path
        for wid, name in editor._widget_names.items():
            if name == "mode":
                sel = editor.query_one(f"#{wid}", Select)
                editor.on_select_changed(Select.Changed(sel, "m2"))
                break

        # valid save path
        w.text = yaml.safe_dump({"ok": True})
        editor.data["misc"] = {"ok": True}
        editor._save()
        await pilot.pause()

    assert "mode" in saved


@pytest.mark.asyncio
async def test_config_and_pass_editors_flow(monkeypatch, tmp_path):
    yaml_path = tmp_path / "cfg.yaml"
    initial = {
        "DATASETNAME": "ds",
        "OUTPUT_TARGET": {"type": "parquet", "path_template": str(tmp_path / "out.parquet")},
        "PASSES": [
            {
                "name": "pass1",
                "time_bucket_seconds": 60,
                "modules": ["generic"],
                "generic_analytics": {"group_by": ["ListingId", "TimeBucket"], "source_pass": "pass1"},
            }
        ],
    }

    monkeypatch.setattr(config_ui.PassEditor, "_render_timeline_controls", lambda self: None)
    app = config_ui.ConfigEditor(yaml_path, initial)
    async with app.run_test() as pilot:
        app.action_toggle_advanced()
        app.action_edit_section("Core")
        await pilot.pause()
        assert isinstance(app.screen, config_ui.ModelEditor)
        app.screen.on_button_pressed(_press("cancel"))
        await pilot.pause()

        app.action_edit_passes()
        await pilot.pause()
        assert isinstance(app.screen, config_ui.PassListEditor)

        # Add flow -> PassEditor then cancel
        app.screen.on_button_pressed(_press("add"))
        await pilot.pause()
        if isinstance(app.screen, config_ui.ConfirmScreen):
            app.screen.on_button_pressed(_press("confirm"))
            await pilot.pause()
        assert isinstance(app.screen, config_ui.PassEditor)
        app.screen.on_button_pressed(_press("cancel"))
        await pilot.pause()

        # Edit first pass and exercise editor actions
        app.push_screen(config_ui.PassListEditor(app.config_data))
        await pilot.pause()
        app.screen.on_button_pressed(_press("edit_0"))
        await pilot.pause()
        if isinstance(app.screen, config_ui.ConfirmScreen):
            app.screen.on_button_pressed(_press("confirm"))
            await pilot.pause()
        pe = app.screen
        assert isinstance(pe, config_ui.PassEditor)

        pe.on_button_pressed(_press("edit_full"))
        await pilot.pause()
        assert isinstance(app.screen, config_ui.ModelEditor)
        app.screen.on_button_pressed(_press("cancel"))
        await pilot.pause()

        pe.on_button_pressed(_press("edit_inputs"))
        await pilot.pause()
        assert isinstance(app.screen, config_ui.ModuleInputsEditor)
        mie = app.screen
        mie.on_button_pressed(_press("save"))
        await pilot.pause()

        pe.on_button_pressed(_press("edit_dense"))
        await pilot.pause()
        assert isinstance(app.screen, config_ui.ModelEditor)
        app.screen.on_button_pressed(_press("cancel"))
        await pilot.pause()

        pe.on_button_pressed(_press("edit_event"))
        await pilot.pause()
        assert isinstance(app.screen, config_ui.ModelEditor)
        app.screen.on_button_pressed(_press("cancel"))
        await pilot.pause()

        # Invalid then valid save branches
        pe.widgets["time_bucket_seconds"].value = "bad"
        pe._save_pass()
        pe.widgets["time_bucket_seconds"].value = "30"
        pe.pass_readonly = False
        pe.widgets["pass_type"].value = "post"
        pe.widgets["timeline_mode"].value = "event"
        pe._save_pass()
        await pilot.pause()

        # Readonly confirm branch (separate instance)
        pe2 = config_ui.PassEditor({"name": "p2", "modules": ["trade"]}, "PassConfig", lambda _d: None)
        app.push_screen(pe2)
        await pilot.pause()
        pe2.pass_readonly = True
        pe2.on_button_pressed(_press("save"))
        await pilot.pause()
        assert isinstance(app.screen, config_ui.ConfirmScreen)
        app.screen.on_button_pressed(_press("cancel"))
        await pilot.pause()

        # config save and section actions
        app.action_edit_section("Outputs")
        await pilot.pause()
        app.screen.on_button_pressed(_press("cancel"))
        await pilot.pause()
        monkeypatch.setattr(config_ui, "_save_yaml_user_config", lambda *_a, **_k: None)
        app.action_save()
        app.action_edit_passes()
        await pilot.pause()
        app.action_toggle_pass_lock()

        # module config picker / confirm screen direct events
        picked = {}
        mcp = config_ui.ModuleConfigPicker(["a", "b"], lambda f: picked.setdefault("f", f), "pick")
        app.push_screen(mcp)
        await pilot.pause()
        mcp.on_button_pressed(_press("pick_a"))
        await pilot.pause()
        assert picked["f"] == "a"

    assert picked["f"] == "a"


def test_main_entrypoint_paths(monkeypatch, tmp_path):
    py_cfg = tmp_path / "demo_cfg.py"
    py_cfg.write_text("USER_CONFIG = {'DATASETNAME': 'x', 'PASSES': []}\n", encoding="utf-8")

    ran = {"args": None}

    class _FakeApp:
        def __init__(self, yaml_path, user_config):
            self.yaml_path = yaml_path
            self.user_config = user_config

        def run(self, **kwargs):
            ran["args"] = kwargs

    monkeypatch.setattr(config_ui, "ConfigEditor", _FakeApp)
    monkeypatch.setattr(config_ui.sys, "argv", ["config_ui", str(py_cfg)])
    config_ui.main()
    assert ran["args"] == {}

    # web mode failure branch
    class _FailApp(_FakeApp):
        def run(self, **kwargs):
            raise RuntimeError("web missing")

    monkeypatch.setattr(config_ui, "ConfigEditor", _FailApp)
    monkeypatch.setattr(config_ui.sys, "argv", ["config_ui", "--web", str(py_cfg)])
    with pytest.raises(SystemExit):
        config_ui.main()

    monkeypatch.setattr(config_ui.sys, "argv", ["config_ui"])
    with pytest.raises(SystemExit):
        config_ui.main()
