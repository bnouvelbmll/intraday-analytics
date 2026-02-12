from __future__ import annotations

import importlib.util
import sys
import json
from pathlib import Path
from typing import Any, Optional, Union, get_args, get_origin
import random
from enum import Enum
import inspect

import yaml
from pydantic import BaseModel, ValidationError
from pydantic_core import PydanticUndefined
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import Screen
from textual.widgets import (
    Button,
    Checkbox,
    Footer,
    Header,
    Input,
    Label,
    Select,
    Static,
    TextArea,
)
from rich.panel import Panel
from rich.text import Text

from basalt.configuration import AnalyticsConfig, PassConfig, OutputTarget
from basalt.tables import ALL_TABLES
from basalt.plugins import get_plugin_module_configs, get_plugin_module_config_models
from basalt.analytics.generic import (
    TalibIndicatorConfig,
    GenericAnalyticsConfig,
    TALIB_AVAILABLE,
    list_talib_functions,
    get_talib_function_metadata,
)
from basalt.schema_utils import get_output_schema, get_full_output_schema
from basalt.analytics.l3 import L3AdvancedConfig


def _iter_global_sections(model_cls: type[BaseModel]):
    sections: dict[str, list[str]] = {}
    for name, field in model_cls.model_fields.items():  # type: ignore[attr-defined]
        section = "Advanced"
        extra = getattr(field, "json_schema_extra", None) or {}
        if isinstance(extra, dict):
            section = extra.get("section", section)
        sections.setdefault(section, []).append(name)

    ordered = []
    for section in (
        "Core",
        "Outputs",
        "Automation",
        "PerformanceAndExecutionEnvironment",
        "Advanced",
    ):
        if section in sections:
            ordered.append((section, sections.pop(section)))
    for section, names in sections.items():
        ordered.append((section, names))
    return ordered


def _load_module_from_path(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


def _resolve_yaml_path(path: Path) -> Path:
    if path.suffix == ".yaml":
        return path
    return path.with_suffix(".yaml")


def _default_yaml_content(py_path: Path) -> str:
    module = _load_module_from_path(py_path)
    if module is None or not hasattr(module, "USER_CONFIG"):
        return yaml.safe_dump({"USER_CONFIG": {}}, sort_keys=False)
    return yaml.safe_dump(
        {"USER_CONFIG": getattr(module, "USER_CONFIG")}, sort_keys=False
    )


def _load_yaml_user_config(path: Path) -> dict:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        return {}
    if not isinstance(data, dict):
        return {}
    if "USER_CONFIG" in data and isinstance(data["USER_CONFIG"], dict):
        return data["USER_CONFIG"]
    return data


def _save_yaml_user_config(path: Path, user_config: dict) -> None:
    payload = {"USER_CONFIG": user_config}
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _unwrap_optional(annotation):
    origin = get_origin(annotation)
    if origin is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _literal_from_union(annotation) -> Optional[list[str]]:
    origin = get_origin(annotation)
    if origin is Union:
        for arg in get_args(annotation):
            if _is_literal(arg):
                return _literal_options(arg)
    return None


def _list_literal_from_union(annotation) -> Optional[list[str]]:
    origin = get_origin(annotation)
    if origin is Union:
        for arg in get_args(annotation):
            opts = _is_list_of_literal(arg)
            if opts:
                return opts
    return None


def _is_literal(annotation) -> bool:
    return get_origin(annotation) is getattr(__import__("typing"), "Literal", None)


def _literal_options(annotation) -> list[str]:
    return [str(v) for v in get_args(annotation)]


def _is_list_of_literal(annotation) -> Optional[list[str]]:
    origin = get_origin(annotation)
    if origin is list:
        args = get_args(annotation)
        if args and _is_literal(args[0]):
            return _literal_options(args[0])
    if origin is not None and origin.__name__ == "List":
        args = get_args(annotation)
        if args and _is_literal(args[0]):
            return _literal_options(args[0])
    return None


def _is_enum(annotation) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, Enum)


def _enum_options(annotation) -> list[str]:
    return [str(v.value) for v in annotation]  # type: ignore[operator]


def _is_basemodel(annotation) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, BaseModel)


def _is_list_of_basemodel(annotation) -> Optional[type[BaseModel]]:
    origin = get_origin(annotation)
    if origin is list:
        args = get_args(annotation)
        if args and _is_basemodel(args[0]):
            return args[0]
    if origin is not None and origin.__name__ == "List":
        args = get_args(annotation)
        if args and _is_basemodel(args[0]):
            return args[0]
    return None


def _is_list_of_str(annotation) -> bool:
    origin = get_origin(annotation)
    if origin is list:
        args = get_args(annotation)
        return bool(args and args[0] is str)
    if origin is not None and origin.__name__ == "List":
        args = get_args(annotation)
        return bool(args and args[0] is str)
    return False


def _field_uses_columns(name: str) -> bool:
    if "columns" in name:
        return True
    return name in {"group_by", "group_cols", "sort_keys", "symbol_cols"}


def _field_short_doc(field) -> Optional[str]:
    if field is None:
        return None
    return getattr(field, "description", None)


def _field_long_doc(field) -> Optional[str]:
    if field is None:
        return None
    extra = getattr(field, "json_schema_extra", None) or {}
    if isinstance(extra, dict):
        return extra.get("long_doc")
    return None


def _yaml_from_value(value: Any) -> str:
    if value is PydanticUndefined:
        return yaml.safe_dump(None, sort_keys=False)
    return yaml.safe_dump(value, sort_keys=False)


def _parse_yaml(text: str) -> Any:
    return yaml.safe_load(text)


def _talib_function_help(name: str) -> str:
    meta = get_talib_function_metadata(name)
    if not meta:
        return "No TA-Lib metadata available for this function."
    lines = [f"{meta.get('description') or name}"]
    group = meta.get("group")
    if group:
        lines.append(f"Group: {group}")
    hint = meta.get("hint")
    if hint:
        lines.append(f"Hint: {hint}")
    params = meta.get("parameters") or []
    if params:
        param_bits = ", ".join(
            f"{p.get('name')}={p.get('default')}" for p in params if isinstance(p, dict)
        )
        if param_bits:
            lines.append(f"Parameters: {param_bits}")
    outputs = meta.get("output_names") or []
    if outputs:
        lines.append("Outputs: " + ", ".join(str(x) for x in outputs))
    return "\n".join(lines)


class RotatingText(Static):
    def __init__(self, items: list[str], prefix: str = "Metrics: ", sep: str = " • "):
        super().__init__("")
        self.items = list(items)
        self.prefix = prefix
        self.sep = sep
        self._offset = 0
        self._text = ""

    def on_mount(self) -> None:
        if self.items:
            random.shuffle(self.items)
            self._text = self.prefix + self.sep.join(self.items)
        else:
            self._text = f"{self.prefix}none"
        self.set_interval(0.15, self._tick)

    def _tick(self) -> None:
        width = max(self.size.width, 10)
        if len(self._text) <= width:
            self.update(self._text)
            return
        gap = "   "
        scroll = self._text + gap + self._text
        max_offset = len(self._text) + len(gap)
        if max_offset <= 0:
            self.update(self._text)
            return
        self._offset = (self._offset + 1) % max_offset
        window = scroll[self._offset : self._offset + width]
        self.update(window)


class PassSummaryBox(Vertical):
    DEFAULT_CSS = """
    PassSummaryBox {
        border: solid $primary;
        padding: 0;
        margin-bottom: 1;
    }
    PassSummaryBox > .summary-title {
        text-style: bold;
    }
    .metrics-row {
        margin-top: 1;
        height: 1;
    }
    .metrics-row > RotatingText {
        height: 1;
        color: $text-muted;
    }
    """

    def __init__(self, title: str, body: str, metrics_list: list[str]):
        super().__init__()
        self.border_title = title
        self._body = body
        self._metrics_list = metrics_list

    def compose(self) -> ComposeResult:
        yield Static(self._body)
        with Horizontal(classes="metrics-row"):
            yield Label("Analytics: ")
            yield RotatingText(self._metrics_list, prefix="")


class ModelEditor(Screen):
    CSS = """
    Screen { background: $surface; color: $text; }
    Vertical { height: auto; }
    Horizontal { height: auto; }
    .field-box { height: auto; margin-bottom: 1; }
    """

    def __init__(
        self,
        model_cls: type[BaseModel],
        data: dict,
        title: str,
        on_save,
    ):
        super().__init__()
        self.model_cls = model_cls
        self.data = data
        self.title = title
        self.on_save = on_save
        self.widgets: dict[str, Any] = {}
        self._widget_names: dict[str, str] = {}
        self._suppress_select_events = False
        self.status = Static("")
        self._skip_fields = {"PASSES"} if model_cls is AnalyticsConfig else set()
        self._section_filter: Optional[str] = None
        self._allowed_fields: Optional[set[str]] = None
        self._field_docs: dict[str, str] = {}
        self._model_doc: Optional[str] = None
        self._column_pick_map: dict[str, str] = {}
        self._schema_columns: Optional[list[str]] = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Label(self.title)
        model_doc = inspect.getdoc(self.model_cls) or ""
        if model_doc:
            lines = model_doc.splitlines()
            yield Label(lines[0])
            if len(lines) > 1:
                self._model_doc = model_doc
                yield Button("Details", id="details_model")
        with VerticalScroll(id="form"):
            if self.model_cls is AnalyticsConfig:
                for section, fields in _iter_global_sections(self.model_cls):
                    if self._section_filter and section != self._section_filter:
                        continue
                    yield Label(f"[{section}]")
                    for name in fields:
                        if name in self._skip_fields:
                            continue
                        field = self.model_cls.model_fields.get(name)  # type: ignore[attr-defined]
                        if field is None:
                            continue
                        annotation = _unwrap_optional(field.annotation)
                        value = self.data.get(name, field.default)
                        yield from self._render_field(name, annotation, value)
            else:
                for name, field in self.model_cls.model_fields.items():  # type: ignore[attr-defined]
                    if name in self._skip_fields:
                        continue
                    if (
                        self._allowed_fields is not None
                        and name not in self._allowed_fields
                    ):
                        continue
                    annotation = _unwrap_optional(field.annotation)
                    value = self.data.get(name, field.default)
                    yield from self._render_field(name, annotation, value)
        with Horizontal():
            yield Button("Save", id="save", variant="primary")
            yield Button("Cancel", id="cancel")
        yield self.status
        yield Footer()

    def _render_field(self, name: str, annotation, value) -> ComposeResult:
        if value is PydanticUndefined:
            field_info = self.model_cls.model_fields.get(name)  # type: ignore[attr-defined]
            if field_info and getattr(field_info, "default_factory", None):
                try:
                    value = field_info.default_factory()
                except Exception:
                    value = None
            else:
                origin = get_origin(annotation)
                if origin is list:
                    value = []
                else:
                    value = None
        if not self._should_render_field(name, annotation):
            return
        label = Label(name)
        if annotation is bool:
            widget = Checkbox(value=bool(value))
        elif self.model_cls is TalibIndicatorConfig and name == "name":
            options = list_talib_functions() if TALIB_AVAILABLE else []
            if not options:
                widget = Input(value="" if value is None else str(value))
            else:
                current = value if value in options else options[0]
                widget = Select([(o, o) for o in options], value=current)
        elif (union_lit := _literal_from_union(annotation)) is not None:
            # If the type allows a list of literals, render as multi-select checkboxes
            if (union_list_lit := _list_literal_from_union(annotation)) is not None:
                widget = []
                current = set(value or [])
                for opt in union_list_lit:
                    cb = Checkbox(opt, value=opt in current)
                    widget.append(cb)
            else:
                if len(union_lit) == 1:
                    self.data[name] = union_lit[0] if value is None else value
                    return
                if isinstance(value, list):
                    value = value[0] if value else None
                widget = Select(
                    [(o, o) for o in union_lit],
                    value=str(value) if value is not None else union_lit[0],
                )
        elif (union_list_lit := _list_literal_from_union(annotation)) is not None:
            widget = []
            current = {str(v) for v in (value or [])}
            for opt in union_list_lit:
                cb = Checkbox(opt, value=opt in current)
                widget.append(cb)
        elif _is_literal(annotation):
            options = _literal_options(annotation)
            if len(options) == 1:
                self.data[name] = options[0] if value is None else value
                return
            if isinstance(value, list):
                value = value[0] if value else None
            widget = Select(
                [(o, o) for o in options],
                value=str(value) if value is not None else options[0],
            )
        elif (list_literal := _is_list_of_literal(annotation)) is not None:
            widget = []
            current = {str(v) for v in (value or [])}
            for opt in list_literal:
                cb = Checkbox(opt, value=opt in current)
                widget.append(cb)
        elif _is_enum(annotation):
            options = _enum_options(annotation)
            if len(options) == 1:
                self.data[name] = options[0] if value is None else value
                return
            current = value.value if hasattr(value, "value") else value
            widget = Select(
                [(o, o) for o in options],
                value=str(current) if current is not None else options[0],
            )
        elif _is_list_of_str(annotation) and _field_uses_columns(name):
            widget = TextArea(text=_yaml_from_value(value), language="yaml")
            widget.styles.height = 4
            self._column_pick_map[f"pick_{name}"] = name
        elif self.model_cls is TalibIndicatorConfig and name == "parameters":
            initial = "{}" if value in (None, PydanticUndefined) else value
            text = (
                json.dumps(initial, indent=2, sort_keys=True)
                if isinstance(initial, dict)
                else _yaml_from_value(initial)
            )
            widget = TextArea(text=text, language="json")
            widget.styles.height = 6
        elif annotation in (int, float, str):
            widget = Input(value="" if value is None else str(value))
        elif _is_list_of_basemodel(annotation) is not None:
            widget = Button("Edit list", id=f"editlist_{name}")
        elif _is_basemodel(annotation):
            widget = Button("Edit", id=f"edit_{name}")
        else:
            widget = TextArea(text=_yaml_from_value(value), language="yaml")
            widget.styles.height = 5

        if not isinstance(widget, list):
            if getattr(widget, "id", None) is None:
                widget.id = f"field_{name}"
            self._widget_names[widget.id] = name
        self.widgets[name] = (widget, annotation)
        with Vertical(classes="field-box"):
            yield Label(f"--")
            yield label
            if isinstance(widget, list):
                for cb in widget:
                    yield cb
            else:
                yield widget
            pick_id = f"pick_{name}"
            if pick_id in self._column_pick_map:
                yield Button("Pick columns", id=pick_id)
            if field_info := self.model_cls.model_fields.get(name):  # type: ignore[attr-defined]
                short_doc = _field_short_doc(field_info)
                long_doc = _field_long_doc(field_info)
                if short_doc:
                    yield Label(f"{short_doc}", classes="help")
                if long_doc:
                    self._field_docs[name] = long_doc
                    yield Button("Details", id=f"details_{name}")
                if name == "output_name_pattern":
                    yield Label(
                        "Default: module-specific naming (leave empty to use default).",
                        classes="help",
                    )
            if self.model_cls is TalibIndicatorConfig and name == "name":
                selected = value
                if isinstance(widget, Select):
                    selected = widget.value
                yield Static(
                    Text(_talib_function_help(str(selected or ""))),
                    classes="help",
                )
            if self.model_cls is TalibIndicatorConfig and name == "parameters":
                yield Label(
                    "JSON object passed as TA-Lib kwargs (e.g. {\"fastperiod\": 12, \"slowperiod\": 26}).",
                    classes="help",
                )

    def _should_render_field(self, name: str, annotation) -> bool:
        field_info = self.model_cls.model_fields.get(name)  # type: ignore[attr-defined]
        if field_info:
            extra = getattr(field_info, "json_schema_extra", None) or {}
            depends_on = extra.get("depends_on") if isinstance(extra, dict) else None
            if depends_on:
                for dep_field, dep_values in depends_on.items():
                    current = None
                    if "." in dep_field:
                        root, leaf = dep_field.split(".", 1)
                        current = self.data.get(root)
                        if isinstance(current, BaseModel):
                            current = current.model_dump()
                        if isinstance(current, dict):
                            current = current.get(leaf)
                    else:
                        current = self.data.get(dep_field)
                    if hasattr(current, "value"):
                        current = current.value
                    if isinstance(dep_values, (list, tuple, set)):
                        allowed = list(dep_values)
                    else:
                        allowed = [dep_values]
                    if current not in allowed:
                        return False
        if self.model_cls is L3AdvancedConfig and name == "fleeting_threshold_ms":
            variant = self.data.get("variant")
            if isinstance(variant, list):
                return "FleetingLiquidityRatio" in variant
            return variant == "FleetingLiquidityRatio"
        if self.model_cls is GenericAnalyticsConfig and name == "talib_indicators":
            if not TALIB_AVAILABLE:
                return False
        return True

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id == "save":
            self._save()
        elif button_id == "cancel":
            self.app.pop_screen()
        elif button_id == "details_model":
            if self._model_doc:
                self.status.update(self._model_doc)
        elif button_id.startswith("details_"):
            field_name = button_id.split("_", 1)[1]
            doc = self._field_docs.get(field_name)
            if doc:
                self.status.update(doc)
        elif button_id.startswith("pick_"):
            field_name = button_id.split("_", 1)[1]
            widget, _ = self.widgets.get(field_name, (None, None))
            current = self.data.get(field_name, [])
            if isinstance(widget, TextArea):
                try:
                    current = _parse_yaml(widget.text) or []
                except Exception:
                    current = []

            def _on_pick(selected: list[str]):
                if isinstance(widget, TextArea):
                    widget.text = yaml.safe_dump(selected, sort_keys=False)
                self.data[field_name] = selected

            columns = self._get_schema_columns()
            self.app.push_screen(
                ColumnPicker(columns, current or [], _on_pick, f"Pick columns: {field_name}")
            )
        elif button_id.startswith("edit_"):
            field_name = button_id.split("_", 1)[1]
            widget, annotation = self.widgets[field_name]
            current = self.data.get(field_name, {})
            if _is_basemodel(annotation):

                def _on_save(updated):
                    self.data[field_name] = updated

                screen = ModelEditor(
                    annotation,
                    (
                        current
                        if isinstance(current, dict)
                        else getattr(current, "model_dump", lambda: {})()
                    ),
                    title=f"{self.title}.{field_name}",
                    on_save=_on_save,
                )
                self.app.push_screen(screen)
        elif button_id.startswith("editlist_"):
            field_name = button_id.split("_", 1)[1]
            widget, annotation = self.widgets[field_name]
            list_model = _is_list_of_basemodel(annotation)
            if list_model is None:
                return
            current = self.data.get(field_name, [])
            if not isinstance(current, list):
                current = []

            def _on_save(updated):
                self.data[field_name] = updated

            screen = ListEditor(
                list_model,
                current,
                title=f"{self.title}.{field_name}",
                on_save=_on_save,
            )
            self.app.push_screen(screen)

    def _get_schema_columns(self) -> list[str]:
        if self._schema_columns is not None:
            return self._schema_columns
        config = None
        try:
            app_cfg = getattr(self.app, "config_data", None)
            if app_cfg:
                config = AnalyticsConfig.model_validate(app_cfg)
        except Exception:
            config = None
        if config is None:
            config = AnalyticsConfig()
        try:
            schema = get_output_schema(config)
        except Exception:
            schema = get_full_output_schema()
        cols: list[str] = []
        for values in schema.values():
            if isinstance(values, list):
                cols.extend(values)
        self._schema_columns = sorted(set(cols))
        return self._schema_columns

    def on_select_changed(self, event: Select.Changed) -> None:
        widget_id = event.select.id
        if not widget_id:
            return
        field_name = self._widget_names.get(widget_id)
        if not field_name:
            return
        self.data[field_name] = event.value
        if self._suppress_select_events:
            return
        self._suppress_select_events = True

        def _recompose():
            self.refresh(recompose=True)

            def _clear():
                self._suppress_select_events = False

            self.call_after_refresh(_clear)

        self.call_after_refresh(_recompose)

    def _save(self) -> None:
        raw = dict(self.data) if isinstance(self.data, dict) else {}
        for name, (widget, annotation) in self.widgets.items():
            if isinstance(widget, Checkbox):
                raw[name] = widget.value
            elif isinstance(widget, Select):
                raw[name] = widget.value
            elif isinstance(widget, Input):
                text = widget.value
                if annotation is int:
                    raw[name] = int(text) if text != "" else None
                elif annotation is float:
                    raw[name] = float(text) if text != "" else None
                else:
                    raw[name] = text
            elif isinstance(widget, TextArea):
                try:
                    raw[name] = _parse_yaml(widget.text)
                except Exception as exc:
                    self.status.update(f"Invalid YAML for {name}: {exc}")
                    return
            elif isinstance(widget, list):
                raw[name] = [str(cb.label) for cb in widget if cb.value]
            elif isinstance(widget, Button) and _is_basemodel(annotation):
                raw[name] = self.data.get(name, {})
            elif (
                isinstance(widget, Button)
                and _is_list_of_basemodel(annotation) is not None
            ):
                raw[name] = self.data.get(name, [])

        try:
            model = self.model_cls.model_validate(raw)  # type: ignore[attr-defined]
        except ValidationError as exc:
            self.status.update(str(exc))
            return
        self.on_save(model.model_dump())
        self.app.pop_screen()


class ListEditor(Screen):
    def __init__(self, model_cls: type[BaseModel], items: list, title: str, on_save):
        super().__init__()
        self.model_cls = model_cls
        self.items = items
        self.title = title
        self.on_save = on_save
        self.status = Static("")

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Label(self.title)
        with VerticalScroll():
            for idx, item in enumerate(self.items):
                label = (
                    item.get("metric_type", f"item{idx+1}")
                    if isinstance(item, dict)
                    else f"item{idx+1}"
                )
                yield Button(f"Edit {label}", id=f"edit_{idx}")
                yield Button(f"Remove {label}", id=f"remove_{idx}")
        with Horizontal():
            yield Button("Add", id="add")
            yield Button("Back", id="back")
        yield self.status
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id == "back":
            self.app.pop_screen()
            return
        if button_id == "add":

            def _on_save(updated):
                self.items.append(updated)
                self.on_save(self.items)

            screen = ModelEditor(self.model_cls, {}, f"{self.title}.new", _on_save)
            self.app.push_screen(screen)
            return
        if button_id.startswith("edit_"):
            idx = int(button_id.split("_", 1)[1])
            current = self.items[idx] if idx < len(self.items) else {}

            def _on_save(updated):
                self.items[idx] = updated
                self.on_save(self.items)

            screen = ModelEditor(
                self.model_cls, current, f"{self.title}[{idx}]", _on_save
            )
            self.app.push_screen(screen)
            return
        if button_id.startswith("remove_"):
            idx = int(button_id.split("_", 1)[1])
            if idx < len(self.items):
                self.items.pop(idx)
                self.on_save(self.items)
            self.app.pop_screen()
            self.app.push_screen(
                ListEditor(self.model_cls, self.items, self.title, self.on_save)
            )


class ColumnPicker(Screen):
    def __init__(self, columns: list[str], selected: list[str], on_save, title: str):
        super().__init__()
        self.columns = columns
        self.selected = set(selected or [])
        self.on_save = on_save
        self.title = title
        self._checkboxes: list[Checkbox] = []

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Label(self.title)
        with VerticalScroll():
            if not self.columns:
                yield Label("No schema columns available.")
            for col in self.columns:
                cb = Checkbox(col, value=col in self.selected)
                self._checkboxes.append(cb)
                yield cb
        with Horizontal():
            yield Button("Use selected", id="save", variant="primary")
            yield Button("Cancel", id="cancel")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel":
            self.app.pop_screen()
            return
        if event.button.id == "save":
            picked = [cb.label for cb in self._checkboxes if cb.value]
            self.on_save(picked)
            self.app.pop_screen()


class ConfigEditor(App):
    CSS = """
    Screen { background: $surface; color: $text; }
    Vertical { height: auto; }
    Horizontal { height: auto; }
    """
    BINDINGS = [
        ("ctrl+s", "save", "Save"),
        ("ctrl+q", "quit", "Quit"),
        ("ctrl+a", "toggle_advanced", "Toggle Advanced Modules"),
        ("ctrl+b", "edit_passes", "Edit Passes"),
        ("ctrl+l", "toggle_pass_lock", "Toggle Pass Lock"),
        ("ctrl+c", "edit_section('Core')", "Core"),
        ("ctrl+o", "edit_section('Outputs')", "Outputs"),
        ("ctrl+u", "edit_section('Automation')", "Automation"),
        ("ctrl+e", "edit_section('PerformanceAndExecutionEnvironment')", "Performance"),
        ("ctrl+g", "edit_section('Advanced')", "Advanced"),
    ]

    def __init__(self, yaml_path: Path, initial_config: dict):
        super().__init__()
        self.yaml_path = yaml_path
        self.config_data = initial_config
        self.status = Static("")
        self.summary = Vertical(id="summary")
        self.advanced_modules = False
        self.pass_readonly = True

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("AnalyticsConfig")
        yield self.summary
        with Vertical():
            for section, _ in _iter_global_sections(AnalyticsConfig):
                label = section
                if section == "Core":
                    label = "Edit Core (Ctrl+C)"
                elif section == "Outputs":
                    label = "Edit Outputs (Ctrl+O)"
                elif section == "Automation":
                    label = "Edit Automation (Ctrl+U)"
                elif section == "PerformanceAndExecutionEnvironment":
                    label = "Edit Performance (Ctrl+E)"
                elif section == "Advanced":
                    label = "Edit Advanced (Ctrl+G)"
                yield Button(label, id=f"edit_section_{section}", variant="primary")
            yield Button("Edit Passes (Ctrl+B)", id="edit_passes")
        yield self.status
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id.startswith("edit_section_"):
            section = button_id.replace("edit_section_", "", 1)
            self._open_section_editor(section)
        elif button_id == "edit_passes":
            self._open_passes_editor()

    def action_save(self) -> None:
        try:
            model = AnalyticsConfig.model_validate(self.config_data)
        except ValidationError as exc:
            self.status.update(str(exc))
            return
        _save_yaml_user_config(self.yaml_path, model.model_dump())
        self.status.update(f"Saved: {self.yaml_path}")

    def action_toggle_advanced(self) -> None:
        self.advanced_modules = not self.advanced_modules
        self._refresh_summary()

    def action_edit_passes(self) -> None:
        self._open_passes_editor()

    def action_edit_section(self, section: str) -> None:
        self._open_section_editor(section)

    def on_mount(self) -> None:
        self._refresh_summary()

    def on_screen_resume(self) -> None:
        self._refresh_summary()

    def _open_section_editor(self, section: str) -> None:
        def _on_save(updated):
            self.config_data.update(updated)
            self._refresh_summary()

        screen = ModelEditor(
            AnalyticsConfig,
            self.config_data,
            f"AnalyticsConfig.{section}",
            _on_save,
        )
        screen._section_filter = section
        self.push_screen(screen)

    def _open_passes_editor(self) -> None:
        screen = PassListEditor(
            self.config_data,
            advanced_modules=self.advanced_modules,
            pass_readonly=self.pass_readonly,
            on_readonly_change=self._set_pass_readonly,
        )
        self.push_screen(screen)

    def _refresh_summary(self) -> None:
        if not self.summary.is_attached:
            return
        self.summary.remove_children()
        data = self.config_data or {}
        dataset = data.get("DATASETNAME", "unknown")
        passes = data.get("PASSES", []) or []
        area = "unknown"
        output_target = data.get("OUTPUT_TARGET") or {}
        if isinstance(output_target, OutputTarget):
            output_target = output_target.model_dump()
        if isinstance(output_target, dict):
            area = output_target.get("area", "unknown")
        universe = data.get("UNIVERSE") or "default"
        start_date = data.get("START_DATE") or "default"
        end_date = data.get("END_DATE") or "default"
        header = (
            f"Dataset name: {dataset}\n"
            f"Area: {area}\n"
            f"Default universe: {universe}\n"
            f"Default dates: {start_date} -> {end_date}\n"
            f"Passes: {len(passes)}\n"
            f"Pass lock: {'ON' if self.pass_readonly else 'OFF'} (Ctrl+L)"
        )
        self.summary.mount(Static(Panel(header, title="Summary", border_style="cyan")))
        for idx, p in enumerate(passes):
            name = p.get("name", f"pass{idx+1}")
            time_bucket = int(p.get("time_bucket_seconds", 60))
            if p.get("sort_keys"):
                grouping = p.get("sort_keys")
            elif "generic" in (p.get("modules") or []):
                grouping = (p.get("generic_analytics") or {}).get(
                    "group_by", ["ListingId", "TimeBucket"]
                )
            else:
                grouping = ["ListingId", "TimeBucket"]
            grouping_label = ",".join(grouping)
            title = f"{name}"
            modules = p.get("modules", []) or []
            modules_str = ", ".join(modules) if modules else "none"
            metrics = _pass_metric_count(p)
            input_driver = data.get("PREPARE_DATA_MODE", "unknown")
            output_target = p.get("output") or data.get("OUTPUT_TARGET")
            output_driver = _format_output_driver(output_target, data)
            inputs_str = _pass_inputs_summary(p)
            timeline_mode = _timeline_mode_label(_derive_timeline_mode(p))
            body = "\n".join(
                [
                    f"Modules: {modules_str}",
                    f"Metrics: {metrics}",
                    f"Input driver: {input_driver}",
                    f"Inputs: {inputs_str}",
                    f"Output driver: {output_driver}",
                    f"Timeline mode: {timeline_mode}",
                    f"Time bucket: {int(p.get('time_bucket_seconds', 60))}s",
                    f"Grouping: {grouping_label}",
                ]
            )
            metrics_list = _pass_metric_list(p)
            self.summary.mount(PassSummaryBox(title, body, metrics_list))

    def _set_pass_readonly(self, value: bool) -> None:
        self.pass_readonly = value

    def action_toggle_pass_lock(self) -> None:
        self.pass_readonly = not self.pass_readonly


_MODULE_META_CACHE: Optional[tuple[dict, dict, dict]] = None
_MODULE_REQUIRES_CACHE: Optional[dict[str, list[str]]] = None


def _module_model_for_field(field_name: str):
    if field_name.startswith("extension:"):
        key = field_name.split(":", 1)[1]
        return get_plugin_module_config_models().get(key)
    field = PassConfig.model_fields.get(field_name)  # type: ignore[attr-defined]
    if field is None:
        return None
    return _unwrap_optional(field.annotation)


def _get_module_config_data(pass_data: dict, field_name: str) -> dict:
    if field_name.startswith("extension:"):
        key = field_name.split(":", 1)[1]
        ext = pass_data.get("extension_configs", {})
        if hasattr(ext, "model_dump"):
            ext = ext.model_dump()
        if not isinstance(ext, dict):
            ext = {}
        cfg = ext.get(key, {})
    else:
        cfg = pass_data.get(field_name, {})
    if hasattr(cfg, "model_dump"):
        cfg = cfg.model_dump()
    if not isinstance(cfg, dict):
        return {}
    return dict(cfg)


def _set_module_config_data(pass_data: dict, field_name: str, value: dict) -> None:
    if field_name.startswith("extension:"):
        key = field_name.split(":", 1)[1]
        ext = pass_data.get("extension_configs", {})
        if hasattr(ext, "model_dump"):
            ext = ext.model_dump()
        if not isinstance(ext, dict):
            ext = {}
        ext[key] = value
        pass_data["extension_configs"] = ext
        return
    pass_data[field_name] = value


def _module_meta() -> tuple[dict, dict, dict]:
    global _MODULE_META_CACHE
    if _MODULE_META_CACHE is not None:
        return _MODULE_META_CACHE

    module_info: dict[str, dict] = {}
    field_map: dict[str, list[str]] = {}
    schema_keys: dict[str, list[str]] = {}

    for field_name, field in PassConfig.model_fields.items():  # type: ignore[attr-defined]
        annotation = _unwrap_optional(field.annotation)
        if not _is_basemodel(annotation):
            continue
        ui = (
            getattr(annotation, "model_config", {})
            .get("json_schema_extra", {})
            .get("ui")
        )
        if not isinstance(ui, dict):
            continue
        module_name = ui.get("module")
        if not module_name:
            continue
        meta = module_info.setdefault(
            module_name,
            {
                "desc": "",
                "columns": [],
                "tier": "core",
            },
        )
        if ui.get("desc") and not meta["desc"]:
            meta["desc"] = ui["desc"]
        outputs = ui.get("outputs")
        if outputs and not meta["columns"]:
            meta["columns"] = list(outputs)
        if ui.get("tier"):
            meta["tier"] = ui["tier"]
        field_map.setdefault(module_name, []).append(field_name)
        keys = ui.get("schema_keys") or []
        if module_name not in schema_keys:
            schema_keys[module_name] = []
        for key in keys:
            if key not in schema_keys[module_name]:
                schema_keys[module_name].append(key)

    for spec in get_plugin_module_configs():
        module_name = spec.get("module")
        config_key = spec.get("config_key") or module_name
        model_cls = spec.get("model")
        if not isinstance(module_name, str) or not module_name:
            continue
        if not isinstance(config_key, str) or not config_key:
            continue
        if not _is_basemodel(model_cls):
            continue
        ui = (
            getattr(model_cls, "model_config", {})
            .get("json_schema_extra", {})
            .get("ui")
        )
        if not isinstance(ui, dict):
            continue
        meta = module_info.setdefault(
            module_name,
            {
                "desc": "",
                "columns": [],
                "tier": "extension",
            },
        )
        if ui.get("desc") and not meta["desc"]:
            meta["desc"] = ui["desc"]
        outputs = ui.get("outputs")
        if outputs and not meta["columns"]:
            meta["columns"] = list(outputs)
        if ui.get("tier"):
            meta["tier"] = ui["tier"]
        field_map.setdefault(module_name, []).append(f"extension:{config_key}")
        keys = ui.get("schema_keys") or []
        if module_name not in schema_keys:
            schema_keys[module_name] = []
        for key in keys:
            if key not in schema_keys[module_name]:
                schema_keys[module_name].append(key)

    _MODULE_META_CACHE = (module_info, field_map, schema_keys)
    return _MODULE_META_CACHE


def _module_requires_map() -> dict[str, list[str]]:
    global _MODULE_REQUIRES_CACHE
    if _MODULE_REQUIRES_CACHE is not None:
        return _MODULE_REQUIRES_CACHE
    try:
        from basalt.analytics_registry import get_registered_entries

        entries = get_registered_entries()
        _MODULE_REQUIRES_CACHE = {
            name: list(getattr(entry.cls, "REQUIRES", []) or [])
            for name, entry in entries.items()
        }
    except Exception:
        _MODULE_REQUIRES_CACHE = {}
    return _MODULE_REQUIRES_CACHE


def _module_inputs_line(pass_data: dict, module_key: str) -> str:
    requires = _module_requires_map().get(module_key, [])
    pairs = _module_input_pairs(pass_data, module_key, requires)
    if not pairs:
        return "Inputs: none"
    return "Inputs = [" + ", ".join(
        f"({req.upper()}:{source})" for req, source in pairs
    ) + "]"


def _module_input_pairs(
    pass_data: dict,
    module_key: str,
    requires: Optional[list[str]] = None,
) -> list[tuple[str, str]]:
    reqs = (
        requires if requires is not None else _module_requires_map().get(module_key, [])
    )
    if reqs:
        module_inputs = pass_data.get("module_inputs", {})
        entry = module_inputs.get(module_key) if isinstance(module_inputs, dict) else None
        if isinstance(entry, str) and len(reqs) == 1:
            return [(reqs[0], entry)]
        if isinstance(entry, dict):
            return [(req, str(entry.get(req, req))) for req in reqs]
        return [(req, req) for req in reqs]
    module_inputs = pass_data.get("module_inputs", {})
    entry = module_inputs.get(module_key) if isinstance(module_inputs, dict) else None
    if isinstance(entry, str):
        return [("df", entry)]
    if isinstance(entry, dict):
        return [(str(req), str(source)) for req, source in entry.items()]
    return _module_source_config_pairs(pass_data, module_key)


def _module_source_config_specs(module_key: str) -> list[tuple[str, str, str]]:
    _, field_map, _ = _module_meta()
    specs: list[tuple[str, str, str]] = []
    for field_name in field_map.get(module_key, []):
        model_cls = _module_model_for_field(field_name)
        model_fields = getattr(model_cls, "model_fields", {})
        for key in model_fields:
            label: Optional[str] = None
            if key == "source_pass":
                label = "df"
            elif key.endswith("_pass"):
                label = key[: -len("_pass")]
            elif key.endswith("_context_key"):
                label = key[: -len("_context_key")]
            if label:
                specs.append((field_name, key, label))
    return specs


def _module_source_default(field_name: str, config_key: str) -> Optional[str]:
    model_cls = _module_model_for_field(field_name)
    model_fields = getattr(model_cls, "model_fields", {})
    source_field = model_fields.get(config_key)
    if source_field is None:
        return None
    default = getattr(source_field, "default", None)
    if isinstance(default, str) and default:
        return default
    default_factory = getattr(source_field, "default_factory", None)
    if default_factory is None:
        return None
    try:
        value = default_factory()
    except Exception:
        return None
    return value if isinstance(value, str) and value else None


def _module_source_config_pairs(pass_data: dict, module_key: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for field_name, config_key, input_name in _module_source_config_specs(module_key):
        cfg = _get_module_config_data(pass_data, field_name)
        source = cfg.get(config_key)
        if not isinstance(source, str) or not source:
            source = _module_source_default(field_name, config_key)
        if isinstance(source, str) and source:
            pairs.append((input_name, source))

    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for input_name, source in pairs:
        key = input_name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append((input_name, source))
    return out


def _module_source_config_updates(
    selected: dict[tuple[str, str, str], str],
) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for (_module, field_name, config_key), source in selected.items():
        if not source:
            continue
        out.setdefault(field_name, {})[config_key] = source
    return out


def _input_source_choices(config_data: dict, pass_index: Optional[int] = None) -> list[str]:
    sources = list(ALL_TABLES.keys())
    for idx, p in enumerate(config_data.get("PASSES", [])):
        if not isinstance(p, dict):
            continue
        name = p.get("name")
        if not name:
            continue
        if pass_index is not None and idx >= pass_index:
            continue
        if name not in sources:
            sources.append(name)
    return sorted(sources)


def _build_module_inputs_from_selection(
    selected: dict[tuple[str, str], str],
    module_requires: dict[str, list[str]],
) -> dict:
    out: dict[str, Any] = {}
    for module, requires in module_requires.items():
        if not requires:
            extras = []
            for (mod_name, req), source in selected.items():
                if mod_name != module:
                    continue
                if not source:
                    continue
                if source == req:
                    continue
                extras.append((req, source))
            if len(extras) == 1 and extras[0][0].lower() == "df":
                out[module] = extras[0][1]
            elif extras:
                out[module] = {req: source for req, source in extras}
            continue
        chosen = {req: selected.get((module, req), req) for req in requires}
        if len(requires) == 1:
            req = requires[0]
            if chosen[req] != req:
                out[module] = chosen[req]
            continue
        if any(chosen[req] != req for req in requires):
            out[module] = chosen
    return out


def _pass_inputs_summary(pass_data: dict) -> str:
    modules = list(pass_data.get("modules", []) or [])
    if not modules:
        return "none"
    parts = []
    for module in modules:
        line = _module_inputs_line(pass_data, module)
        if line.endswith("none"):
            continue
        parts.append(f"{module}:{line.replace('Inputs = ', '')}")
    return " | ".join(parts) if parts else "none"


def _derive_timeline_mode(pass_data: dict) -> str:
    mode = pass_data.get("timeline_mode")
    if isinstance(mode, str) and mode:
        return mode
    modules = set(pass_data.get("modules", []) or [])
    if "external_events" in modules or "events" in modules:
        return "event"
    if "dense" in modules:
        return "dense"
    return "dense"


def _timeline_mode_label(mode: str) -> str:
    labels = {
        "sparse_original": "sparse original",
        "sparse_digitised": "sparse digitised",
        "dense": "dense",
        "event": "event",
    }
    return labels.get(mode, mode)


def _external_events_preview(pass_data: dict, axis_width: int = 51) -> str:
    cfg = pass_data.get("external_event_analytics", {}) if isinstance(pass_data, dict) else {}
    if hasattr(cfg, "model_dump"):
        cfg = cfg.model_dump()
    if not isinstance(cfg, dict):
        cfg = {}
    radius = float(cfg.get("radius_seconds", 0.0) or 0.0)
    directions = str(cfg.get("directions", "both") or "both")
    extra = int(cfg.get("extra_observations", 0) or 0)
    scale = float(cfg.get("scale_factor", 1.0) or 1.0)
    if radius <= 0 or extra <= 0:
        return "External events context: anchors only (no before/after context)."
    try:
        from basalt.time.external_events import ExternalEventsAnalytics

        offsets = ExternalEventsAnalytics.build_context_offsets(
            radius_seconds=radius,
            directions=directions,
            extra_observations=extra,
            scale_factor=scale,
        )
    except Exception:
        return (
            "External events context: "
            f"radius={radius:g}s, directions={directions}, extra={extra}, scale_factor={scale:g}."
        )

    offsets_sorted = sorted(offsets, key=lambda x: x[1])
    values = " ".join(f"{delta:g}s" for _, delta in offsets_sorted)
    span = max(abs(radius), 1e-9)
    width = max(21, axis_width)
    if width % 2 == 0:
        width -= 1
    axis = [" "] * width
    center = width // 2
    for _, delta in offsets_sorted:
        pos = int(round((delta / span) * center)) + center
        pos = max(0, min(width - 1, pos))
        axis[pos] = "."
    # Keep the event anchor visible even when delta=0 is also a sampled point.
    axis[center] = "|"
    scale_txt = "arithmetic" if scale <= 1.0 else f"geometric(base={scale:g})"
    return (
        f"External events context [{directions}, radius={radius:g}s, extra={extra}, {scale_txt}]\n"
        f"[{''.join(axis)}]\n"
        f"{values}"
    )


def _render_external_events_preview(pass_data: dict, axis_width: int = 51) -> Text:
    raw = _external_events_preview(pass_data, axis_width=axis_width)
    lines = raw.splitlines()
    if len(lines) < 3:
        return Text(raw)
    rendered = Text()
    rendered.append(lines[0], style="bold")
    rendered.append("\n")
    rendered.append(lines[1], style="bold cyan")
    rendered.append("\n")
    rendered.append(lines[2], style="dim")
    for extra in lines[3:]:
        rendered.append("\n")
        rendered.append(extra, style="dim")
    return rendered


class PassListEditor(Screen):
    def __init__(
        self,
        config_data: dict,
        advanced_modules: bool = False,
        pass_readonly: bool = True,
        on_readonly_change=None,
    ):
        super().__init__()
        self.config_data = config_data
        self.advanced_modules = advanced_modules
        self.pass_readonly = pass_readonly
        self.on_readonly_change = on_readonly_change
        self.status = Static("")

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Label("PASSES")
        yield Label("Pass lock: use Ctrl+L to toggle")
        with Vertical():
            for idx, p in enumerate(self.config_data.get("PASSES", [])):
                name = p.get("name", f"pass{idx+1}")
                time_bucket = int(p.get("time_bucket_seconds", 60))
                if p.get("sort_keys"):
                    grouping = p.get("sort_keys")
                elif "generic" in (p.get("modules") or []):
                    grouping = (p.get("generic_analytics") or {}).get(
                        "group_by", ["ListingId", "TimeBucket"]
                    )
                else:
                    grouping = ["ListingId", "TimeBucket"]
                grouping_label = ",".join(grouping)
                title = f"{name}"
                modules = p.get("modules", []) or []
                modules_str = ", ".join(modules) if modules else "none"
                metrics = _pass_metric_count(p)
                input_driver = self.config_data.get("PREPARE_DATA_MODE", "unknown")
                output_target = p.get("output") or self.config_data.get("OUTPUT_TARGET")
                output_driver = _format_output_driver(output_target, self.config_data)
                inputs_str = _pass_inputs_summary(p)
                timeline_mode = _timeline_mode_label(_derive_timeline_mode(p))
                body = "\n".join(
                    [
                        f"Modules: {modules_str}",
                        f"Metrics: {metrics}",
                        f"Input driver: {input_driver}",
                        f"Inputs: {inputs_str}",
                        f"Output driver: {output_driver}",
                        f"Timeline mode: {timeline_mode}",
                        f"Time bucket: {int(p.get('time_bucket_seconds', 60))}s",
                        f"Grouping: {grouping_label}",
                    ]
                )
                metrics_list = _pass_metric_list(p)
                yield PassSummaryBox(title, body, metrics_list)
                yield Button(f"Edit {name}", id=f"edit_{idx}")
        with Horizontal():
            yield Button("Add Pass", id="add")
            yield Button("Back", id="back")
        yield self.status
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id == "back":
            self.app.pop_screen()
            if hasattr(self.app, "_refresh_summary"):
                self.app._refresh_summary()
        elif button_id == "add":
            self._ensure_unlocked(self._open_pass_editor)
        elif button_id.startswith("edit_"):
            idx = int(button_id.split("_", 1)[1])
            self._ensure_unlocked(lambda: self._edit_pass(idx))

    def _ensure_unlocked(self, action) -> None:
        if not self.pass_readonly:
            action()
            return

        def _on_confirm():
            self.pass_readonly = False
            if self.on_readonly_change:
                self.on_readonly_change(False)
            action()

        self.app.push_screen(
            ConfirmScreen(
                "Passes are in read-only mode. Editing will change the process. Continue?",
                on_confirm=_on_confirm,
            )
        )

    def _open_pass_editor(self) -> None:
        def _on_save(updated):
            self.config_data.setdefault("PASSES", []).append(updated)
            if hasattr(self.app, "_refresh_summary"):
                self.app._refresh_summary()

        screen = PassEditor(
            {},
            "PassConfig",
            _on_save,
            input_sources=_input_source_choices(self.config_data, pass_index=None),
            advanced_modules=self.advanced_modules,
            pass_readonly=self.pass_readonly,
            on_readonly_change=self.on_readonly_change,
        )
        self.app.push_screen(screen)

    def _edit_pass(self, idx: int) -> None:
        passes = self.config_data.get("PASSES", [])
        current = passes[idx] if idx < len(passes) else {}

        def _on_save(updated):
            passes[idx] = updated
            self.config_data["PASSES"] = passes
            if hasattr(self.app, "_refresh_summary"):
                self.app._refresh_summary()

        screen = PassEditor(
            current,
            "PassConfig",
            _on_save,
            input_sources=_input_source_choices(self.config_data, pass_index=idx),
            advanced_modules=self.advanced_modules,
            pass_readonly=self.pass_readonly,
            on_readonly_change=self.on_readonly_change,
        )
        self.app.push_screen(screen)


class PassEditor(Screen):
    def __init__(
        self,
        data: dict,
        title: str,
        on_save,
        input_sources: Optional[list[str]] = None,
        advanced_modules: bool = False,
        pass_readonly: bool = True,
        on_readonly_change=None,
    ):
        super().__init__()
        self.data = data
        self.title = title
        self.on_save = on_save
        self.input_sources = input_sources or sorted(list(ALL_TABLES.keys()))
        self.advanced_modules = advanced_modules
        self.pass_readonly = pass_readonly
        self.on_readonly_change = on_readonly_change
        self.status = Static("")
        self.widgets: dict[str, Any] = {}
        self._full_schema_counts: Optional[dict[str, int]] = None
        self._module_edit_map: dict[str, str] = {}
        self._render_token = 0
        self._timeline_render_token = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Label(self.title)
        with VerticalScroll():
            yield Label("Pass type")
            default_pass_type = self.data.get("_pass_type")
            if default_pass_type is None:
                modules = self.data.get("modules", [])
                module_info, _, _ = _module_meta()
                tiers = {
                    module_info.get(name if name != "trades" else "trade", {}).get(
                        "tier"
                    )
                    for name in modules
                    if module_info.get(name if name != "trades" else "trade")
                }
                if tiers and tiers <= {"post"}:
                    default_pass_type = "post"
                elif tiers and tiers <= {"pre"}:
                    default_pass_type = "pre"
                else:
                    default_pass_type = "core"
            pass_type = Select(
                [
                    ("Preprocessing", "pre"),
                    ("Core", "core"),
                    ("Postprocessing", "post"),
                ],
                value=default_pass_type,
            )
            pass_type.id = "pass_type"
            self.widgets["pass_type"] = pass_type
            yield pass_type

            yield Label("Name")
            name = Input(value=str(self.data.get("name", "")))
            self.widgets["name"] = name
            yield name

            yield Label("Time bucket seconds")
            tbs = Input(value=str(self.data.get("time_bucket_seconds", 60)))
            self.widgets["time_bucket_seconds"] = tbs
            yield tbs

            default_timeline = _derive_timeline_mode(self.data)
            yield Label("Timeline mode")
            timeline_mode = Select(
                [
                    ("Sparse original (TimeBucket=timestamp)", "sparse_original"),
                    ("Sparse digitised (TimeBucket=ceil timestamp)", "sparse_digitised"),
                    ("Dense", "dense"),
                    ("External events", "event"),
                ],
                value=default_timeline,
            )
            timeline_mode.id = "timeline_mode"
            self.widgets["timeline_mode"] = timeline_mode
            yield timeline_mode
            timeline_controls = Vertical(id="timeline_controls")
            self.widgets["timeline_controls"] = timeline_controls
            yield timeline_controls

            yield Label("Modules (purpose and key outputs)")
            self.widgets["modules"] = []
            self.widgets["module_edit_buttons"] = {}
            modules_box = Vertical(id="modules_box")
            self.widgets["modules_box"] = modules_box
            yield modules_box

            yield Label("Advanced options")
            yield Button("Edit full PassConfig", id="edit_full")
            yield Button("Edit input mappings", id="edit_inputs")
        with Horizontal():
            yield Button("Save", id="save", variant="primary")
            yield Button("Cancel", id="cancel")
        yield self.status
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id == "cancel":
            self.app.pop_screen()
            return
        if button_id == "edit_full":

            def _on_save(updated):
                self.data.update(updated)

            selected_modules = (
                self.data.get("modules", []) if isinstance(self.data, dict) else []
            )
            allowed = {
                "name",
                "time_bucket_seconds",
                "time_bucket_anchor",
                "time_bucket_closed",
                "sort_keys",
                "timeline_mode",
                "modules",
                "module_inputs",
                "dense_analytics",
                "external_event_analytics",
                "generic_analytics",
                "quality_checks",
            }
            _, field_map, _ = _module_meta()
            for mod in selected_modules:
                allowed.update(field_map.get(mod, []))
            screen = ModelEditor(
                PassConfig, self.data, "PassConfig (Advanced)", _on_save
            )
            screen._allowed_fields = allowed
            self.app.push_screen(screen)
            return
        if button_id == "edit_inputs":
            selected_modules = [
                key for key, cb in self.widgets.get("modules", []) if cb.value
            ] or list(self.data.get("modules", []) or [])

            def _on_save(module_inputs: dict, config_updates: dict[str, dict[str, str]]):
                self.data["module_inputs"] = module_inputs
                for field_name, patch in config_updates.items():
                    existing = _get_module_config_data(self.data, field_name)
                    existing.update(patch)
                    _set_module_config_data(self.data, field_name, existing)

            screen = ModuleInputsEditor(
                pass_data=self.data,
                selected_modules=selected_modules,
                input_sources=self.input_sources,
                on_save=_on_save,
            )
            self.app.push_screen(screen)
            return
        if button_id.startswith("edit_dense"):
            def _on_save(updated):
                self.data["dense_analytics"] = updated
            current = self.data.get("dense_analytics", {})
            dense_model = PassConfig.model_fields["dense_analytics"].annotation  # type: ignore[assignment]
            screen = ModelEditor(
                dense_model,
                current
                if isinstance(current, dict)
                else getattr(current, "model_dump", lambda: {})(),
                title="PassConfig.dense_analytics",
                on_save=_on_save,
            )
            self.app.push_screen(screen)
            return
        if button_id.startswith("edit_event"):
            def _on_save(updated):
                self.data["external_event_analytics"] = updated
            current = self.data.get("external_event_analytics", {})
            event_model = PassConfig.model_fields["external_event_analytics"].annotation  # type: ignore[assignment]
            screen = ModelEditor(
                event_model,
                current
                if isinstance(current, dict)
                else getattr(current, "model_dump", lambda: {})(),
                title="PassConfig.external_event_analytics",
                on_save=_on_save,
            )
            self.app.push_screen(screen)
            return
        if button_id.startswith("edit_module_"):
            module_key = self._module_edit_map.get(button_id)
            if not module_key:
                return
            _, field_map, _ = _module_meta()
            fields = field_map.get(module_key, [])
            if not fields:
                return
            if len(fields) == 1:
                self._open_module_editor(fields[0])
            else:
                self.app.push_screen(
                    ModuleConfigPicker(
                        fields=fields,
                        on_pick=self._open_module_editor,
                        title=f"{module_key} configs",
                    )
                )
            return
        if button_id != "save":
            return

        if self.pass_readonly:

            def _on_confirm():
                self.pass_readonly = False
                if self.on_readonly_change:
                    self.on_readonly_change(False)
                self._save_pass()

            self.app.push_screen(
                ConfirmScreen(
                    "Passes are in read-only mode. Editing will change the process. Continue?",
                    on_confirm=_on_confirm,
                )
            )
            return

        self._save_pass()

    def _save_pass(self) -> None:
        try:
            time_bucket = float(self.widgets["time_bucket_seconds"].value)
        except Exception:
            self.status.update("time_bucket_seconds must be a number")
            return

        selected_modules = [key for key, cb in self.widgets["modules"] if cb.value]
        selected_modules = [
            m for m in selected_modules if m not in {"dense", "external_events"}
        ]
        pass_type = self.widgets["pass_type"].value
        timeline_mode = self.widgets["timeline_mode"].value or "dense"
        # enforce pass type constraints
        if pass_type == "pre":
            module_info, _, _ = _module_meta()
            selected_modules = [
                m for m in selected_modules if module_info[m]["tier"] == "pre"
            ]
        elif pass_type == "core":
            module_info, _, _ = _module_meta()
            selected_modules = [
                m
                for m in selected_modules
                if module_info[m]["tier"] == "core"
                or module_info[m]["tier"] == "advanced"
            ]
        else:
            # postprocessing = free
            pass

        if timeline_mode == "dense":
            selected_modules.append("dense")
            dense_cfg = self.data.get("dense_analytics", {})
            if not isinstance(dense_cfg, dict):
                dense_cfg = {}
            dense_cfg["ENABLED"] = True
            self.data["dense_analytics"] = dense_cfg
            selected_modules = [m for m in selected_modules if m != "external_events"]
        elif timeline_mode == "event":
            selected_modules.append("external_events")
            dense_cfg = self.data.get("dense_analytics", {})
            if not isinstance(dense_cfg, dict):
                dense_cfg = {}
            dense_cfg["ENABLED"] = False
            self.data["dense_analytics"] = dense_cfg
            event_cfg = self.data.get("external_event_analytics", {})
            if not isinstance(event_cfg, dict):
                event_cfg = {}
            try:
                event_cfg["radius_seconds"] = float(
                    self.widgets.get("ext_radius_seconds").value
                )
            except Exception:
                self.status.update(
                    "external_event_analytics.radius_seconds must be a number"
                )
                return
            event_cfg["directions"] = (
                self.widgets.get("ext_directions").value
                if self.widgets.get("ext_directions")
                else event_cfg.get("directions", "both")
            )
            try:
                event_cfg["extra_observations"] = int(
                    self.widgets.get("ext_extra_observations").value
                )
            except Exception:
                self.status.update(
                    "external_event_analytics.extra_observations must be an integer"
                )
                return
            try:
                event_cfg["scale_factor"] = float(
                    self.widgets.get("ext_scale_factor").value
                )
            except Exception:
                self.status.update(
                    "external_event_analytics.scale_factor must be a number"
                )
                return
            event_cfg["ENABLED"] = True
            self.data["external_event_analytics"] = event_cfg
            selected_modules = [m for m in selected_modules if m != "dense"]
        else:
            selected_modules = [
                m for m in selected_modules if m not in {"dense", "external_events"}
            ]
            dense_cfg = self.data.get("dense_analytics", {})
            if not isinstance(dense_cfg, dict):
                dense_cfg = {}
            dense_cfg["ENABLED"] = False
            self.data["dense_analytics"] = dense_cfg

        selected_modules = list(dict.fromkeys(selected_modules))
        self.data.update(
            {
                "_pass_type": pass_type,
                "name": self.widgets["name"].value or "pass",
                "time_bucket_seconds": time_bucket,
                "timeline_mode": timeline_mode,
                "modules": selected_modules,
            }
        )
        try:
            model = PassConfig.model_validate(self.data)
        except ValidationError as exc:
            self.status.update(str(exc))
            return
        self.on_save(model.model_dump())
        if hasattr(self.app, "_refresh_summary"):
            self.app._refresh_summary()
        self.app.pop_screen()

    def on_mount(self) -> None:
        self._render_timeline_controls()
        self._render_modules()

    def on_resize(self) -> None:
        if self.widgets.get("timeline_mode") is None:
            return
        if self.widgets["timeline_mode"].value != "event":
            return
        self.call_after_refresh(self._update_external_events_preview_from_widgets)

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "pass_type":
            self.call_after_refresh(self._render_modules)
        if event.select.id == "timeline_mode":
            self.call_after_refresh(self._render_timeline_controls)
        if event.select.id == "ext_directions":
            self._update_external_events_preview_from_widgets()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id in {
            "ext_radius_seconds",
            "ext_extra_observations",
            "ext_scale_factor",
        }:
            self._update_external_events_preview_from_widgets()

    def _render_timeline_controls(self) -> None:
        controls = self.widgets.get("timeline_controls")
        if controls is None or not controls.is_attached:
            return
        controls.remove_children()
        self._timeline_render_token += 1
        suffix = str(self._timeline_render_token)
        mode = self.widgets.get("timeline_mode").value if self.widgets.get("timeline_mode") else "dense"
        if mode == "dense":
            controls.mount(
                Button(
                    "Edit dense timeline config",
                    id=f"edit_dense_{suffix}",
                )
            )
        elif mode == "event":
            cfg = self.data.get("external_event_analytics", {})
            if hasattr(cfg, "model_dump"):
                cfg = cfg.model_dump()
            if not isinstance(cfg, dict):
                cfg = {}
            radius = Input(value=str(cfg.get("radius_seconds", 0.0)), id="ext_radius_seconds")
            directions = Select(
                [("Both", "both"), ("Backward only", "backward"), ("Forward only", "forward")],
                value=cfg.get("directions", "both"),
                id="ext_directions",
            )
            extra = Input(
                value=str(cfg.get("extra_observations", 0)),
                id="ext_extra_observations",
            )
            scale = Input(value=str(cfg.get("scale_factor", 1.0)), id="ext_scale_factor")
            self.widgets["ext_radius_seconds"] = radius
            self.widgets["ext_directions"] = directions
            self.widgets["ext_extra_observations"] = extra
            self.widgets["ext_scale_factor"] = scale
            controls.mount(Label("Radius (seconds)"))
            controls.mount(radius)
            controls.mount(Label("Directions"))
            controls.mount(directions)
            controls.mount(Label("Extra observations per enabled direction"))
            controls.mount(extra)
            controls.mount(Label("Scale factor (1=arithmetic, >1=geometric base)"))
            controls.mount(scale)
            preview = Static(
                _render_external_events_preview(
                    self.data, axis_width=self._preview_axis_width()
                ),
                classes="help",
                id="ext_preview",
            )
            self.widgets["ext_preview"] = preview
            controls.mount(preview)

    def _preview_axis_width(self) -> int:
        preview = self.widgets.get("ext_preview")
        if preview is not None and getattr(preview, "is_attached", False):
            width = int(getattr(preview.size, "width", 0) or 0)
            if width > 0:
                return max(21, width - 2)
        controls = self.widgets.get("timeline_controls")
        if controls is not None and getattr(controls, "is_attached", False):
            width = int(getattr(controls.size, "width", 0) or 0)
            if width > 0:
                return max(21, width - 4)
        return 51

    def _update_external_events_preview_from_widgets(self) -> None:
        if self.widgets.get("timeline_mode") is None:
            return
        if self.widgets["timeline_mode"].value != "event":
            return
        cfg = self.data.get("external_event_analytics", {})
        if hasattr(cfg, "model_dump"):
            cfg = cfg.model_dump()
        if not isinstance(cfg, dict):
            cfg = {}
        parse_errors: list[str] = []
        try:
            cfg["radius_seconds"] = float(self.widgets["ext_radius_seconds"].value)
        except Exception:
            parse_errors.append("radius must be numeric")
        if self.widgets.get("ext_directions") is not None:
            cfg["directions"] = self.widgets["ext_directions"].value or "both"
        try:
            cfg["extra_observations"] = int(self.widgets["ext_extra_observations"].value)
        except Exception:
            parse_errors.append("extra observations must be an integer")
        try:
            cfg["scale_factor"] = float(self.widgets["ext_scale_factor"].value)
        except Exception:
            parse_errors.append("scale factor must be numeric")
        self.data["external_event_analytics"] = cfg
        preview = self.widgets.get("ext_preview")
        if preview is not None and getattr(preview, "is_attached", False):
            axis_width = self._preview_axis_width()
            rendered = _render_external_events_preview(
                {"external_event_analytics": cfg},
                axis_width=axis_width,
            )
            if parse_errors:
                rendered.append("\n")
                rendered.append(
                    f"Input warning: {', '.join(parse_errors)}",
                    style="bold yellow",
                )
            preview.update(rendered)

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        if self.widgets.get("modules_box") is None:
            return
        self.call_after_refresh(self._render_modules)

    def _render_modules(self) -> None:
        modules_box = self.widgets.get("modules_box")
        if modules_box is None or not modules_box.is_attached:
            return
        modules_box.remove_children()
        raw_modules = self.data.get("modules", [])
        if isinstance(raw_modules, list):
            normalized = []
            for name in raw_modules:
                if name == "trades":
                    normalized.append("trade")
                else:
                    normalized.append(name)
            raw_modules = normalized
            self.data["modules"] = raw_modules
        current = {
            key for key, cb in self.widgets.get("modules", []) if cb.value
        } or set(raw_modules or [])
        self.widgets["modules"] = []
        self.widgets["module_edit_buttons"] = {}
        self._module_edit_map.clear()
        self._render_token += 1
        pass_type = (
            self.widgets.get("pass_type").value
            if self.widgets.get("pass_type")
            else "core"
        )

        current_counts, full_counts = self._module_schema_counts()
        module_info, _, _ = _module_meta()

        for key, meta in module_info.items():
            if key in {"dense", "external_events"}:
                continue
            if meta["tier"] == "advanced" and not self.advanced_modules:
                continue
            if pass_type == "pre" and meta["tier"] != "pre":
                continue
            if pass_type == "core" and meta["tier"] not in {"core", "advanced"}:
                continue
            if pass_type == "post" and meta["tier"] != "post":
                continue
            checked = key in current
            count = current_counts.get(key)
            full_count = full_counts.get(key, count)
            if count is None:
                label = f"{key}"
            else:
                enabled_count = count if checked else 0
                label = f"{key} ({enabled_count}/{full_count})"
            cb = Checkbox(label, value=checked)
            self.widgets["modules"].append((key, cb))
            modules_box.mount(cb)
            if checked:
                edit_id = f"edit_module_{self._render_token}_{key}"
                edit_btn = Button("Edit config", id=edit_id)
                self.widgets["module_edit_buttons"][key] = edit_btn
                self._module_edit_map[edit_id] = key
                modules_box.mount(edit_btn)
            modules_box.mount(Label(f"  {meta['desc']}", classes="help"))
            modules_box.mount(
                Label(f"  Outputs: {', '.join(meta['columns'])}", classes="help")
            )
            modules_box.mount(
                Label(f"  {_module_inputs_line(self.data, key)}", classes="help")
            )

    def _open_module_editor(self, field_name: str) -> None:
        current = _get_module_config_data(self.data, field_name)
        annotation = _module_model_for_field(field_name)
        if _is_basemodel(annotation):

            def _on_save(updated):
                _set_module_config_data(self.data, field_name, updated)

            screen = ModelEditor(
                annotation,
                current,
                title=f"{self.title}.{field_name}",
                on_save=_on_save,
            )
            self.app.push_screen(screen)

    def _module_schema_counts(self) -> tuple[dict[str, int], dict[str, int]]:
        try:
            if self._full_schema_counts is None:
                full_schema = get_full_output_schema()
                self._full_schema_counts = _counts_from_schema(full_schema)
        except Exception:
            self._full_schema_counts = {}

        try:
            current_data = dict(self.data)
            current_modules = [
                key for key, cb in self.widgets.get("modules", []) if cb.value
            ]
            if current_modules:
                current_data["modules"] = current_modules
            pass_cfg = PassConfig.model_validate(current_data)
            schema = get_output_schema(pass_cfg)
            current_counts = _counts_from_schema(schema)
        except Exception:
            current_counts = {}

        full_counts = self._full_schema_counts or {}
        if "generic" not in full_counts:
            full_counts = dict(full_counts)
            full_counts["generic"] = _generic_metric_count(PassConfig(name="pass"))
        if "generic" not in current_counts:
            current_counts = dict(current_counts)
            current_counts["generic"] = (
                _generic_metric_count(pass_cfg) if "pass_cfg" in locals() else 0
            )
        return current_counts, full_counts


def _module_metric_count(data: dict, module_key: str) -> Optional[int]:
    if not isinstance(data, dict):
        return None
    if module_key == "trade":
        cfg = data.get("trade_analytics", {}) or {}
        count = 0
        for name in (
            "generic_metrics",
            "discrepancy_metrics",
            "flag_metrics",
            "change_metrics",
            "impact_metrics",
        ):
            vals = cfg.get(name, []) if isinstance(cfg, dict) else []
            count += len(vals) if isinstance(vals, list) else 0
        retail = cfg.get("retail_imbalance", {}) if isinstance(cfg, dict) else {}
        if isinstance(retail, dict) and retail.get("ENABLED", False):
            count += 1
        return count
    if module_key == "l2":
        cfg = data.get("l2_analytics", {}) or {}
        if not isinstance(cfg, dict):
            return None
        return sum(
            len(cfg.get(name, []))
            for name in ("liquidity", "spreads", "imbalances", "volatility", "ohlc")
        )
    if module_key == "l3":
        cfg = data.get("l3_analytics", {}) or {}
        if not isinstance(cfg, dict):
            return None
        return len(cfg.get("generic_metrics", [])) + len(
            cfg.get("advanced_metrics", [])
        )
    if module_key == "execution":
        cfg = data.get("execution_analytics", {}) or {}
        if not isinstance(cfg, dict):
            return None
        return (
            len(cfg.get("l3_execution", []))
            + len(cfg.get("trade_breakdown", []))
            + len(cfg.get("derived_metrics", []))
        )
    if module_key == "iceberg":
        cfg = _get_module_config_data(data, "extension:iceberg_analytics")
        if not isinstance(cfg, dict):
            return None
        return len(cfg.get("metrics", []))
    if module_key == "cbbo_analytics":
        cfg = data.get("cbbo_analytics", {}) or {}
        if not isinstance(cfg, dict):
            return None
        return len(cfg.get("measures", []))
    if module_key == "generic":
        cfg = data.get("generic_analytics", {}) or {}
        if not isinstance(cfg, dict):
            return None
        return len(cfg.get("aggregations", {})) + len(cfg.get("talib_indicators", []))
    return 0


def _counts_from_schema(schema: dict) -> dict[str, int]:
    counts: dict[str, int] = {}
    _, _, schema_keys = _module_meta()
    for module, keys in schema_keys.items():
        total = 0
        for key in keys:
            cols = schema.get(key, [])
            if isinstance(cols, list):
                total += len(cols)
        counts[module] = total
    return counts


def _generic_metric_count(pass_config: PassConfig) -> int:
    cfg = pass_config.generic_analytics
    count = 0
    try:
        count += len(cfg.aggregations)
    except Exception:
        pass
    try:
        count += len(cfg.talib_indicators)
    except Exception:
        pass
    return count


def _pass_metric_count(pass_data: dict) -> int:
    if not isinstance(pass_data, dict):
        return 0
    try:
        pass_cfg = PassConfig.model_validate(pass_data)
        schema = get_output_schema(pass_cfg)
    except Exception:
        return 0
    total = 0
    for cols in schema.values():
        if isinstance(cols, list):
            total += len(cols)
    return total


def _pass_metric_list(pass_data: dict) -> list[str]:
    if not isinstance(pass_data, dict):
        return []
    try:
        pass_cfg = PassConfig.model_validate(pass_data)
        schema = get_output_schema(pass_cfg)
    except Exception:
        return []
    metrics: list[str] = []
    for cols in schema.values():
        if isinstance(cols, list):
            metrics.extend([str(c) for c in cols])
    return metrics


def _format_output_driver(output_target: Any, config_data: dict) -> str:
    target = output_target or {}
    if isinstance(target, OutputTarget):
        target = target.model_dump()
    if not isinstance(target, dict):
        target = {}
    output_type = target.get("type", "parquet")
    if isinstance(output_type, Enum):
        output_type = output_type.value
    if output_type == "sql":
        table = target.get("sql_table", "unknown_table")
        conn = target.get("sql_connection", "unknown_connection")
        return f"sql {table} @ {conn}"
    template = target.get("path_template", "")
    return f"{output_type} {template}".strip()


class ModuleInputsEditor(Screen):
    def __init__(
        self,
        *,
        pass_data: dict,
        selected_modules: list[str],
        input_sources: list[str],
        on_save,
    ):
        super().__init__()
        self.pass_data = pass_data
        self.selected_modules = selected_modules
        self.input_sources = input_sources
        self.on_save = on_save
        self.widgets: dict[tuple[str, str], Select] = {}
        self.source_widgets: dict[tuple[str, str, str], Select] = {}

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Label("Input Mappings")
        yield Label("Select source per required input (REQ:SOURCE).")
        requires_map = _module_requires_map()
        module_inputs = self.pass_data.get("module_inputs", {})
        with VerticalScroll():
            for module in self.selected_modules:
                requires = requires_map.get(module, [])
                source_specs = _module_source_config_specs(module)
                yield Label(f"Module: {module}")
                current_entry = (
                    module_inputs.get(module) if isinstance(module_inputs, dict) else None
                )
                shown = False
                for req in requires:
                    current_source = req
                    if isinstance(current_entry, str) and len(requires) == 1:
                        current_source = current_entry
                    elif isinstance(current_entry, dict):
                        current_source = current_entry.get(req, req)
                    options = [req] + [s for s in self.input_sources if s != req]
                    select = Select(
                        [(s, s) for s in options],
                        value=current_source if current_source in options else req,
                    )
                    self.widgets[(module, req)] = select
                    yield Label(f"{req.upper()} source")
                    yield select
                    shown = True
                for field_name, config_key, input_name in source_specs:
                    cfg = _get_module_config_data(self.pass_data, field_name)
                    current_source = cfg.get(config_key)
                    if not isinstance(current_source, str) or not current_source:
                        current_source = _module_source_default(field_name, config_key)
                    if not isinstance(current_source, str) or not current_source:
                        continue
                    options = list(self.input_sources)
                    if current_source not in options:
                        options.append(current_source)
                    select = Select(
                        [(s, s) for s in options],
                        value=current_source,
                    )
                    self.source_widgets[(module, field_name, config_key)] = select
                    yield Label(f"{input_name.upper()} source")
                    yield select
                    shown = True
                if not shown and isinstance(current_entry, str):
                    options = list(self.input_sources)
                    if current_entry not in options:
                        options.append(current_entry)
                    select = Select(
                        [(s, s) for s in options],
                        value=current_entry,
                    )
                    self.widgets[(module, "df")] = select
                    yield Label("DF source")
                    yield select
                    shown = True
                if not shown and isinstance(current_entry, dict):
                    for req, current_source in current_entry.items():
                        req = str(req)
                        current_source = str(current_source)
                        options = list(self.input_sources)
                        if current_source not in options:
                            options.append(current_source)
                        select = Select(
                            [(s, s) for s in options],
                            value=current_source,
                        )
                        self.widgets[(module, req)] = select
                        yield Label(f"{req.upper()} source")
                        yield select
        with Horizontal():
            yield Button("Save", id="save", variant="primary")
            yield Button("Cancel", id="cancel")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel":
            self.app.pop_screen()
            return
        if event.button.id != "save":
            return

        selection = {
            key: widget.value for key, widget in self.widgets.items() if widget.value
        }
        module_requires = {
            m: _module_requires_map().get(m, []) for m in self.selected_modules
        }
        module_inputs = _build_module_inputs_from_selection(selection, module_requires)
        source_selection = {
            key: widget.value
            for key, widget in self.source_widgets.items()
            if widget.value is not None
        }
        config_updates = _module_source_config_updates(source_selection)
        self.on_save(module_inputs, config_updates)
        self.app.pop_screen()


class ModuleConfigPicker(Screen):
    def __init__(self, fields: list[str], on_pick, title: str):
        super().__init__()
        self.fields = fields
        self.on_pick = on_pick
        self.title = title

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Label(self.title)
        with Vertical():
            for field in self.fields:
                yield Button(f"Edit {field}", id=f"pick_{field}")
        yield Button("Back", id="back")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id == "back":
            self.app.pop_screen()
            return
        if button_id.startswith("pick_"):
            field = button_id.split("_", 1)[1]
            self.app.pop_screen()
            self.on_pick(field)


class ConfirmScreen(Screen):
    def __init__(self, message: str, on_confirm):
        super().__init__()
        self.message = message
        self.on_confirm = on_confirm

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Label(self.message)
        with Horizontal():
            yield Button("Cancel", id="cancel")
            yield Button("Continue", id="confirm", variant="primary")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm":
            self.app.pop_screen()
            self.on_confirm()
        else:
            self.app.pop_screen()


def main():
    args = sys.argv[1:]
    use_web = False
    if "--web" in args:
        use_web = True
        args = [a for a in args if a != "--web"]
    if len(args) < 1:
        print(
            "Usage: python -m basalt.config_ui [--web] <config.py|config.yaml>"
        )
        raise SystemExit(2)
    target = Path(args[0]).resolve()
    yaml_path = _resolve_yaml_path(target)
    if yaml_path.exists():
        user_config = _load_yaml_user_config(yaml_path)
    else:
        if target.suffix != ".yaml":
            text = _default_yaml_content(target)
            yaml_path.write_text(text, encoding="utf-8")
            user_config = _load_yaml_user_config(yaml_path)
        else:
            user_config = {}
            _save_yaml_user_config(yaml_path, user_config)

    try:
        model = AnalyticsConfig.model_validate(user_config)
        user_config = model.model_dump()
    except ValidationError:
        pass

    app = ConfigEditor(yaml_path, user_config)
    if use_web:
        try:
            app.run(driver="web")
        except Exception as exc:
            print(f"Failed to start web UI: {exc}")
            print("Install textual-web and try again.")
            raise SystemExit(3)
    else:
        app.run()


if __name__ == "__main__":
    main()
