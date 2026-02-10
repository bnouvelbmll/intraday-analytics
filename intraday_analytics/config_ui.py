from __future__ import annotations

import importlib.util
import sys
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

from intraday_analytics.configuration import AnalyticsConfig, PassConfig, OutputTarget
from intraday_analytics.execution import _derive_tables_to_load
from intraday_analytics.schema_utils import get_output_schema, get_full_output_schema
from intraday_analytics.analytics.l3 import L3AdvancedConfig


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
        self.status = Static("")
        self._skip_fields = {"PASSES"} if model_cls is AnalyticsConfig else set()
        self._section_filter: Optional[str] = None
        self._allowed_fields: Optional[set[str]] = None
        self._field_docs: dict[str, str] = {}
        self._model_doc: Optional[str] = None

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
            current = set(value or [])
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
            current = set(value or [])
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
        elif annotation in (int, float, str):
            widget = Input(value="" if value is None else str(value))
        elif _is_list_of_basemodel(annotation) is not None:
            widget = Button("Edit list", id=f"editlist_{name}")
        elif _is_basemodel(annotation):
            widget = Button("Edit", id=f"edit_{name}")
        else:
            widget = TextArea(text=_yaml_from_value(value), language="yaml")
            widget.styles.height = 5

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

    def _should_render_field(self, name: str, annotation) -> bool:
        field_info = self.model_cls.model_fields.get(name)  # type: ignore[attr-defined]
        if field_info:
            extra = getattr(field_info, "json_schema_extra", None) or {}
            depends_on = extra.get("depends_on") if isinstance(extra, dict) else None
            if depends_on:
                for dep_field, dep_values in depends_on.items():
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

    def on_select_changed(self, event: Select.Changed) -> None:
        widget_id = event.select.id
        if not widget_id:
            return
        field_name = self._widget_names.get(widget_id)
        if not field_name:
            return
        self.data[field_name] = event.value
        self.refresh(recompose=True)

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
                raw[name] = [cb.label for cb in widget if cb.value]
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
        area = data.get("AREA", "unknown")
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
            tables = _tables_for_pass(p, data)
            tables_str = ", ".join(tables) if tables else "none"
            body = "\n".join(
                [
                    f"Modules: {modules_str}",
                    f"Metrics: {metrics}",
                    f"Input driver: {input_driver}",
                    f"Output driver: {output_driver}",
                    f"Time bucket: {int(p.get('time_bucket_seconds', 60))}s",
                    f"Grouping: {grouping_label}",
                    f"Tables to load: {tables_str}",
                ]
            )
            metrics_list = _pass_metric_list(p)
            self.summary.mount(PassSummaryBox(title, body, metrics_list))

    def _set_pass_readonly(self, value: bool) -> None:
        self.pass_readonly = value

    def action_toggle_pass_lock(self) -> None:
        self.pass_readonly = not self.pass_readonly


MODULE_INFO = {
    "iceberg": {
        "desc": "Preprocessing: detect iceberg executions; run before trade analytics.",
        "columns": ["IcebergExecution"],
        "tier": "pre",
    },
    "trade": {
        "desc": "Core: trade aggregates (OHLC, volume, VWAP, etc.).",
        "columns": ["Volume", "VWAP", "OHLC", "Notional"],
        "tier": "core",
    },
    "l2": {
        "desc": "Core: L2 snapshot metrics (spreads, depth, imbalances).",
        "columns": ["Bid/Ask", "Spread", "Depth", "Imbalance"],
        "tier": "core",
    },
    "l3": {
        "desc": "Core: L3 event metrics (counts, volumes, flows).",
        "columns": ["OrderCount", "Volume", "Flow"],
        "tier": "core",
    },
    "execution": {
        "desc": "Core: execution analytics (slippage, costs).",
        "columns": ["ExecutionCost", "Slippage"],
        "tier": "core",
    },
    "cbbo": {
        "desc": "Core: CBBO-derived metrics.",
        "columns": ["CBBO", "Mid", "Spread"],
        "tier": "core",
    },
    "generic": {
        "desc": "Postprocessing: generic expressions and derived metrics.",
        "columns": ["Custom"],
        "tier": "post",
    },
    "reaggregate": {
        "desc": "Postprocessing: reaggregate previous pass by a group map.",
        "columns": ["IndexAggregation"],
        "tier": "post",
    },
    "characteristics": {
        "desc": "Advanced/internal: characteristics over L3/trades.",
        "columns": ["L3Characteristics", "TradeCharacteristics"],
        "tier": "advanced",
    },
}

PASS_MODULE_FIELD_MAP = {
    "iceberg": ["iceberg_analytics"],
    "trade": ["trade_analytics"],
    "l2": ["l2_analytics"],
    "l3": ["l3_analytics"],
    "execution": ["execution_analytics"],
    "cbbo": ["cbbo_analytics"],
    "generic": ["generic_analytics"],
    "reaggregate": ["reaggregate_analytics"],
    "characteristics": [
        "l3_characteristics_analytics",
        "trade_characteristics_analytics",
    ],
}

MODULE_SCHEMA_KEYS = {
    "l2": ["l2_last", "l2_tw"],
    "trade": ["trade"],
    "l3": ["l3"],
    "execution": ["execution"],
    "iceberg": ["iceberg"],
    "cbbo": ["cbbo"],
}


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
                tables = _tables_for_pass(p, self.config_data)
                tables_str = ", ".join(tables) if tables else "none"
                body = "\n".join(
                    [
                        f"Modules: {modules_str}",
                        f"Metrics: {metrics}",
                        f"Input driver: {input_driver}",
                        f"Output driver: {output_driver}",
                        f"Time bucket: {int(p.get('time_bucket_seconds', 60))}s",
                        f"Grouping: {grouping_label}",
                        f"Tables to load: {tables_str}",
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
            self._open_pass_editor()
        elif button_id.startswith("edit_"):
            idx = int(button_id.split("_", 1)[1])
            self._edit_pass(idx)

    def _open_pass_editor(self) -> None:
        def _on_save(updated):
            self.config_data.setdefault("PASSES", []).append(updated)
            if hasattr(self.app, "_refresh_summary"):
                self.app._refresh_summary()

        screen = PassEditor(
            {},
            "PassConfig",
            _on_save,
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
        advanced_modules: bool = False,
        pass_readonly: bool = True,
        on_readonly_change=None,
    ):
        super().__init__()
        self.data = data
        self.title = title
        self.on_save = on_save
        self.advanced_modules = advanced_modules
        self.pass_readonly = pass_readonly
        self.on_readonly_change = on_readonly_change
        self.status = Static("")
        self.widgets: dict[str, Any] = {}
        self._full_schema_counts: Optional[dict[str, int]] = None
        self._module_edit_map: dict[str, str] = {}
        self._render_token = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Label(self.title)
        with VerticalScroll():
            yield Label("Pass type")
            default_pass_type = self.data.get("_pass_type")
            if default_pass_type is None:
                modules = self.data.get("modules", [])
                tiers = {
                    MODULE_INFO.get(name if name != "trades" else "trade", {}).get(
                        "tier"
                    )
                    for name in modules
                    if MODULE_INFO.get(name if name != "trades" else "trade")
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

            yield Label("Modules (purpose and key outputs)")
            self.widgets["modules"] = []
            self.widgets["module_edit_buttons"] = {}
            modules_box = Vertical(id="modules_box")
            self.widgets["modules_box"] = modules_box
            yield modules_box

            yield Label("Advanced options")
            yield Button("Edit full PassConfig", id="edit_full")
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
                "modules",
                "dense_analytics",
                "generic_analytics",
            }
            for mod in selected_modules:
                allowed.update(PASS_MODULE_FIELD_MAP.get(mod, []))
            screen = ModelEditor(
                PassConfig, self.data, "PassConfig (Advanced)", _on_save
            )
            screen._allowed_fields = allowed
            self.app.push_screen(screen)
            return
        if button_id.startswith("edit_module_"):
            module_key = self._module_edit_map.get(button_id)
            if not module_key:
                return
            fields = PASS_MODULE_FIELD_MAP.get(module_key, [])
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
        pass_type = self.widgets["pass_type"].value
        # enforce pass type constraints
        if pass_type == "pre":
            selected_modules = [
                m for m in selected_modules if MODULE_INFO[m]["tier"] == "pre"
            ]
        elif pass_type == "core":
            selected_modules = [
                m
                for m in selected_modules
                if MODULE_INFO[m]["tier"] == "core"
                or MODULE_INFO[m]["tier"] == "advanced"
            ]
        else:
            # postprocessing = free
            pass

        self.data.update(
            {
                "_pass_type": pass_type,
                "name": self.widgets["name"].value or "pass",
                "time_bucket_seconds": time_bucket,
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
        self._render_modules()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "pass_type":
            self.call_after_refresh(self._render_modules)

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

        for key, meta in MODULE_INFO.items():
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

    def _open_module_editor(self, field_name: str) -> None:
        current = self.data.get(field_name, {})
        if not isinstance(current, dict):
            current = {}
        annotation = PassConfig.model_fields.get(field_name).annotation  # type: ignore[attr-defined]
        if _is_basemodel(annotation):

            def _on_save(updated):
                self.data[field_name] = updated

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
        cfg = data.get("iceberg_analytics", {}) or {}
        if not isinstance(cfg, dict):
            return None
        return len(cfg.get("metrics", []))
    if module_key == "cbbo":
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
    for module, keys in MODULE_SCHEMA_KEYS.items():
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


def _tables_for_pass(pass_data: dict, config_data: dict) -> list[str]:
    try:
        pass_cfg = PassConfig.model_validate(pass_data)
    except Exception:
        return []
    user_tables = config_data.get("TABLES_TO_LOAD")
    tables = _derive_tables_to_load(pass_cfg, user_tables)
    if "generic" in pass_cfg.modules and pass_cfg.generic_analytics.source_pass:
        if "previous_pass" not in tables:
            tables.append("previous_pass")
    return tables


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
            "Usage: python -m intraday_analytics.config_ui [--web] <config.py|config.yaml>"
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
