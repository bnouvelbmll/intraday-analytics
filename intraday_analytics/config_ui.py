from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any, Optional, Union, get_args, get_origin
from enum import Enum

import yaml
from pydantic import BaseModel, ValidationError
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import Screen
from textual.widgets import Button, Checkbox, Footer, Header, Input, Label, Select, Static, TextArea

from intraday_analytics.configuration import AnalyticsConfig, PassConfig


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
    return yaml.safe_dump({"USER_CONFIG": getattr(module, "USER_CONFIG")}, sort_keys=False)


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


def _is_literal(annotation) -> bool:
    return get_origin(annotation) is getattr(__import__("typing"), "Literal", None)


def _literal_options(annotation) -> list[str]:
    return [str(v) for v in get_args(annotation)]


def _is_enum(annotation) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, Enum)


def _enum_options(annotation) -> list[str]:
    return [str(v.value) for v in annotation]  # type: ignore[operator]


def _is_basemodel(annotation) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, BaseModel)


def _yaml_from_value(value: Any) -> str:
    return yaml.safe_dump(value, sort_keys=False)


def _parse_yaml(text: str) -> Any:
    return yaml.safe_load(text)


class ModelEditor(Screen):
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
        self.status = Static("")

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Label(self.title)
        with VerticalScroll():
            for name, field in self.model_cls.model_fields.items():  # type: ignore[attr-defined]
                annotation = _unwrap_optional(field.annotation)
                value = self.data.get(name, field.default)
                yield from self._render_field(name, annotation, value)
        with Horizontal():
            yield Button("Save", id="save", variant="primary")
            yield Button("Cancel", id="cancel")
        yield self.status
        yield Footer()

    def _render_field(self, name: str, annotation, value) -> ComposeResult:
        label = Label(name)
        if annotation is bool:
            widget = Checkbox(value=bool(value))
        elif _is_literal(annotation):
            options = _literal_options(annotation)
            widget = Select([(o, o) for o in options], value=str(value) if value is not None else options[0])
        elif _is_enum(annotation):
            options = _enum_options(annotation)
            current = value.value if hasattr(value, "value") else value
            widget = Select([(o, o) for o in options], value=str(current) if current is not None else options[0])
        elif annotation in (int, float, str):
            widget = Input(value="" if value is None else str(value))
        elif _is_basemodel(annotation):
            widget = Button("Edit", id=f"edit:{name}")
        else:
            widget = TextArea(text=_yaml_from_value(value), language="yaml")

        self.widgets[name] = (widget, annotation)
        with Horizontal():
            yield label
            yield widget

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id == "save":
            self._save()
        elif button_id == "cancel":
            self.app.pop_screen()
        elif button_id.startswith("edit:"):
            field_name = button_id.split(":", 1)[1]
            widget, annotation = self.widgets[field_name]
            current = self.data.get(field_name, {})
            if _is_basemodel(annotation):
                def _on_save(updated):
                    self.data[field_name] = updated
                screen = ModelEditor(
                    annotation,
                    current if isinstance(current, dict) else getattr(current, "model_dump", lambda: {})(),
                    title=f"{self.title}.{field_name}",
                    on_save=_on_save,
                )
                self.app.push_screen(screen)

    def _save(self) -> None:
        raw = {}
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
            elif isinstance(widget, Button) and _is_basemodel(annotation):
                raw[name] = self.data.get(name, {})

        try:
            model = self.model_cls.model_validate(raw)  # type: ignore[attr-defined]
        except ValidationError as exc:
            self.status.update(str(exc))
            return
        self.on_save(model.model_dump())
        self.app.pop_screen()


class ConfigEditor(App):
    BINDINGS = [("ctrl+s", "save", "Save"), ("ctrl+q", "quit", "Quit")]

    def __init__(self, yaml_path: Path, initial_config: dict):
        super().__init__()
        self.yaml_path = yaml_path
        self.config_data = initial_config
        self.status = Static("")

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("AnalyticsConfig")
        yield Button("Edit Config", id="edit_config", variant="primary")
        yield Button("Edit Passes", id="edit_passes")
        yield self.status
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id == "edit_config":
            def _on_save(updated):
                self.config_data.update(updated)
            screen = ModelEditor(AnalyticsConfig, self.config_data, "AnalyticsConfig", _on_save)
            self.push_screen(screen)
        elif button_id == "edit_passes":
            self.push_screen(PassListEditor(self.config_data))

    def action_save(self) -> None:
        try:
            model = AnalyticsConfig.model_validate(self.config_data)
        except ValidationError as exc:
            self.status.update(str(exc))
            return
        _save_yaml_user_config(self.yaml_path, model.model_dump())
        self.status.update(f"Saved: {self.yaml_path}")


class PassListEditor(Screen):
    def __init__(self, config_data: dict):
        super().__init__()
        self.config_data = config_data
        self.status = Static("")

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Label("PASSES")
        with Vertical():
            for idx, p in enumerate(self.config_data.get("PASSES", [])):
                name = p.get("name", f"pass{idx+1}")
                yield Button(f"Edit {name}", id=f"edit:{idx}")
        with Horizontal():
            yield Button("Add Pass", id="add")
            yield Button("Back", id="back")
        yield self.status
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id == "back":
            self.app.pop_screen()
        elif button_id == "add":
            def _on_save(updated):
                self.config_data.setdefault("PASSES", []).append(updated)
            screen = ModelEditor(PassConfig, {}, "PassConfig", _on_save)
            self.app.push_screen(screen)
        elif button_id.startswith("edit:"):
            idx = int(button_id.split(":", 1)[1])
            passes = self.config_data.get("PASSES", [])
            current = passes[idx] if idx < len(passes) else {}

            def _on_save(updated):
                passes[idx] = updated
                self.config_data["PASSES"] = passes

            screen = ModelEditor(PassConfig, current, "PassConfig", _on_save)
            self.app.push_screen(screen)


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
