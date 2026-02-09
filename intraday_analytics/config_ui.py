from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import yaml
from textual.app import App, ComposeResult
from textual.containers import Vertical
from textual.widgets import Header, Footer, Static, TextArea


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


class ConfigEditor(App):
    BINDINGS = [("ctrl+s", "save", "Save"), ("ctrl+q", "quit", "Quit")]

    def __init__(self, yaml_path: Path, initial_text: str):
        super().__init__()
        self.yaml_path = yaml_path
        self.initial_text = initial_text
        self.status = Static("")
        self.editor = TextArea(text=initial_text, language="yaml")

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical():
            yield self.editor
            yield self.status
        yield Footer()

    def action_save(self) -> None:
        text = self.editor.text
        try:
            yaml.safe_load(text)
        except Exception as exc:
            self.status.update(f"YAML error: {exc}")
            return
        self.yaml_path.write_text(text, encoding="utf-8")
        self.status.update(f"Saved: {self.yaml_path}")


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
        text = yaml_path.read_text(encoding="utf-8")
    else:
        if target.suffix != ".yaml":
            text = _default_yaml_content(target)
        else:
            text = yaml.safe_dump({"USER_CONFIG": {}}, sort_keys=False)
        yaml_path.write_text(text, encoding="utf-8")
    app = ConfigEditor(yaml_path, text)
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
