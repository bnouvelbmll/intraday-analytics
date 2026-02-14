from __future__ import annotations

import importlib
import inspect
import pkgutil

from pydantic import BaseModel

import basalt
import basalt.config_ui as config_ui


def _iter_config_models():
    for modinfo in pkgutil.walk_packages(basalt.__path__, basalt.__name__ + "."):
        mod_name = modinfo.name
        if ".tests" in mod_name:
            continue
        try:
            module = importlib.import_module(mod_name)
        except Exception:
            continue
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if cls.__module__ != module.__name__:
                continue
            if not issubclass(cls, BaseModel) or cls is BaseModel:
                continue
            if not cls.__name__.endswith("Config"):
                continue
            yield cls


def test_all_config_models_have_docstrings_and_field_descriptions():
    missing = []
    for cls in _iter_config_models():
        doc = (inspect.getdoc(cls) or "").strip()
        if not doc:
            missing.append(f"{cls.__module__}.{cls.__name__}: missing class docstring")
        for field_name, field in cls.model_fields.items():
            description = (field.description or "").strip()
            if not description:
                missing.append(
                    f"{cls.__module__}.{cls.__name__}.{field_name}: missing field description"
                )

    assert not missing, "Undocumented config fields:\n" + "\n".join(sorted(missing))


def _non_empty_line_count(text: str) -> int:
    return len([line for line in text.splitlines() if line.strip()])


def test_ui_module_docs_are_verbose():
    module_info, field_map, _ = config_ui._module_meta()
    missing = []
    for module_key, meta in sorted(module_info.items()):
        fields = field_map.get(module_key, [])
        model_cls = config_ui._module_model_for_field(fields[0]) if fields else None
        doc = config_ui._module_long_doc(module_key, meta, model_cls)
        if _non_empty_line_count(doc) < 10:
            missing.append(
                f"{module_key}: module docs must have at least 10 non-empty lines"
            )
    assert not missing, "UI module docs are too short:\n" + "\n".join(sorted(missing))


def test_ui_option_docs_are_verbose():
    module_info, field_map, _ = config_ui._module_meta()
    missing = []
    for module_key in sorted(module_info):
        for field_name in field_map.get(module_key, []):
            model_cls = config_ui._module_model_for_field(field_name)
            if model_cls is None:
                continue
            for option_name, field in model_cls.model_fields.items():
                doc = config_ui._field_long_doc(field, field_name=option_name) or ""
                if _non_empty_line_count(doc) < 3:
                    missing.append(
                        f"{model_cls.__module__}.{model_cls.__name__}.{option_name}: "
                        "option docs must have at least 3 non-empty lines"
                    )
    assert not missing, "UI option docs are too short:\n" + "\n".join(sorted(missing))


def test_all_module_docs_are_authored_and_verbose():
    module_info, field_map, _ = config_ui._module_meta()
    missing = []
    for module_key, meta in sorted(module_info.items()):
        module_desc = (meta.get("desc") or "").strip()
        if _non_empty_line_count(module_desc) < 10:
            missing.append(f"{module_key}: ui.desc has fewer than 10 non-empty lines")
        fields = field_map.get(module_key, [])
        if not fields:
            missing.append(f"{module_key}: has no config field mapping in UI metadata")
            continue
        model_cls = config_ui._module_model_for_field(fields[0])
        if model_cls is None:
            missing.append(f"{module_key}: no config model resolved from field mapping")
            continue
        for option_name, field in model_cls.model_fields.items():
            extra = getattr(field, "json_schema_extra", None) or {}
            long_doc = extra.get("long_doc") if isinstance(extra, dict) else None
            if _non_empty_line_count(long_doc or "") < 3:
                missing.append(f"{module_key}.{model_cls.__name__}.{option_name}")
    assert not missing, (
        "all UI modules must provide authored docs: module ui.desc >=10 lines and "
        f"field long_doc >=3 lines.\n{chr(10).join(sorted(missing))}"
    )
