from __future__ import annotations

import importlib
import inspect
import pkgutil

from pydantic import BaseModel

import basalt


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
