from __future__ import annotations

import importlib
import pkgutil

import basalt.analytics as analytics_pkg
from basalt.analytics_base import ANALYTIC_DOCS
from basalt.schema_utils import _apply_docs


def _load_registered_analytics_docs() -> None:
    for mod in pkgutil.iter_modules(
        analytics_pkg.__path__, analytics_pkg.__name__ + "."
    ):
        importlib.import_module(mod.name)
    importlib.import_module("basalt.time.dense")
    importlib.import_module("basalt.time.external_events")


def test_analytic_docs_have_definition_unit_and_description():
    _load_registered_analytics_docs()
    missing = []
    for doc in ANALYTIC_DOCS:
        module = str(doc.get("module") or "")
        pattern = str(doc.get("pattern") or "")
        template = str(doc.get("template") or "").strip()
        unit = str(doc.get("unit") or "").strip()
        description = str(doc.get("description") or "").strip()

        if not template:
            missing.append(f"{module}:{pattern}: missing definition template")
        if not unit:
            missing.append(f"{module}:{pattern}: missing unit")
        if not description:
            missing.append(f"{module}:{pattern}: missing description")

    assert not missing, "Incomplete analytic docs metadata:\n" + "\n".join(sorted(missing))


def test_explorer_docs_present_for_alpha101_and_event_modules():
    _load_registered_analytics_docs()
    checks = [
        ("alpha101", "Alpha001"),
        ("external_events", "EventAnchorTime"),
        ("external_events", "EventContextIndex"),
        ("external_events", "EventDeltaSeconds"),
        ("observed_events", "EventType"),
        ("observed_events", "Indicator"),
        ("observed_events", "IndicatorValue"),
        ("correlation", "MetricX"),
        ("correlation", "MetricY"),
        ("correlation", "Corr"),
    ]
    for module, column in checks:
        docs = _apply_docs(module, column)
        assert docs["definition"], f"Missing definition for {module}:{column}"
        assert docs["unit"], f"Missing unit for {module}:{column}"
        assert docs["description"], f"Missing description for {module}:{column}"
