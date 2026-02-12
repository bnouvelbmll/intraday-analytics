from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional, Tuple

import yaml
from pydantic import BaseModel

from basalt.configuration import AnalyticsConfig, PassConfig
from basalt.schema_utils import get_output_schema
from basalt.schema_utils import _build_full_config, _apply_docs
from basalt.analytics.l2 import L2AnalyticsConfig
from basalt.analytics.l3 import L3AnalyticsConfig
from basalt.analytics.trade import TradeAnalyticsConfig
from basalt.analytics.execution import ExecutionAnalyticsConfig
from basalt.analytics.cbbo import CBBOAnalyticsConfig
from basalt.preprocessors.iceberg import IcebergAnalyticsConfig
from basalt.analytics.generic import GenericAnalyticsConfig
from basalt.time.dense import DenseAnalyticsConfig


MODULE_CONFIG_MAP: dict[str, tuple[str, type[BaseModel]]] = {
    "trade": ("trade_analytics", TradeAnalyticsConfig),
    "l2": ("l2_analytics", L2AnalyticsConfig),
    "l3": ("l3_analytics", L3AnalyticsConfig),
    "execution": ("execution_analytics", ExecutionAnalyticsConfig),
    "cbbo_analytics": ("cbbo_analytics", CBBOAnalyticsConfig),
    "iceberg": ("iceberg_analytics", IcebergAnalyticsConfig),
    "generic": ("generic_analytics", GenericAnalyticsConfig),
    "dense": ("dense_analytics", DenseAnalyticsConfig),
}


def _load_yaml_user_config(pipeline: str) -> dict:
    if not pipeline:
        return {}
    yaml_path = Path(pipeline).with_suffix(".yaml")
    try:
        data = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        return {}
    if isinstance(data, dict) and "USER_CONFIG" in data and isinstance(
        data["USER_CONFIG"], dict
    ):
        return data["USER_CONFIG"]
    return data if isinstance(data, dict) else {}


def _field_docs(model: type[BaseModel]) -> list[dict[str, str]]:
    docs = []
    for name, field in model.model_fields.items():  # type: ignore[attr-defined]
        short = field.description or ""
        extra = getattr(field, "json_schema_extra", None) or {}
        long_doc = extra.get("long_doc") if isinstance(extra, dict) else None
        entry = {"field": name, "description": short}
        if long_doc:
            entry["long_doc"] = long_doc
        docs.append(entry)
    return docs


def _find_column_in_pass(pass_cfg: PassConfig, column: str) -> Optional[Tuple[str, list[str]]]:
    schema = get_output_schema(pass_cfg)
    for module_key, cols in schema.items():
        if column in cols:
            return module_key, cols
    return None


def explain_column(
    pipeline: str,
    column: str,
    *,
    config_dict: Optional[dict] = None,
    module_hint: Optional[str] = None,
    pass_name: Optional[str] = None,
) -> dict[str, Any]:
    user_cfg = config_dict or _load_yaml_user_config(pipeline)
    if user_cfg:
        config = AnalyticsConfig(**user_cfg)
    else:
        config = _build_full_config(levels=10, impact_horizons=["1s", "10s"])

    for pass_cfg in config.PASSES:
        found = _find_column_in_pass(pass_cfg, column)
        if not found:
            continue
        module_key, _ = found
        normalized = module_key
        if module_key in {"l2_last", "l2_tw"}:
            normalized = "l2"
        config_attr, model = MODULE_CONFIG_MAP.get(normalized, (None, None))
        module_config = None
        if config_attr and hasattr(pass_cfg, config_attr):
            module_config = getattr(pass_cfg, config_attr)
        metric_doc = _apply_docs(module_key, column)
        payload: dict[str, Any] = {
            "column": column,
            "pass_name": pass_cfg.name,
            "module": normalized,
            "config_path": f"PassConfig.{config_attr}" if config_attr else None,
            "metric_doc": metric_doc,
            "module_doc": (model.__doc__ or "").strip() if model else "",
            "module_config": module_config.model_dump() if module_config else {},
            "module_options": _field_docs(model) if model else [],
        }
        return payload

    if module_hint:
        normalized = module_hint
        if module_hint in {"l2_last", "l2_tw"}:
            normalized = "l2"
        config_attr, model = MODULE_CONFIG_MAP.get(normalized, (None, None))
        module_config = None
        if config.PASSES:
            for pass_cfg in config.PASSES:
                if pass_name and pass_cfg.name != pass_name:
                    continue
                if config_attr and hasattr(pass_cfg, config_attr):
                    module_config = getattr(pass_cfg, config_attr)
                    break
        metric_doc = _apply_docs(module_hint, column)
        payload = {
            "column": column,
            "pass_name": pass_name or (config.PASSES[0].name if config.PASSES else None),
            "module": normalized,
            "config_path": f"PassConfig.{config_attr}" if config_attr else None,
            "metric_doc": metric_doc,
            "module_doc": (model.__doc__ or "").strip() if model else "",
            "module_config": module_config.model_dump() if module_config else {},
            "module_options": _field_docs(model) if model else [],
        }
        return payload

    return {
        "column": column,
        "found": False,
        "message": "Column not found in any pass schema for this pipeline.",
    }


def format_explain_markdown(payload: dict[str, Any]) -> str:
    if not payload.get("found", True):
        return payload.get("message", "No details found.")

    lines = [
        f"# {payload.get('column','')}",
        f"Module: `{payload.get('module','')}`",
    ]
    if payload.get("pass_name"):
        lines.append(f"Pass: `{payload.get('pass_name')}`")
    if payload.get("config_path"):
        lines.append(f"Config path: `{payload.get('config_path')}`")

    metric_doc = payload.get("metric_doc") or {}
    definition = metric_doc.get("definition") or ""
    description = metric_doc.get("description") or ""
    unit = metric_doc.get("unit") or ""
    if definition:
        lines.append("\n## Metric Definition")
        lines.append(definition)
    if description:
        lines.append("\n## Detailed Description")
        lines.append(description)
    if unit:
        lines.append(f"\nUnit: `{unit}`")

    module_doc = payload.get("module_doc") or ""
    if module_doc:
        lines.append("\n## Module Overview")
        lines.append(module_doc)

    options = payload.get("module_options") or []
    if options:
        lines.append("\n## Relevant Config Options")
        for opt in options:
            desc = opt.get("description", "")
            long_doc = opt.get("long_doc", "")
            lines.append(f"- `{opt.get('field','')}`: {desc}")
            if long_doc:
                lines.append(f"  {long_doc}")

    return "\n".join(lines)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Explain an analytic column.")
    parser.add_argument("--pipeline", required=True, help="Path to pipeline .py file.")
    parser.add_argument("--column", required=True, help="Column name to explain.")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON.",
    )
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Output as Markdown.",
    )
    args = parser.parse_args()
    payload = explain_column(args.pipeline, args.column)
    if args.json:
        print(json.dumps(payload, indent=2))
        return
    if args.markdown:
        print(format_explain_markdown(payload))
        return
    print(yaml.safe_dump(payload, sort_keys=False))
