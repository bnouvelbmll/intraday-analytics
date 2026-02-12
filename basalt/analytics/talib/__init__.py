from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field

try:
    import talib  # type: ignore[import-not-found]
    from talib import abstract as talib_abstract  # type: ignore[import-not-found]

    TALIB_AVAILABLE = True
except Exception:
    talib = None
    talib_abstract = None
    TALIB_AVAILABLE = False


class TalibIndicatorConfig(BaseModel):
    """Configuration for a TA-Lib indicator call."""

    name: str = Field(
        ...,
        description="TA-Lib indicator name (e.g., SMA, RSI, MACD).",
    )
    input_col: str = Field(
        ...,
        description="Input column used as the primary TA-Lib input (typically close).",
    )
    timeperiod: Optional[int] = Field(
        14,
        description="Backward-compatible default timeperiod when parameters.timeperiod is not provided.",
    )
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="JSON kwargs passed to the TA-Lib function (supports multi-parameter indicators).",
        json_schema_extra={
            "long_doc": (
                "Arbitrary function parameters passed to TA-Lib as keyword args.\n"
                "Example for MACD: {\"fastperiod\": 12, \"slowperiod\": 26, \"signalperiod\": 9}.\n"
                "If both `timeperiod` and `parameters.timeperiod` are set, `parameters.timeperiod` wins.\n"
                "This field should be provided as a JSON/YAML object."
            )
        },
    )
    output_col: Optional[str] = Field(
        None,
        description="Optional output column name override.",
    )
    output_index: int = Field(
        0,
        description="For multi-output TA-Lib functions, selects which output to keep (0-based).",
    )


def list_talib_functions() -> list[str]:
    if not TALIB_AVAILABLE or talib is None:
        return []
    try:
        return sorted(talib.get_functions())
    except Exception:
        return []


def get_talib_function_metadata(name: str) -> dict[str, Any]:
    if not name:
        return {}
    if not TALIB_AVAILABLE or talib_abstract is None:
        return {}
    try:
        fn = talib_abstract.Function(name)
        info = dict(getattr(fn, "info", {}) or {})
    except Exception:
        return {}

    desc = str(info.get("display_name") or name)
    group = str(info.get("group") or "")
    hint = str(info.get("hint") or "")
    function_flags = info.get("function_flags") or []
    input_names = info.get("input_names") or {}
    parameters = info.get("parameters") or {}
    output_names = info.get("output_names") or []

    params = []
    if isinstance(parameters, dict):
        for key, value in parameters.items():
            params.append({"name": str(key), "default": value})
    return {
        "name": name,
        "description": desc,
        "group": group,
        "hint": hint,
        "function_flags": function_flags,
        "input_names": input_names,
        "parameters": params,
        "output_names": output_names if isinstance(output_names, list) else [output_names],
    }


def get_basalt_plugin() -> dict[str, Any]:
    return {
        "name": "talib",
        "provides": ["TA-Lib metadata and indicator config support"],
        "analytics_packages": ["basalt.analytics.talib"],
    }
