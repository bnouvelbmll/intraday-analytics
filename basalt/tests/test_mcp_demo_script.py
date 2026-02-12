from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


def _load_demo_module():
    root = Path(__file__).resolve().parents[2]
    script = root / "scripts" / "mcp_llm_demo.py"
    spec = importlib.util.spec_from_file_location("mcp_llm_demo", script)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_parse_tool_invocation_and_default_plan():
    mod = _load_demo_module()
    parsed = mod.parse_tool_invocation('optimize_summary={"output_dir":"x"}')
    assert parsed.name == "optimize_summary"
    assert parsed.arguments["output_dir"] == "x"

    fallback = mod.parse_tool_invocation("capabilities")
    assert fallback.name == "capabilities"
    assert fallback.arguments == {}

    plan = mod.default_tool_plan("show me recent runs")
    assert plan and plan[0].name == "recent_runs"
