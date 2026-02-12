#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shlex
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


@dataclass
class ToolInvocation:
    name: str
    arguments: dict[str, Any]


def _parse_command(command: str) -> tuple[str, list[str]]:
    tokens = shlex.split(command)
    if not tokens:
        raise ValueError("Empty command.")
    return tokens[0], tokens[1:]


def parse_tool_invocation(raw: str) -> ToolInvocation:
    """
    Parse `tool=JSON` payloads.

    Example:
      capabilities={}
      optimize_summary={"output_dir":"optimize_results"}
    """
    if "=" not in raw:
        return ToolInvocation(name=raw.strip(), arguments={})
    name, payload = raw.split("=", 1)
    name = name.strip()
    payload = payload.strip()
    if not name:
        raise ValueError(f"Invalid tool invocation: {raw}")
    if not payload:
        return ToolInvocation(name=name, arguments={})
    try:
        parsed = json.loads(payload)
    except Exception as exc:
        raise ValueError(f"Invalid JSON for '{name}': {payload}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"Tool args must be an object for '{name}'.")
    return ToolInvocation(name=name, arguments=parsed)


def default_tool_plan(query: str) -> list[ToolInvocation]:
    q = (query or "").lower()
    if "optimiz" in q or "trial" in q:
        return [ToolInvocation("optimize_summary", {"output_dir": "optimize_results"})]
    if "success" in q or "failure" in q:
        return [ToolInvocation("success_rate", {"executor": "pipeline", "limit": 50})]
    if "recent" in q or "runs" in q:
        return [ToolInvocation("recent_runs", {"executor": "pipeline", "limit": 20})]
    return [ToolInvocation("capabilities", {})]


async def run_mcp_calls(
    *,
    command: str,
    cwd: str | None,
    invocations: list[ToolInvocation],
) -> dict[str, Any]:
    cmd, args = _parse_command(command)
    server = StdioServerParameters(command=cmd, args=args, cwd=cwd)
    payload: dict[str, Any] = {"tools": [], "results": []}
    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            tools = await session.list_tools()
            tool_names = [t.name for t in tools.tools]
            payload["tools"] = tool_names
            for invocation in invocations:
                result = await session.call_tool(
                    invocation.name,
                    invocation.arguments or {},
                )
                payload["results"].append(
                    {
                        "tool": invocation.name,
                        "arguments": invocation.arguments,
                        "content": [c.model_dump() for c in result.content],
                        "is_error": bool(getattr(result, "isError", False)),
                    }
                )
    return payload


def _extract_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("output_text", "text", "content"):
            if key in value:
                return _extract_text(value[key])
        return json.dumps(value, indent=2, sort_keys=True)
    if isinstance(value, list):
        return "\n".join(_extract_text(v) for v in value)
    return str(value)


def summarize_with_openai(
    *,
    model: str,
    prompt: str,
    api_key: str | None,
) -> str:
    from openai import OpenAI

    client = OpenAI(api_key=api_key or os.environ.get("OPENAI_API_KEY"))
    resp = client.responses.create(model=model, input=prompt)
    text = getattr(resp, "output_text", None)
    if text:
        return str(text)
    return _extract_text(resp.model_dump())


def summarize_with_gemini(
    *,
    model: str,
    prompt: str,
    api_key: str | None,
) -> str:
    from google import genai

    client = genai.Client(api_key=api_key or os.environ.get("GEMINI_API_KEY"))
    resp = client.models.generate_content(model=model, contents=prompt)
    text = getattr(resp, "text", None)
    return str(text or _extract_text(resp))


def summarize_with_local_dspy(
    *,
    model: str,
    prompt: str,
    api_base: str,
    api_key: str,
    cache_dir: str,
) -> str:
    # dspy may fail at import if cache path is read-only.
    os.environ.setdefault("DSPY_CACHEDIR", cache_dir)
    import dspy

    try:
        lm = dspy.LM(f"openai/{model}", api_base=api_base, api_key=api_key)
    except TypeError:
        # Compatibility across dspy/litellm versions.
        lm = dspy.LM(f"openai/{model}", base_url=api_base, api_key=api_key)
    dspy.configure(lm=lm)
    predictor = dspy.Predict("context,question -> answer")
    out = predictor(context=prompt, question="Summarize and suggest the next action.")
    return str(getattr(out, "answer", out))


def render_prompt(*, query: str, mcp_payload: dict[str, Any]) -> str:
    return textwrap.dedent(
        f"""
        You are a demo assistant that can use Basalt MCP tools.
        User query:
        {query}

        Available MCP tools:
        {json.dumps(mcp_payload.get("tools", []), indent=2)}

        Tool call results:
        {json.dumps(mcp_payload.get("results", []), indent=2, default=str)}

        Provide:
        1) A concise answer to the user query.
        2) A short list of recommended next MCP tool calls.
        """
    ).strip()


def maybe_start_sglang(args: argparse.Namespace) -> subprocess.Popen[str] | None:
    if not args.start_sglang:
        return None
    cmd = args.sglang_command or (
        "export HF_HOME='/tmp/hf'; "
        "export HF_HUB_CACHE='/tmp/hfc'; "
        "PYTHONWARNINGS='once' "
        "python -m sglang.launch_server "
        "--port 7501 "
        "--model-path mistralai/Mistral-7B-Instruct-v0.1"
    )
    return subprocess.Popen(cmd, shell=True, text=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Demo script: connect a local/remote LLM to Basalt MCP server over stdio."
    )
    parser.add_argument(
        "--provider",
        choices=["openai", "gemini", "local-dspy", "none"],
        default="none",
        help="LLM provider for summarization.",
    )
    parser.add_argument("--query", default="What can this Basalt environment do?")
    parser.add_argument(
        "--mcp-command",
        default=f"{sys.executable} -m basalt.basalt mcp serve",
        help="Command used to run/connect to MCP server over stdio.",
    )
    parser.add_argument(
        "--tool-call",
        action="append",
        default=[],
        help="Tool call as `name=JSON` (repeatable). Example: capabilities={} ",
    )
    parser.add_argument(
        "--show-tools",
        action="store_true",
        help="Only list tools and exit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions without calling LLM provider.",
    )

    parser.add_argument("--openai-model", default="gpt-4.1-mini")
    parser.add_argument("--openai-api-key", default=None)
    parser.add_argument("--gemini-model", default="gemini-2.0-flash")
    parser.add_argument("--gemini-api-key", default=None)

    parser.add_argument("--local-model", default="mistralai/Mistral-7B-Instruct-v0.1")
    parser.add_argument("--local-api-base", default="http://127.0.0.1:7501/v1")
    parser.add_argument("--local-api-key", default="EMPTY")
    parser.add_argument("--dspy-cache-dir", default="/tmp/dspy_cache")
    parser.add_argument("--start-sglang", action="store_true")
    parser.add_argument("--sglang-command", default=None)
    parser.add_argument("--cwd", default=str(Path.cwd()))
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    invocations = (
        [parse_tool_invocation(raw) for raw in args.tool_call]
        if args.tool_call
        else default_tool_plan(args.query)
    )

    sglang_proc = maybe_start_sglang(args)
    try:
        mcp_payload = asyncio.run(
            run_mcp_calls(
                command=args.mcp_command,
                cwd=args.cwd,
                invocations=invocations if not args.show_tools else [ToolInvocation("capabilities", {})],
            )
        )
        if args.show_tools:
            print(json.dumps({"tools": mcp_payload.get("tools", [])}, indent=2))
            return 0

        prompt = render_prompt(query=args.query, mcp_payload=mcp_payload)
        if args.dry_run or args.provider == "none":
            print("MCP payload:")
            print(json.dumps(mcp_payload, indent=2, default=str))
            print("\nPrompt:")
            print(prompt)
            return 0

        if args.provider == "openai":
            text = summarize_with_openai(
                model=args.openai_model,
                prompt=prompt,
                api_key=args.openai_api_key,
            )
        elif args.provider == "gemini":
            text = summarize_with_gemini(
                model=args.gemini_model,
                prompt=prompt,
                api_key=args.gemini_api_key,
            )
        elif args.provider == "local-dspy":
            text = summarize_with_local_dspy(
                model=args.local_model,
                prompt=prompt,
                api_base=args.local_api_base,
                api_key=args.local_api_key,
                cache_dir=args.dspy_cache_dir,
            )
        else:
            raise ValueError(f"Unknown provider: {args.provider}")

        print(text)
        return 0
    finally:
        if sglang_proc is not None:
            try:
                sglang_proc.terminate()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
