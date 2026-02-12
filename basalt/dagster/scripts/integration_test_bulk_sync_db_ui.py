#!/usr/bin/env python3
"""
End-to-end UI/GraphQL test for DB-injected asset materializations.

Starts a temporary Dagster dev server with minimal definitions, injects
materializations via DB fast-path, then queries GraphQL to confirm the
partitions appear in the UI layer.
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import select
import tempfile
import time
from pathlib import Path
from urllib.request import Request, urlopen

from dagster import AssetKey, AssetMaterialization, DagsterInstance, MultiPartitionKey
from dagster._core.instance.utils import RUNLESS_JOB_NAME, RUNLESS_RUN_ID

REPO_ROOT = Path(__file__).resolve().parents[3]

from basalt.dagster.scripts.s3_bulk_sync_db import (
    _build_event,
    _insert_events,
    _update_asset_indexes,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _write_defs_file(defs_dir: Path) -> Path:
    defs_path = defs_dir / "definitions.py"
    defs_path.write_text(
        "\n".join(
            [
                "from dagster import Definitions, DailyPartitionsDefinition, StaticPartitionsDefinition",
                "from basalt.dagster.dagster_compat import build_input_source_assets",
                "",
                "date_parts = DailyPartitionsDefinition(start_date='2020-01-01', end_date='2020-01-03')",
                "mic_parts = StaticPartitionsDefinition(['XNAS', 'XLON'])",
                "assets, _ = build_input_source_assets(",
                "    tables=['l2'],",
                "    date_partitions_def=date_parts,",
                "    mic_partitions_def=mic_parts,",
                "    asset_key_prefix=['BMLL'],",
                "    date_dim='date',",
                "    mic_dim='mic',",
                "    group_name='BMLL',",
                ")",
                "defs = Definitions(assets=assets)",
                "",
            ]
        )
        + "\n"
    )
    return defs_path


def _graphql(url: str, query: str, variables: dict | None = None) -> dict:
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = Request(url, data=payload, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode())


def _wait_for_server(
    url: str,
    proc: subprocess.Popen,
    output_buffer: list[str],
    timeout_s: int = 60,
) -> None:
    query = "{ version }"
    start = time.time()
    while True:
        if proc.poll() is not None:
            raise RuntimeError("Dagster dev server exited early.")
        if proc.stdout is not None:
            r, _, _ = select.select([proc.stdout], [], [], 0.1)
            if r:
                line = proc.stdout.readline()
                if line:
                    output_buffer.append(line.rstrip())
        try:
            data = _graphql(url, query)
            if "errors" not in data:
                return
        except Exception:
            pass
        if time.time() - start > timeout_s:
            raise TimeoutError("Dagster dev server did not become ready in time.")
        time.sleep(1)


def _inject_events(dagster_home: str, partitions: list[str]) -> None:
    os.environ["DAGSTER_HOME"] = dagster_home
    instance = DagsterInstance.get()
    storage = instance.event_log_storage

    asset_key = AssetKey(["BMLL", "l2"])
    events = []
    ts = time.time()
    for partition in partitions:
        mat = AssetMaterialization(
            asset_key=asset_key,
            partition=partition,
            metadata={"source": "ui_test"},
        )
        events.append(_build_event(mat, ts, RUNLESS_RUN_ID, RUNLESS_JOB_NAME))
    event_ids = _insert_events(storage, events, RUNLESS_RUN_ID)
    _update_asset_indexes(storage, events, event_ids)


def main() -> int:
    dagster_home = tempfile.mkdtemp(prefix="dagster_ui_test_")
    defs_dir = Path(tempfile.mkdtemp(prefix="dagster_defs_"))
    defs_path = _write_defs_file(defs_dir)

    port = _free_port()
    url = f"http://127.0.0.1:{port}/graphql"

    env = os.environ.copy()
    env["DAGSTER_HOME"] = dagster_home
    existing_pp = env.get("PYTHONPATH", "")
    if existing_pp:
        env["PYTHONPATH"] = f"{REPO_ROOT}{os.pathsep}{existing_pp}"
    else:
        env["PYTHONPATH"] = str(REPO_ROOT)
    env["DAGSTER_DISABLE_TELEMETRY"] = "1"

    cmd = [
        sys.executable,
        "-m",
        "dagster",
        "dev",
        "-f",
        str(defs_path),
        "-p",
        str(port),
        "-h",
        "127.0.0.1",
    ]

    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    output_buffer: list[str] = []

    try:
        _wait_for_server(url, proc, output_buffer)
        partitions = [
            str(MultiPartitionKey({"date": "2020-01-01", "mic": "XNAS"})),
            str(MultiPartitionKey({"date": "2020-01-01", "mic": "XLON"})),
            str(MultiPartitionKey({"date": "2020-01-02", "mic": "XNAS"})),
            str(MultiPartitionKey({"date": "2020-01-02", "mic": "XLON"})),
        ]

        _inject_events(dagster_home, partitions)

        query = """
        query($assetKey: AssetKeyInput!, $parts: [String!]) {
          assetNodeOrError(assetKey: $assetKey) {
            __typename
            ... on AssetNode {
              assetKey { path }
              assetMaterializations(partitions: $parts, limit: 100) {
                partition
              }
            }
            ... on AssetNotFoundError { message }
          }
        }
        """
        variables = {"assetKey": {"path": ["BMLL", "l2"]}, "parts": partitions}
        data = _graphql(url, query, variables)
        if "errors" in data:
            print("ERROR: GraphQL errors:", data["errors"])
            return 2
        node = data["data"]["assetNodeOrError"]
        if node["__typename"] != "AssetNode":
            print("ERROR: assetNodeOrError:", node)
            return 3
        materialized = {m["partition"] for m in node["assetMaterializations"]}
        missing = set(partitions) - materialized
        if missing:
            print("FAIL: UI/GraphQL missing partitions:")
            for part in sorted(missing):
                print(f"  {part}")
            return 1
        print("PASS: UI/GraphQL sees all injected partitions")
        return 0
    finally:
        try:
            if proc.stdout is not None:
                rest = proc.stdout.read()
                if rest:
                    output_buffer.extend(rest.splitlines())
        except Exception:
            pass
        print(f"dagster dev exit code: {proc.poll()}")
        if output_buffer:
            print("dagster dev output (tail):")
            for line in output_buffer[-80:]:
                print(line)
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
