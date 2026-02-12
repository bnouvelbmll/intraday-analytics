#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
import textwrap
import yaml

DEFAULT_PROJECT = "intraday_dagster_demo"
DEFAULT_DEMO = "demo/01_ohlcv_bars.py"


def _run(cmd: list[str], cwd: Path | None = None) -> None:
    subprocess.run(cmd, check=True, cwd=cwd)


def _write_definitions(path: Path) -> None:
    path.write_text(
        textwrap.dedent(
            """
            import datetime as dt
            import importlib
            import importlib.util
            import os
            from pathlib import Path

            from dagster import (
                Definitions,
                DailyPartitionsDefinition,
                StaticPartitionsDefinition,
                TimeWindowPartitionsDefinition,
                MultiPartitionsDefinition,
                schedule,
            )
            from basalt.dagster.dagster_compat import (
                build_assets,
                build_assets_for_module,
                build_materialization_checks,
                build_materialization_checks_for_module,
                build_schedules,
                build_schedules_for_module,
                build_input_source_assets,
                build_s3_input_asset_checks,
                build_s3_input_observation_sensor,
                build_s3_input_sync_job,
                build_s3_input_sync_asset,
                list_cbbo_partitions_from_s3,
                build_universe_partitions,
                default_universes,
            )
            import yaml

            def _load_yaml():
                cfg_path = os.path.join(os.path.dirname(__file__), "definitions.yaml")
                if not os.path.exists(cfg_path):
                    return {}
                with open(cfg_path, "r") as fh:
                    return yaml.safe_load(fh) or {}

            def _load_pipeline_module(path_or_name: str):
                p = Path(path_or_name)
                if p.exists():
                    spec = importlib.util.spec_from_file_location(p.stem, p)
                    if spec is None or spec.loader is None:
                        return None
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)  # type: ignore[attr-defined]
                    return module
                return importlib.import_module(path_or_name)

            def _deep_merge(a: dict, b: dict) -> dict:
                out = dict(a)
                for k, v in b.items():
                    if isinstance(v, dict) and isinstance(out.get(k), dict):
                        out[k] = _deep_merge(out[k], v)
                    else:
                        out[k] = v
                return out

            def _apply_overrides(module, overrides: dict | None) -> None:
                if not overrides:
                    return
                base = getattr(module, "USER_CONFIG", {}) or {}
                module.USER_CONFIG = _deep_merge(base, overrides)

            def _available_mics():
                import bmll.reference
                return (
                    bmll.reference.available_markets()
                    .query("Schema=='Equity'")
                    .query("IsAlive")
                    ["MIC"]
                    .tolist()
                )

            def _build_date_partitions():
                start_date = "2015-01-01"
                end_date = (dt.date.today() - dt.timedelta(days=1)).isoformat()
                batch_freq = os.getenv("BATCH_FREQ")
                if batch_freq == "D":
                    date_partitions = DailyPartitionsDefinition(start_date=start_date, end_date=end_date)
                else:
                    cron = "0 0 * * *"
                    if batch_freq == "W":
                        cron = "0 0 * * 1"
                    elif batch_freq == "2W":
                        cron = "0 0 */14 * *"
                    elif batch_freq == "M":
                        cron = "0 0 1 * *"
                    elif batch_freq == "A":
                        cron = "0 0 1 1 *"
                    date_partitions = TimeWindowPartitionsDefinition(
                        start=start_date,
                        end=end_date,
                        fmt="%Y-%m-%d",
                        cron_schedule=cron,
                    )
                universes = default_universes()
                universe_partitions = StaticPartitionsDefinition(build_universe_partitions(universes))
                return MultiPartitionsDefinition({"universe": universe_partitions, "date": date_partitions})

            def _build_daily_partitions():
                start_date = "2015-01-01"
                end_date = (dt.date.today() - dt.timedelta(days=1)).isoformat()
                return DailyPartitionsDefinition(start_date=start_date, end_date=end_date)

            def _cbbo_partitions():
                cbbo = os.getenv("CBBO_PARTITIONS", "cbbo")
                values = [v.strip() for v in cbbo.split(",") if v.strip()]
                if values != ["cbbo"]:
                    return StaticPartitionsDefinition(values)
                return StaticPartitionsDefinition(list_cbbo_partitions_from_s3())

            cfg = _load_yaml()
            demo_pipeline = cfg.get("DEMO_PIPELINE") or os.getenv("DEMO_PIPELINE")
            overrides_map = cfg.get("PIPELINE_OVERRIDES", {}) or {}

            partitions_def = _build_date_partitions()
            daily_partitions = _build_daily_partitions()
            mic_partitions = StaticPartitionsDefinition(_available_mics())
            cbbo_partitions = _cbbo_partitions()

            input_assets, input_table_map = build_input_source_assets(
                date_partitions_def=daily_partitions,
                mic_partitions_def=mic_partitions,
                cbbo_partitions_def=cbbo_partitions,
                asset_key_prefix=["BMLL"],
                date_dim="date",
                mic_dim="mic",
                cbbo_dim="cbbo",
                group_name="BMLL",
            )

            if demo_pipeline:
                module = _load_pipeline_module(demo_pipeline)
                if module is None:
                    raise RuntimeError(f"Could not load demo pipeline: {demo_pipeline}")
                key = demo_pipeline
                stem = Path(demo_pipeline).stem
                overrides = overrides_map.get(key) or overrides_map.get(stem)
                _apply_overrides(module, overrides)
                demo_assets = build_assets_for_module(
                    module,
                    partitions_def=partitions_def,
                    split_passes=True,
                    input_asset_keys=[asset.key for asset in input_assets],
                )
                demo_schedules = build_schedules_for_module(
                    module,
                    partitions_def=partitions_def,
                    split_passes=True,
                )
                demo_checks = build_materialization_checks_for_module(
                    module,
                    split_passes=True,
                    check_mode=os.getenv("S3_CHECK_MODE", "recursive"),
                )
            else:
                demo_assets = build_assets(
                    partitions_def=partitions_def,
                    split_passes=True,
                    input_asset_keys=[asset.key for asset in input_assets],
                )
                demo_schedules = build_schedules(
                    partitions_def=partitions_def,
                    split_passes=True,
                )
                demo_checks = build_materialization_checks(
                    check_mode=os.getenv("S3_CHECK_MODE", "recursive"),
                    split_passes=True,
                )

            input_checks = build_s3_input_asset_checks(
                assets=input_assets,
                table_map=input_table_map,
                check_mode=os.getenv("S3_CHECK_MODE", "recursive"),
            )
            input_sensor = build_s3_input_observation_sensor(
                name="s3_input_observation",
                assets=input_assets,
                table_map=input_table_map,
                check_mode=os.getenv("S3_CHECK_MODE", "recursive"),
                mics=_available_mics()[:5],
            )
            input_sync_job = build_s3_input_sync_job(
                name="s3_input_sync",
                assets=input_assets,
                table_map=input_table_map,
                check_mode=os.getenv("S3_CHECK_MODE", "recursive"),
            )
            input_sync_asset = build_s3_input_sync_asset(
                name="s3_input_sync_by_date",
                assets=input_assets,
                table_map=input_table_map,
                date_partitions_def=daily_partitions,
                check_mode=os.getenv("S3_CHECK_MODE", "recursive"),
                group_name="BMLL",
                use_db_bulk=True,
                repo_root=os.getenv("BMLL_REPO_ROOT"),
                asset_key_prefix=["BMLL"],
            )

            _cron = cfg.get("S3_SYNC_CRON")

            if _cron:
                @schedule(cron_schedule=_cron, job=input_sync_job, execution_timezone="UTC")
                def s3_sync_schedule():
                    return {}
                sync_schedule = s3_sync_schedule
            else:
                sync_schedule = None

            defs = Definitions(
                assets=[*demo_assets, *input_assets, input_sync_asset],
                asset_checks=[*demo_checks, *input_checks],
                sensors=[input_sensor],
                jobs=[input_sync_job],
                schedules=[*demo_schedules, *([sync_schedule] if sync_schedule else [])],
            )
            """
        ).lstrip()
    )


def _write_definitions_yaml(path: Path, demo_pipeline: str, enable_schedules: bool, cron: str, timezone: str, bmll_backend: bool, instance_size: int | None) -> None:
    data: dict = {
        "DEMO_PIPELINE": demo_pipeline,
    }
    overrides: dict = {}
    if enable_schedules:
        overrides.setdefault(Path(demo_pipeline).stem, {})
        overrides[Path(demo_pipeline).stem]["SCHEDULES"] = [
            {
                "name": "demo_schedule",
                "enabled": True,
                "cron": cron,
                "timezone": timezone,
            }
        ]
    if bmll_backend:
        overrides.setdefault(Path(demo_pipeline).stem, {})
        overrides[Path(demo_pipeline).stem]["BMLL_JOBS"] = {
            "enabled": True,
        }
        if instance_size is not None:
            overrides[Path(demo_pipeline).stem]["BMLL_JOBS"]["default_instance_size"] = instance_size
    if overrides:
        data["PIPELINE_OVERRIDES"] = overrides
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Setup Dagster demo project")
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--demo", default=DEFAULT_DEMO, help="Path to demo pipeline (default: demo/01_ohlcv_bars.py)")
    parser.add_argument("--mode", choices=["dev", "bmll"], default="dev", help="Run dagster dev or prepare for bmll scheduler")
    parser.add_argument("--enable-schedules", action="store_true")
    parser.add_argument("--schedule-cron", default="0 2 * * *")
    parser.add_argument("--schedule-timezone", default="UTC")
    parser.add_argument("--bmll-backend", action="store_true", help="Enable BMLL backend tags in demo config (default when --mode bmll)")
    parser.add_argument("--bmll-instance-size", type=int, default=None)

    args = parser.parse_args()

    project_dir = Path(args.project).resolve()
    definitions_file = project_dir / "definitions.py"
    definitions_yaml = project_dir / "definitions.yaml"

    os.environ.setdefault("DAGSTER_HOME", str(Path.home() / "user" / "my-dagster"))
    os.environ.setdefault("BMLL_REPO_ROOT", str(Path.cwd()))
    Path(os.environ["DAGSTER_HOME"]).mkdir(parents=True, exist_ok=True)

    _run([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])
    _run([sys.executable, "-m", "pip", "install", "dagster", "dagster-webserver"])

    if not project_dir.exists():
        _run(["dagster", "project", "scaffold", "--name", project_dir.name])

    _write_definitions(definitions_file)
    bmll_backend = args.bmll_backend or args.mode == "bmll"

    _write_definitions_yaml(
        definitions_yaml,
        demo_pipeline=args.demo,
        enable_schedules=args.enable_schedules,
        cron=args.schedule_cron,
        timezone=args.schedule_timezone,
        bmll_backend=bmll_backend,
        instance_size=args.bmll_instance_size,
    )

    if args.mode == "dev":
        _run(["dagster", "dev", "-f", str(definitions_file)])
        return 0

    print("\nDagster demo prepared for remote scheduler.")
    print(f"Definitions: {definitions_file}")
    print("\nSuggested commands:")
    print(f"  basalt dagster scheduler install --workspace {definitions_file}")
    print(f"  basalt dagster ui --workspace {definitions_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
