from __future__ import annotations

import os
import sys
import fire

from basalt import SUITE_FULL_NAME
from basalt.cli import (
    run_cli,
    resolve_user_config,
)
from basalt import schema_utils
from basalt import config_ui
from basalt.analytics_explain import main as analytics_explain_main
from basalt.plugins import list_plugins_payload
from basalt.configuration import AnalyticsConfig
from basalt.process import get_final_output_path
from basalt.orchestrator import _derive_tables_to_load
from basalt.tables import ALL_TABLES
import pandas as pd


def _disable_fire_pager() -> None:
    if "PAGER" not in os.environ:
        os.environ["PAGER"] = "cat"


def _schema_utils(*args, **kwargs):
    arg_list = list(args)
    for key, value in kwargs.items():
        flag = f"--{key.replace('_','-')}"
        if isinstance(value, bool):
            if value:
                arg_list.append(flag)
        else:
            arg_list.extend([flag, str(value)])
    if "--pipeline" in arg_list:
        from basalt.cli import _load_yaml_config, _extract_user_config
        import json
        import tempfile
        from pathlib import Path

        idx = arg_list.index("--pipeline")
        if idx + 1 < len(arg_list):
            pipeline_path = arg_list[idx + 1]
            yaml_path = Path(pipeline_path).with_suffix(".yaml")
            data = _load_yaml_config(str(yaml_path))
            user_cfg = _extract_user_config(data) if isinstance(data, dict) else {}
            temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
            temp.write(json.dumps(user_cfg).encode("utf-8"))
            temp.close()
            arg_list[idx:idx + 2] = ["--config", temp.name]
    sys.argv = ["schema_utils", *arg_list]
    return schema_utils.main()


def _config_ui(*args):
    sys.argv = ["config_ui", *args]
    return config_ui.main()

def _analytics_explain(*args):
    sys.argv = ["analytics_explain", *args]
    return analytics_explain_main()

def _load_pipeline_module(path_or_name: str):
    from pathlib import Path
    import importlib.util
    import importlib

    def _module_name_from_path(path: Path) -> str:
        # Prefer package-qualified names (e.g. demo.arnaud) so functions
        # remain pickleable across multiprocessing workers.
        parts = [path.stem]
        parent = path.parent
        while (parent / "__init__.py").exists():
            parts.append(parent.name)
            parent = parent.parent
        return ".".join(reversed(parts))

    path = Path(path_or_name)
    if path.exists():
        module_name = _module_name_from_path(path.resolve())
        existing = sys.modules.get(module_name)
        if existing is not None and getattr(existing, "__file__", None) == str(path.resolve()):
            return existing

        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Unable to load pipeline: {path_or_name}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)  # type: ignore[attr-defined]
        return module
    return importlib.import_module(path_or_name)


def _help_requested() -> bool:
    return any(arg in {"-h", "--help"} for arg in sys.argv)


def _print_suite_header() -> None:
    print(SUITE_FULL_NAME)


def _pipeline_run(*, pipeline: str | None = None, **kwargs):
    if _help_requested():
        _print_suite_header()
        print(
            "Usage: basalt pipeline run --pipeline <path_or_module> [options]\n"
            "Options are forwarded to the pipeline CLI (e.g. --start_date, --end_date, --batch_freq)."
        )
        return None
    if not pipeline:
        raise SystemExit("Provide --pipeline <path_or_module>")
    module = _load_pipeline_module(pipeline)
    if not hasattr(module, "USER_CONFIG"):
        raise SystemExit("Pipeline module must define USER_CONFIG.")
    if not hasattr(module, "get_universe"):
        raise SystemExit("Pipeline module must define get_universe(date).")
    config_file = getattr(module, "__file__", None) or pipeline
    kwargs.setdefault("config_file", config_file)
    return run_cli(
        module.USER_CONFIG,
        module.get_universe,
        get_pipeline=getattr(module, "get_pipeline", None),
        **kwargs,
    )


def _pipeline_datasets(
    *,
    pipeline: str | None = None,
    config_precedence: str = "yaml_overrides",
):
    if not pipeline:
        raise SystemExit("Provide --pipeline <path_or_module>")
    module = _load_pipeline_module(pipeline)
    if not hasattr(module, "USER_CONFIG"):
        raise SystemExit("Pipeline module must define USER_CONFIG.")
    config_file = getattr(module, "__file__", None) or pipeline
    user_cfg = resolve_user_config(
        dict(module.USER_CONFIG),
        module_file=config_file,
        precedence=config_precedence,
    )
    config = AnalyticsConfig(**user_cfg)

    start_date = str(config.START_DATE or "")
    end_date = str(config.END_DATE or start_date)
    if not start_date:
        start_date = end_date = pd.Timestamp.utcnow().date().isoformat()
    date_for_universe = start_date

    ref = None
    mics: list[str] = []
    try:
        if hasattr(module, "get_universe"):
            ref_raw = module.get_universe(date_for_universe)
            if hasattr(ref_raw, "to_pandas"):
                ref = ref_raw
            else:
                import polars as pl
                ref = pl.DataFrame(ref_raw)
            if "MIC" in ref.columns:
                mics = [x for x in ref["MIC"].drop_nulls().unique().to_list() if x is not None]
            elif "OPOL" in ref.columns:
                mics = [x for x in ref["OPOL"].drop_nulls().unique().to_list() if x is not None]
    except Exception:
        ref = None

    known_context: list[str] = []
    inputs_by_pass: dict[str, list[str]] = {}
    for pass_cfg in config.PASSES:
        tables = _derive_tables_to_load(
            pass_cfg,
            config.TABLES_TO_LOAD,
            known_context_sources=known_context,
        )
        known_context.append(pass_cfg.name)
        inputs_by_pass[pass_cfg.name] = tables

    inputs = []
    for pass_name, tables in inputs_by_pass.items():
        for table_name in tables:
            table = ALL_TABLES.get(table_name)
            if table is None:
                continue
            sample_paths = []
            if mics:
                try:
                    ts = pd.Timestamp(start_date)
                    sample_paths = table.get_s3_paths(mics[:2], ts.year, ts.month, ts.day)[:2]
                except Exception:
                    sample_paths = []
            inputs.append(
                {
                    "pass": pass_name,
                    "table": table_name,
                    "bmll_table": table.bmll_table_name,
                    "s3_folder": table.s3_folder_name,
                    "snowflake_table": table.snowflake_table_name(),
                    "databricks_table": table.databricks_table_name(),
                    "sample_bmll_paths": sample_paths,
                }
            )

    outputs = []
    for pass_cfg in config.PASSES:
        output_target = pass_cfg.output or config.OUTPUT_TARGET
        try:
            out_path = get_final_output_path(
                pd.Timestamp(start_date),
                pd.Timestamp(end_date),
                config,
                pass_cfg.name,
                output_target=output_target,
            )
        except Exception:
            template = str(getattr(output_target, "path_template", "") or "")
            out_path = template.format(
                bucket="",
                prefix="",
                datasetname=f"{config.DATASETNAME}_{pass_cfg.name}",
                **{"pass": pass_cfg.name},
                universe=config.UNIVERSE or "all",
                start_date=start_date,
                end_date=end_date,
            ).replace("//", "/")
        outputs.append(
            {
                "pass": pass_cfg.name,
                "output_type": str(getattr(output_target.type, "value", output_target.type)),
                "path": out_path,
            }
        )

    return {
        "pipeline": pipeline,
        "config_file": config_file,
        "data_source_path": [str(x.value if hasattr(x, "value") else x) for x in (config.DATA_SOURCE_PATH or [])],
        "date_range": {"start_date": start_date, "end_date": end_date},
        "inputs": inputs,
        "outputs": outputs,
    }


def _job_run(*, pipeline: str | None = None, **kwargs):
    if _help_requested():
        _print_suite_header()
        print("Usage: basalt bmll run --pipeline <path_or_module> [options]")
        return None
    return _bmll_run(pipeline=pipeline, **kwargs)


def _bmll_run(*, pipeline: str | None = None, **kwargs):
    try:
        from basalt.executors.bmll import bmll_run
    except Exception as exc:
        raise SystemExit("BMLL executor is not available in this installation.") from exc
    return bmll_run(pipeline=pipeline, **kwargs)


def _bmll_install(*, pipeline: str | None = None, **kwargs):
    try:
        from basalt.executors.bmll import bmll_install
    except Exception as exc:
        raise SystemExit("BMLL executor is not available in this installation.") from exc
    return bmll_install(pipeline=pipeline, **kwargs)


def _bmll_bootstrap_refresh(**kwargs):
    try:
        from basalt.cli import bmll_bootstrap_refresh
    except Exception as exc:
        raise SystemExit("BMLL bootstrap refresh is not available.") from exc
    return bmll_bootstrap_refresh(**kwargs)


def _ec2_run(*, pipeline: str | None = None, **kwargs):
    try:
        from basalt.executors.aws_ec2 import ec2_run
    except Exception as exc:
        raise SystemExit(
            "EC2 executor is not installed. Install bmll-basalt-aws-ec2."
        ) from exc
    return ec2_run(pipeline=pipeline, **kwargs)


def _ec2_install(*, pipeline: str | None = None, **kwargs):
    try:
        from basalt.executors.aws_ec2 import ec2_install
    except Exception as exc:
        raise SystemExit(
            "EC2 executor is not installed. Install bmll-basalt-aws-ec2."
        ) from exc
    return ec2_install(pipeline=pipeline, **kwargs)


def _k8s_run(*args, **kwargs):
    try:
        from basalt.executors.kubernetes import k8s_run
    except Exception as exc:
        raise SystemExit(
            "Kubernetes executor is not installed. Install bmll-basalt-kubernetes."
        ) from exc
    return k8s_run(*args, **kwargs)


def _k8s_install(*args, **kwargs):
    try:
        from basalt.executors.kubernetes import k8s_install
    except Exception as exc:
        raise SystemExit(
            "Kubernetes executor is not installed. Install bmll-basalt-kubernetes."
        ) from exc
    return k8s_install(*args, **kwargs)


def _load_cli_extensions() -> dict:
    try:
        from importlib.metadata import entry_points
    except Exception:
        try:
            from importlib_metadata import entry_points  # type: ignore[import-not-found]
        except Exception:
            return {}

    try:
        eps = entry_points(group="basalt.cli")
    except TypeError:
        eps = entry_points().get("basalt.cli", [])

    extensions: dict = {}
    for entry in eps:
        try:
            obj = entry.load()
        except Exception as exc:
            sys.stderr.write(f"Skipping CLI extension {entry.name}: {exc}\n")
            continue
        try:
            value = obj() if callable(obj) else obj
        except Exception as exc:
            sys.stderr.write(f"Skipping CLI extension {entry.name}: {exc}\n")
            continue
        if isinstance(value, dict):
            extensions.update(value)
        else:
            extensions[entry.name] = value
    return extensions


class AnalyticsCLI:
    @staticmethod
    def list(*args, **kwargs):
        return _schema_utils(*args, **kwargs)

    @staticmethod
    def explain(*args, **kwargs):
        return _analytics_explain(*args, **kwargs)


class PipelineCLI:
    @staticmethod
    def run(*args, **kwargs):
        return _pipeline_run(*args, **kwargs)

    @staticmethod
    def config(*args, **kwargs):
        return _config_ui(*args, **kwargs)

    @staticmethod
    def datasets(*args, **kwargs):
        return _pipeline_datasets(*args, **kwargs)


class JobCLI:
    @staticmethod
    def run(*args, **kwargs):
        return _job_run(*args, **kwargs)

    @staticmethod
    def install(*args, **kwargs):
        return _bmll_install(*args, **kwargs)


class BMLLCLI:
    @staticmethod
    def run(*args, **kwargs):
        return _bmll_run(*args, **kwargs)

    @staticmethod
    def install(*args, **kwargs):
        return _bmll_install(*args, **kwargs)

    @staticmethod
    def refresh_bootstrap(*args, **kwargs):
        return _bmll_bootstrap_refresh(*args, **kwargs)


class EC2CLI:
    @staticmethod
    def run(*args, **kwargs):
        return _ec2_run(*args, **kwargs)

    @staticmethod
    def install(*args, **kwargs):
        return _ec2_install(*args, **kwargs)


class K8SCLI:
    @staticmethod
    def run(*args, **kwargs):
        return _k8s_run(*args, **kwargs)

    @staticmethod
    def install(*args, **kwargs):
        return _k8s_install(*args, **kwargs)


class PluginsCLI:
    @staticmethod
    def list():
        return list_plugins_payload()


def main():
    _disable_fire_pager()
    if _help_requested():
        _print_suite_header()
    root = {
        "analytics": AnalyticsCLI,
        "pipeline": PipelineCLI,
        "job": JobCLI,
        "bmll": BMLLCLI,
        "ec2": EC2CLI,
        "k8s": K8SCLI,
        "plugins": PluginsCLI,
    }
    root.update(_load_cli_extensions())
    fire.Fire(root)


if __name__ == "__main__":
    main()
