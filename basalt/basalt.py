from __future__ import annotations

import os
import sys
import fire

from basalt import SUITE_FULL_NAME
from basalt.cli import (
    bmll_job_install,
    bmll_job_run,
    run_cli,
)
from basalt import schema_utils
from basalt import config_ui
from basalt.analytics_explain import main as analytics_explain_main


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

    path = Path(path_or_name)
    if path.exists():
        spec = importlib.util.spec_from_file_location(path.stem, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Unable to load pipeline: {path_or_name}")
        module = importlib.util.module_from_spec(spec)
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


def _job_run(*, pipeline: str | None = None, **kwargs):
    if _help_requested():
        from fire import helptext

        _print_suite_header()
        print(helptext.HelpText(bmll_job_run))
        return None
    if pipeline and "config_file" not in kwargs:
        kwargs["config_file"] = pipeline
    return bmll_job_run(**kwargs)


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


class JobCLI:
    @staticmethod
    def run(*args, **kwargs):
        return _job_run(*args, **kwargs)

    @staticmethod
    def install(*args, **kwargs):
        return bmll_job_install(*args, **kwargs)


def main():
    _disable_fire_pager()
    if _help_requested():
        _print_suite_header()
    root = {
        "analytics": AnalyticsCLI,
        "pipeline": PipelineCLI,
        "job": JobCLI,
    }
    root.update(_load_cli_extensions())
    fire.Fire(root)


if __name__ == "__main__":
    main()
