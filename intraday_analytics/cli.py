import importlib
import logging
from typing import Callable, Optional

import fire

from intraday_analytics.configuration import AnalyticsConfig
from intraday_analytics.execution import run_multiday_pipeline


def _load_universe_override(spec: str) -> Callable:
    """
    Load a universe selector from intraday_analytics/universes/<name>.py.

    Spec format: "<module>=<value>" or "<module>".
    Module must expose get_universe(date, value).
    """
    if "=" in spec:
        module_name, value = spec.split("=", 1)
    else:
        module_name, value = spec, None

    module = importlib.import_module(f"intraday_analytics.universes.{module_name}")
    if not hasattr(module, "get_universe"):
        raise ValueError(f"Universe module {module_name} must define get_universe(date, value).")

    def _wrapper(date):
        return module.get_universe(date, value)

    return _wrapper


def run_cli(
    user_config: dict,
    default_get_universe: Callable,
    get_pipeline: Optional[Callable] = None,
    date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    datasetname: Optional[str] = None,
    temp_dir: Optional[str] = None,
    prepare_data_mode: Optional[str] = None,
    batch_freq: Optional[str] = None,
    num_workers: Optional[int] = None,
    eager_execution: bool = False,
    universe: Optional[str] = None,
) -> None:
    """
    Run a standard CLI flow using a base USER_CONFIG and a default universe.
    """
    config_overrides = {}
    if date:
        config_overrides["START_DATE"] = date
        config_overrides["END_DATE"] = date
    if start_date:
        config_overrides["START_DATE"] = start_date
    if end_date:
        config_overrides["END_DATE"] = end_date
    if datasetname:
        config_overrides["DATASETNAME"] = datasetname
    if temp_dir:
        config_overrides["TEMP_DIR"] = temp_dir
    if prepare_data_mode:
        config_overrides["PREPARE_DATA_MODE"] = prepare_data_mode
    if batch_freq is not None:
        config_overrides["BATCH_FREQ"] = batch_freq
    if num_workers is not None:
        config_overrides["NUM_WORKERS"] = num_workers
    if eager_execution:
        config_overrides["EAGER_EXECUTION"] = True

    config = AnalyticsConfig(**{**user_config, **config_overrides})

    get_universe = default_get_universe
    if universe:
        get_universe = _load_universe_override(universe)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    run_multiday_pipeline(
        config=config,
        get_universe=get_universe,
        get_pipeline=get_pipeline,
    )


def main():
    fire.Fire(run_cli)
