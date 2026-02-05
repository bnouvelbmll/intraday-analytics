import polars as pl
from typing import List, Dict, Optional, Callable, Any
from abc import ABC, abstractmethod
import logging

from .configuration import PassConfig
from .utils import dc, ffill_with_shifts, assert_unique_lazy
from .utils import SYMBOL_COL

logger = logging.getLogger(__name__)

from .analytics_base import BaseAnalytics, BaseTWAnalytics

class AnalyticsPipeline:
    """
    Orchestrates the execution of a series of analytics modules for a single pass.
    """

    def __init__(self, modules, config, pass_config, context=None):
        """
        Initializes the AnalyticsPipeline.

        Args:
            modules: A list of `BaseAnalytics` instances to be run.
            config: The global `AnalyticsConfig` instance.
            pass_config: The configuration for the specific analytics pass.
            context: A dictionary for sharing data between passes.
        """
        self.modules = modules
        self.config = config
        self.pass_config = pass_config
        self.context = context if context is not None else {}

    def run_on_multi_tables(
        self, **tables_for_sym: Dict[str, pl.DataFrame]
    ) -> pl.DataFrame:
        """
        Runs the analytics pipeline on a set of data tables for a single pass.
        """
        base: pl.LazyFrame | None = None
        prev_specific_cols = {}

        join_keys_override = None
        if self.pass_config.sort_keys:
            join_keys_override = list(self.pass_config.sort_keys)
            if "TimeBucket" not in join_keys_override:
                join_keys_override.append("TimeBucket")

        for module in self.modules:
            # Provide the context to the module
            module.context = self.context
            if join_keys_override is not None:
                module.join_keys = join_keys_override
                if hasattr(module, "config") and hasattr(module.config, "symbol_cols"):
                    module.config.symbol_cols = [
                        k for k in join_keys_override if k != "TimeBucket"
                    ]

            # Provide the necessary data tables to the module
            for key in self.config.TABLES_TO_LOAD:
                if (
                    (key in module.REQUIRES) or (key in ["marketstate"])
                ) and key in tables_for_sym:
                    if hasattr(tables_for_sym[key], "lazy"):
                        setattr(module, key, tables_for_sym[key].lazy())
                    else:
                        setattr(module, key, tables_for_sym[key])

            # Ensure all required tables are present
            for r in module.REQUIRES:
                assert getattr(module, r) is not None, f"{r} is not loaded"

            # Compute the analytics for the module
            lf_result = module.compute()

            if self.config.EAGER_EXECUTION:
                module.df = lf_result.collect().lazy()
                lf_result = module.df

            # Assert uniqueness of the result
            lf_result = assert_unique_lazy(
                lf_result, module.join_keys, name=str(module)
            )

            if self.config.EAGER_EXECUTION:
                lf_result = lf_result.collect().lazy()

            # Join the result with the base DataFrame
            if base is None:
                base = lf_result
                prev_specific_cols = module.specific_fill_cols
            else:
                base = module.join(base, prev_specific_cols, self.config.DEFAULT_FFILL)
                if self.config.EAGER_EXECUTION:
                    base = base.collect().lazy()
                prev_specific_cols.update(module.specific_fill_cols)

        # Store the result in the context for subsequent passes
        self.context[self.pass_config.name] = base.collect()
        return self.context[self.pass_config.name]

    def save(self, df: pl.DataFrame, path: str, profile: bool = False):
        """
        Saves the pipeline's output to a Parquet file.

        Args:
            df: The DataFrame to be saved.
            path: The path to the output Parquet file.
            profile: If True, profiles the sorting operation before saving.
                            Overrides ENABLE_POLARS_PROFILING config if set to True.
        """
        should_profile = profile or self.config.ENABLE_POLARS_PROFILING
        if should_profile:
            res = df.sort(["ListingId", "TimeBucket"]).profile(show_plot=True)
            return res
        df.sort(["ListingId", "TimeBucket"]).sink_parquet(
            path, region="us-east-1"
        )  # Ben;, region='us-east-1'
        return df


class AnalyticsRunner:
    """
    Dispatches batches of data to an analytics pipeline for processing.

    This class takes batches of data, runs them through a given
    `AnalyticsPipeline`, and uses a writer function to output the results.
    It can be configured to process data for each symbol in a batch individually.
    """

    def __init__(
        self,
        pipeline: AnalyticsPipeline,
        out_writer: Callable[[pl.DataFrame, str], None],
        config: dict,
    ):
        self.pipeline = pipeline
        self.out_writer = out_writer
        self.config = config

    def run_batch(self, batch_data: Dict[str, pl.DataFrame]):
        """
        Runs a single batch of data through the analytics pipeline.

        Args:
            batch_data: A dictionary where keys are table names and values are
                        DataFrames containing the data for the batch.
        """
        if not len(batch_data):
            logging.warning("Empty batch received.")
            return

        # RUN SYMBOL BY SYMBOL
        if self.config.RUN_ONE_SYMBOL_AT_A_TIME:
            for sym in sorted(
                    set().union(
                        *[
                            df.select(SYMBOL_COL).collect()[SYMBOL_COL].to_list()
                            for df in batch_data.values()
                            if len(df) > 0
                    ]
                )
            ):
                tables_for_sym = {
                    name: df.filter(pl.col(SYMBOL_COL) == sym)
                    for name, df in batch_data.items()
                }
                result = self.pipeline.run_on_multi_tables(**tables_for_sym)
                self.out_writer(result, sym)
            return result if "result" in locals() else None
        else:
            result = self.pipeline.run_on_multi_tables(**batch_data)
            self.out_writer(result, "batch")
            return result


# --- Generic Pipeline Factory ---


def create_pipeline(
    pass_config: PassConfig,
    context: dict,
    ref: pl.DataFrame,
    custom_modules: Dict[str, BaseAnalytics] = None,
    **kwargs,
) -> "AnalyticsPipeline":
    """
    Constructs an analytics pipeline from a configuration using a module registry.
    """
    from intraday_analytics.analytics_registry import build_module_registry

    module_registry = build_module_registry(pass_config, ref)

    # Add any custom modules provided by the user
    if custom_modules:
        module_registry.update(custom_modules)

    # Build the list of module instances for this pass
    modules = []
    module_names = list(pass_config.modules)
    if pass_config.trade_analytics.enable_retail_imbalance:
        if "retail_imbalance" not in module_names:
            module_names.append("retail_imbalance")
    if pass_config.trade_analytics.use_tagged_trades:
        if "iceberg" not in module_names:
            module_names.insert(0, "iceberg")
        else:
            try:
                trade_idx = module_names.index("trade")
                iceberg_idx = module_names.index("iceberg")
                if iceberg_idx > trade_idx:
                    module_names.pop(iceberg_idx)
                    module_names.insert(trade_idx, "iceberg")
            except ValueError:
                pass
    for module_name in module_names:
        factory = module_registry.get(module_name)
        if isinstance(factory, BaseAnalytics):
            modules.append(factory)
        elif callable(factory):
            modules.append(factory())
        else:
            logging.warning(f"Module '{module_name}' not recognized in factory.")

    config = kwargs.get("config")
    if config is None:
        raise ValueError("create_pipeline requires config=AnalyticsConfig")

    return AnalyticsPipeline(modules, config, pass_config, context)
