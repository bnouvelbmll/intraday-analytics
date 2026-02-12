import polars as pl
from typing import List, Dict, Optional, Callable, Any
from abc import ABC, abstractmethod
import logging

from .configuration import PassConfig
from .utils import dc, ffill_with_shifts, assert_unique_lazy
from .utils import SYMBOL_COL
from .polars_engine import collect_lazy

logger = logging.getLogger(__name__)

from .analytics_base import BaseAnalytics, BaseTWAnalytics


def _order_modules_for_timeline(module_names: list[str]) -> list[str]:
    time_modules = [m for m in module_names if m in {"dense", "external_events"}]
    other_modules = [m for m in module_names if m not in {"dense", "external_events"}]
    return time_modules + other_modules


def _resolve_module_inputs(pass_config: PassConfig, module: BaseAnalytics) -> dict[str, str]:
    overrides = pass_config.module_inputs or {}
    module_key = getattr(module, "name", None)
    if not module_key or module_key not in overrides:
        return {}
    entry = overrides.get(module_key)
    requires = list(getattr(module, "REQUIRES", []) or [])
    if isinstance(entry, str):
        if len(requires) != 1:
            raise ValueError(
                f"module_inputs for '{module_key}' must be a mapping when REQUIRES has multiple entries."
            )
        return {requires[0]: entry}
    if isinstance(entry, dict):
        return {str(k): str(v) for k, v in entry.items()}
    raise ValueError(
        f"module_inputs for '{module_key}' must be a string or mapping."
    )


def _to_df(value: Any, config) -> pl.DataFrame | None:
    if value is None:
        return None
    if isinstance(value, pl.DataFrame):
        return value
    if isinstance(value, pl.LazyFrame):
        return collect_lazy(value, config=config)
    if hasattr(value, "lazy"):
        try:
            return value.lazy().collect()
        except Exception:
            return None
    return None


def _reference_input_df(
    pass_config: PassConfig,
    modules: list[BaseAnalytics],
    tables_for_sym: dict[str, Any],
    context: dict[str, Any],
    config,
) -> pl.DataFrame | None:
    for module in modules:
        module_inputs = _resolve_module_inputs(pass_config, module)
        for req in list(getattr(module, "REQUIRES", []) or []):
            source = module_inputs.get(req, req)
            if source in tables_for_sym:
                ref = _to_df(tables_for_sym[source], config)
                if ref is not None:
                    return ref
            if source in context:
                ref = _to_df(context[source], config)
                if ref is not None:
                    return ref
    for value in tables_for_sym.values():
        ref = _to_df(value, config)
        if ref is not None:
            return ref
    return None


def _validate_pass_expectations(
    pass_config: PassConfig,
    output_df: pl.DataFrame,
    reference_df: pl.DataFrame | None,
) -> None:
    exp = pass_config.pass_expectations
    checks_enabled = any(
        [
            exp.preserve_listing_id_count,
            exp.preserve_listing_ids,
            exp.preserve_rows,
            exp.preserve_existing_columns,
            exp.all_non_nans,
        ]
    )
    if not checks_enabled:
        return

    failures: list[str] = []
    if reference_df is None:
        failures.append("no reference input available for expectation checks")
    else:
        if exp.preserve_rows and output_df.height != reference_df.height:
            failures.append(
                f"preserve_rows failed: output={output_df.height} reference={reference_df.height}"
            )
        if exp.preserve_existing_columns:
            missing = sorted(set(reference_df.columns) - set(output_df.columns))
            if missing:
                failures.append(
                    f"preserve_existing_columns failed: missing={missing}"
                )
        if exp.preserve_listing_id_count:
            if "ListingId" not in output_df.columns or "ListingId" not in reference_df.columns:
                failures.append(
                    "preserve_listing_id_count failed: ListingId missing in output or reference"
                )
            else:
                out_n = output_df.select(pl.col("ListingId").n_unique()).item()
                ref_n = reference_df.select(pl.col("ListingId").n_unique()).item()
                if out_n != ref_n:
                    failures.append(
                        f"preserve_listing_id_count failed: output={out_n} reference={ref_n}"
                    )
        if exp.preserve_listing_ids:
            if "ListingId" not in output_df.columns or "ListingId" not in reference_df.columns:
                failures.append(
                    "preserve_listing_ids failed: ListingId missing in output or reference"
                )
            else:
                out_ids = set(output_df.get_column("ListingId").to_list())
                ref_ids = set(reference_df.get_column("ListingId").to_list())
                if out_ids != ref_ids:
                    failures.append(
                        f"preserve_listing_ids failed: only_in_output={sorted(out_ids - ref_ids)[:5]} "
                        f"only_in_reference={sorted(ref_ids - out_ids)[:5]}"
                    )

    if exp.all_non_nans:
        exempt = set(exp.non_nan_exempt_columns or [])
        for col in output_df.columns:
            if col in exempt:
                continue
            series = output_df.get_column(col)
            nulls = series.null_count()
            nans = 0
            if series.dtype in (pl.Float32, pl.Float64):
                nans = output_df.select(pl.col(col).is_nan().sum()).item()
            if nulls or nans:
                failures.append(
                    f"all_non_nans failed: {col} has nulls={nulls}, nans={nans}"
                )

    if not failures:
        return

    message = (
        f"Pass expectations failed for pass '{pass_config.name}': " + "; ".join(failures)
    )
    if exp.action == "raise":
        raise AssertionError(message)
    logger.warning(message)


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
        if not self.modules:
            raise ValueError(
                f"Pass '{self.pass_config.name}' has no modules configured."
            )
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

            module_inputs = _resolve_module_inputs(self.pass_config, module)

            # Provide the necessary data tables to the module
            for req in module.REQUIRES:
                source = module_inputs.get(req, req)
                data = None
                if source in tables_for_sym:
                    data = tables_for_sym[source]
                elif source in self.context:
                    data = self.context[source]
                if data is not None:
                    if hasattr(data, "lazy"):
                        setattr(module, req, data.lazy())
                    else:
                        setattr(module, req, data)

            if "marketstate" in tables_for_sym and "marketstate" not in module.REQUIRES:
                data = tables_for_sym["marketstate"]
                setattr(module, "marketstate", data.lazy() if hasattr(data, "lazy") else data)

            # Ensure all required tables are present
            for r in module.REQUIRES:
                if getattr(module, r) is None:
                    source = module_inputs.get(r, r)
                    raise AssertionError(f"{r} is not loaded (source='{source}')")

            # Compute the analytics for the module
            lf_result = module.compute()

            if self.config.EAGER_EXECUTION:
                module.df = collect_lazy(lf_result, config=self.config).lazy()
                lf_result = module.df

            # Assert uniqueness of the result
            lf_result = assert_unique_lazy(
                lf_result, module.join_keys, name=str(module)
            )

            if self.config.EAGER_EXECUTION:
                lf_result = collect_lazy(lf_result, config=self.config).lazy()

            # Join the result with the base DataFrame
            if base is None:
                base = lf_result
                prev_specific_cols = module.specific_fill_cols
            else:
                timeline_mode = getattr(self.pass_config, "timeline_mode", None)
                if hasattr(timeline_mode, "value"):
                    timeline_mode = timeline_mode.value
                is_time_module = module.__class__.__module__.startswith("basalt.time.")
                use_asof = bool(timeline_mode == "event" and not is_time_module)
                base = module.join(
                    base,
                    prev_specific_cols,
                    self.config.DEFAULT_FFILL,
                    use_asof=use_asof,
                )
                if self.config.EAGER_EXECUTION:
                    base = collect_lazy(base, config=self.config).lazy()
                prev_specific_cols.update(module.specific_fill_cols)

        # Store the result in the context for subsequent passes
        output_df = collect_lazy(base, config=self.config)
        reference_df = _reference_input_df(
            self.pass_config,
            self.modules,
            tables_for_sym,
            self.context,
            self.config,
        )
        _validate_pass_expectations(self.pass_config, output_df, reference_df)
        self.context[self.pass_config.name] = output_df
        return self.context[self.pass_config.name]

    def save(self, df: pl.DataFrame | pl.LazyFrame, path: str, profile: bool = False):
        """
        Saves the pipeline's output to a Parquet file.

        Args:
            df: The DataFrame to be saved.
            path: The path to the output Parquet file.
            profile: If True, profiles the sorting operation before saving.
                            Overrides ENABLE_POLARS_PROFILING config if set to True.
        """
        should_profile = profile or self.config.ENABLE_POLARS_PROFILING
        is_lazy = isinstance(df, pl.LazyFrame)
        if should_profile:
            target = df if is_lazy else df.lazy()
            res = target.sort(["ListingId", "TimeBucket"]).profile(show_plot=True)
            return res
        if is_lazy:
            df.sort(["ListingId", "TimeBucket"]).sink_parquet(path)
        else:
            df.sort(["ListingId", "TimeBucket"]).write_parquet(path)
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
            symbols = set()
            for df in batch_data.values():
                if df is None:
                    continue
                cols = df.collect_schema().names() if hasattr(df, "collect_schema") else df.columns
                if SYMBOL_COL not in cols:
                    continue
                if isinstance(df, pl.LazyFrame):
                    sym_vals = collect_lazy(
                        df.select(SYMBOL_COL), config=self.config
                    )[SYMBOL_COL].to_list()
                else:
                    sym_vals = df[SYMBOL_COL].to_list()
                symbols.update(sym_vals)

            for sym in sorted(
                symbols
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
    from basalt.analytics_registry import build_module_registry

    module_registry = build_module_registry(pass_config, ref)

    # Add any custom modules provided by the user
    if custom_modules:
        module_registry.update(custom_modules)

    # Build the list of module instances for this pass
    modules = []
    module_names = _order_modules_for_timeline(list(pass_config.modules))
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

    if not modules:
        raise ValueError(
            f"Pass '{pass_config.name}' has no runnable modules. "
            "Set PassConfig.modules with at least one registered module."
        )

    config = kwargs.get("config")
    if config is None:
        raise ValueError("create_pipeline requires config=AnalyticsConfig")

    return AnalyticsPipeline(modules, config, pass_config, context)
