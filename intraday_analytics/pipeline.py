import polars as pl
from typing import List, Dict, Optional, Callable, Any
from abc import ABC, abstractmethod
import logging

from .analytics.dense import DenseAnalytics
from .analytics.trade import TradeAnalytics
from .analytics.l2 import L2Analytics
from .analytics.l3 import L3Analytics
from .analytics.execution import ExecutionAnalytics
from .analytics.generic import GenericAnalytics
from .configuration import PassConfig
from .utils import dc, ffill_with_shifts, assert_unique_lazy
from .utils import SYMBOL_COL

logger = logging.getLogger(__name__)


class BaseAnalytics(ABC):
    """
    Abstract base class for all analytics modules.
    """

    def __init__(
        self,
        name: str,
        specific_fill_cols=None,
        join_keys: List[str] = ["ListingId", "TimeBucket"],
    ):
        self.name = name
        self.join_keys = join_keys
        self.df: Optional[pl.DataFrame] = None
        self.l2 = None
        self.l3 = None
        self.trades = None
        self.marketstate = None
        self.specific_fill_cols = specific_fill_cols or {}
        self.context: Dict[str, Any] = {}

    @abstractmethod
    def compute(self, **kwargs) -> pl.LazyFrame:
        """
        Computes the analytics for the module.
        """
        raise NotImplementedError

    def join(
        self, base_df: pl.LazyFrame, other_specific_cols, default_ffill=False
    ) -> pl.LazyFrame:
        """
        Joins the computed analytics to a base DataFrame.

        This method handles the merging of the module's results with the main
        DataFrame being built by the pipeline. It also provides options for
        filling null values that may result from the join.
        """
        if self.df is None:
            raise ValueError(f"{self.name} has no computed data.")

        r = dc(
            base_df.join(
                self.df, on=self.join_keys, how="full", suffix=f"_{self.name}"
            ),
            f"_{self.name}",
        )

        ra = {}
        rccc = r.collect_schema().names()
        for c, a in {**self.specific_fill_cols, **(other_specific_cols or {})}.items():
            if c in rccc:
                if a == "zero":
                    ra[c] = pl.col(c).fill_null(0)
                elif a == "last":
                    ra[c] = pl.col(c).forward_fill().over("ListingId")
        if default_ffill:
            for c in rccc:
                if c not in ra and c not in self.join_keys:
                    ra[c] = pl.col(c).forward_fill().over("ListingId")

        if ra:
            r = r.with_columns(**ra)
        return r


class BaseTWAnalytics(BaseAnalytics):
    """
    A base class for time-weighted analytics modules.

    This class extends `BaseAnalytics` and provides a framework for computing
    time-weighted metrics. It includes logic for resampling data and applying
    time-weighted calculations. The `tw_analytics` method must be implemented
    by subclasses.
    """

    def __init__(self, name: str, specific_fill_cols=None, nanoseconds=None):
        super().__init__(name, specific_fill_cols=specific_fill_cols)
        self.nanoseconds = nanoseconds
        self._tables = {"l2": None, "l3": None, "trades": None}

    @abstractmethod
    def tw_analytics(self, **tables):
        """
        Performs time-weighted analytics calculations.

        This method must be implemented by subclasses. It receives the resampled
        data and should return a LazyFrame with the time-weighted metrics.
        """
        raise NotImplemented

    def compute(self) -> pl.LazyFrame:
        """
        Computes the time-weighted analytics for the module.

        This method orchestrates the resampling of data and calls the
        `tw_analytics` method to perform the actual calculations.
        """
        nanoseconds = self.nanoseconds
        gcol_list = ["MIC", "ListingId", "Ticker", "TimeBucket", "CurrencyCode"]
        rt = {}

        # Also there is question dow we ant to ffill
        for tn in self._tables.keys():
            t = getattr(self, tn)
            if t is None:
                continue
            tcn = t.collect_schema().names()
            col_list = [c for c in tcn if c not in gcol_list]
            if "TimeBucket" not in tcn:
                raise ValueError("TimeBucket is required for metrics")

            t2_twr = ffill_with_shifts(
                t,
                gcol_list,
                "EventTimestamp",
                col_list,
                [(pl.duration(nanoseconds=nanoseconds * o)) for o in [-1, 0, 1]],
                lambda x: pl.when(x == x.dt.truncate(f"{nanoseconds}ns"))
                .then(x)
                .otherwise(
                    x.dt.truncate(f"{nanoseconds}ns")
                    + pl.duration(nanoseconds=nanoseconds)
                ),
            )

            t2_twr = t2_twr.with_columns(
                ((pl.col("ListingId").diff(-1)).cast(bool).cast(int)).alias("DP"),
            ).with_columns(
                (
                    (1 - pl.col("DP"))
                    * ((-pl.col("EventTimestamp").diff(-1)).clip(0, 10**12))
                ).alias("DT")
            )

            rt[tn] = t2_twr

        self.df = self.tw_analytics(**rt)
        return self.df


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

        for module in self.modules:
            # Provide the context to the module
            module.context = self.context

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
                assert getattr(module, r) is not None

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
        else:
            result = self.pipeline.run_on_multi_tables(**batch_data)
            self.out_writer(result, "batch")


# --- Generic Pipeline Factory ---


def create_pipeline(
    pass_config: PassConfig,
    context: dict,
    ref: pl.DataFrame,
    custom_modules: Dict[str, BaseAnalytics] = None,
    **kwargs,
) -> AnalyticsPipeline:
    """
    Constructs an analytics pipeline from a configuration using a module registry.

    Args:
        pass_config: The configuration for the specific analytics pass.
        context: The shared context dictionary.
        ref: The reference data DataFrame.
        custom_modules: A dictionary of custom analytics modules to register.
        **kwargs: Additional arguments (e.g., symbols, date) that are ignored by
                  this factory but might be passed by the executor.

    Returns:
        An `AnalyticsPipeline` instance.
    """
    # Default registry of framework modules
    module_registry = {
        "dense": lambda: DenseAnalytics(ref, pass_config.dense_analytics),
        "trade": lambda: TradeAnalytics(pass_config.trade_analytics),
        "l2": lambda: L2Analytics(pass_config.l2_analytics),
        "l3": lambda: L3Analytics(pass_config.l3_analytics),
        "execution": lambda: ExecutionAnalytics(pass_config.execution_analytics),
        "generic": lambda: GenericAnalytics(pass_config.generic_analytics),
    }

    # Add any custom modules provided by the user
    if custom_modules:
        module_registry.update(custom_modules)

    # Build the list of module instances for this pass
    modules = []
    for module_name in pass_config.modules:
        factory = module_registry.get(module_name)
        if factory:
            modules.append(factory())
        else:
            logging.warning(f"Module '{module_name}' not recognized in factory.")

    return AnalyticsPipeline(modules, pass_config, context)
