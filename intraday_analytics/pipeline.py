import polars as pl
from typing import List, Dict, Optional, Callable, Any
from abc import ABC, abstractmethod

from intraday_analytics.utils import dc, ffill_with_shifts, assert_unique_lazy, lprint


class BaseAnalytics(ABC):
    """
    Abstract base class for all analytics modules.

    This class defines the basic structure and interface for analytics modules.
    Each module is responsible for computing a specific set of metrics from the
    input data. The `compute` method must be implemented by all subclasses.
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

    @abstractmethod
    def compute(self, **kwargs) -> pl.LazyFrame:
        """
        Computes the analytics for the module.

        This method must be implemented by subclasses. It should perform the
        necessary computations and return a LazyFrame with the results.
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
    Orchestrates the execution of a series of analytics modules.

    This class manages the entire analytics pipeline, from running individual
    modules to joining their results into a final, comprehensive DataFrame.
    It takes a list of `BaseAnalytics` instances and runs them sequentially,
    passing the output of one module as the input to the next.
    """

    def __init__(self, modules, config, lprint):
        """
        Initializes the AnalyticsPipeline.

        Args:
            modules: A list of `BaseAnalytics` instances to be run.
            config: A dictionary of configuration settings for the pipeline.
            lprint: A logging function.
        """
        self.modules = modules
        self.config = config
        self.lprint = lprint

    # ----------------------------
    def run_on_multi_tables(
        self, **tables_for_sym: Dict[str, pl.DataFrame]
    ) -> pl.DataFrame:
        """
        Runs the analytics pipeline on a set of data tables.

        This method is the core of the pipeline's execution logic. It iterates
        through the analytics modules, provides them with the necessary data
        tables, and joins their results.

        Args:
            **tables_for_sym: A dictionary where keys are table names (e.g.,
                              'l2', 'l3') and values are the corresponding
                              DataFrames.

        Returns:
            A DataFrame containing the final, combined analytics results.
        """
        base: pl.LazyFrame | None = None

        prev_specific_cols = {}
        module_timings = {}
        for module in self.modules:
            for key in ["l2", "l3", "trades", "marketstate"]:
                # print(module, module.REQUIRES, key, tables_for_sym.keys())
                if (
                    (key in module.REQUIRES) or (key in ["marketstate"])
                ) and key in tables_for_sym:
                    # print(module, key)
                    if hasattr(tables_for_sym[key], "lazy"):
                        setattr(module, key, tables_for_sym[key].lazy())
                    else:
                        setattr(module, key, tables_for_sym[key])
            for r in module.REQUIRES:
                # print(module, r)
                assert getattr(module, r) is not None
            lf_result = module.compute()

            lf_result = assert_unique_lazy(
                lf_result, ["ListingId", "TimeBucket"], name=str(module)
            )

            if base is None:
                base = lf_result
                prev_specific_cols = module.specific_fill_cols
            else:
                base = module.join(
                    base, prev_specific_cols, self.config["DEFAULT_FFILL"]
                )
                prev_specific_cols.update(module.specific_fill_cols)
            if self.config["PROFILE"]:
                # Many null listing ids
                try:
                    print(module, "SHAPE", base.select(pl.len()).collect())
                except Exception as e:
                    print(e)

        # finally collect as eager DataFrame
        if self.config.get("ENABLE_PERFORMANCE_LOGS", False):
            r = base.profile(show_plot=False)
            self.lprint("/PERFORMANCE_LOGS - output shape = ", r[0].shape)
            print(r[1].with_columns(dt=pl.col("end") - pl.col("start")).sort("dt"))
            return r[0]
        else:
            return base.collect()

    def save(self, df: pl.DataFrame, path: str, profile: bool = False):
        """
        Saves the pipeline's output to a Parquet file.

        Args:
            df: The DataFrame to be saved.
            path: The path to the output Parquet file.
            profile: If True, profiles the sorting operation before saving.
                            Overrides ENABLE_POLARS_PROFILING config if set to True.
        """
        should_profile = profile or self.config.get("ENABLE_POLARS_PROFILING", False)
        if should_profile:
            res = df.sort(["ListingId", "TimeBucket"]).profile(show_plot=True)
            return res
        df.sort(["ListingId", "TimeBucket"]).sink_parquet(
            path, region="us-east-1"
        )  # Ben;, region='us-east-1'
        return df
