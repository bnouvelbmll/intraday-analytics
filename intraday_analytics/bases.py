import polars as pl
from typing import List, Dict, Optional, Any
from abc import ABC, abstractmethod

from intraday_analytics.utils import dc, ffill_with_shifts


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
    """

    def __init__(self, name: str, specific_fill_cols=None, nanoseconds=None):
        super().__init__(name, specific_fill_cols=specific_fill_cols)
        self.nanoseconds = nanoseconds
        self._tables = {"l2": None, "l3": None, "trades": None}

    @abstractmethod
    def tw_analytics(self, **tables):
        """
        Performs time-weighted analytics calculations.
        """
        raise NotImplemented

    def compute(self) -> pl.LazyFrame:
        """
        Computes the time-weighted analytics for the module.
        """
        nanoseconds = self.nanoseconds
        gcol_list = ["MIC", "ListingId", "Ticker", "TimeBucket", "CurrencyCode"]
        rt = {}

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
