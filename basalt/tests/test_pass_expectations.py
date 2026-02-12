from __future__ import annotations

import datetime as dt
import logging

import polars as pl

from basalt.analytics_base import BaseAnalytics
from basalt.configuration import AnalyticsConfig, PassConfig
from basalt.pipeline import AnalyticsPipeline


class _PassThroughTrades(BaseAnalytics):
    REQUIRES = ["trades"]

    def __init__(self):
        super().__init__("dummy_pass")

    def compute(self, **kwargs) -> pl.LazyFrame:
        return self.trades


class _DropOneRowTrades(BaseAnalytics):
    REQUIRES = ["trades"]

    def __init__(self):
        super().__init__("dummy_drop_row")

    def compute(self, **kwargs) -> pl.LazyFrame:
        return self.trades.filter(pl.col("ListingId") == "A")


class _DropColumnTrades(BaseAnalytics):
    REQUIRES = ["trades"]

    def __init__(self):
        super().__init__("dummy_drop_col")

    def compute(self, **kwargs) -> pl.LazyFrame:
        return self.trades.select(["ListingId", "TimeBucket"])


class _InjectNullTrades(BaseAnalytics):
    REQUIRES = ["trades"]

    def __init__(self):
        super().__init__("dummy_nulls")

    def compute(self, **kwargs) -> pl.LazyFrame:
        return self.trades.with_columns(pl.lit(None).cast(pl.Float64).alias("Bad"))


def _trades_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ListingId": ["A", "B"],
            "TimeBucket": [
                dt.datetime(2025, 1, 1, 10, 0, 0),
                dt.datetime(2025, 1, 1, 10, 0, 0),
            ],
            "Value": [1.0, 2.0],
        }
    )


def test_pass_expectations_preserve_listing_count_passes():
    pass_cfg = PassConfig(
        name="p1",
        modules=["trade"],
        pass_expectations={"preserve_listing_id_count": True},
    )
    config = AnalyticsConfig(PASSES=[pass_cfg])
    pipe = AnalyticsPipeline([_PassThroughTrades()], config, pass_cfg)
    out = pipe.run_on_multi_tables(trades=_trades_df())
    assert out.height == 2


def test_pass_expectations_preserve_rows_raises_when_broken():
    pass_cfg = PassConfig(
        name="p1",
        modules=["trade"],
        pass_expectations={"preserve_rows": True, "action": "raise"},
    )
    config = AnalyticsConfig(PASSES=[pass_cfg])
    pipe = AnalyticsPipeline([_DropOneRowTrades()], config, pass_cfg)
    try:
        pipe.run_on_multi_tables(trades=_trades_df())
        assert False, "Expected preserve_rows violation"
    except AssertionError as exc:
        assert "preserve_rows failed" in str(exc)


def test_pass_expectations_preserve_existing_columns_raises_when_broken():
    pass_cfg = PassConfig(
        name="p1",
        modules=["trade"],
        pass_expectations={"preserve_existing_columns": True, "action": "raise"},
    )
    config = AnalyticsConfig(PASSES=[pass_cfg])
    pipe = AnalyticsPipeline([_DropColumnTrades()], config, pass_cfg)
    try:
        pipe.run_on_multi_tables(trades=_trades_df())
        assert False, "Expected preserve_existing_columns violation"
    except AssertionError as exc:
        assert "preserve_existing_columns failed" in str(exc)


def test_pass_expectations_all_non_nans_detects_nulls():
    pass_cfg = PassConfig(
        name="p1",
        modules=["trade"],
        pass_expectations={"all_non_nans": True, "action": "raise"},
    )
    config = AnalyticsConfig(PASSES=[pass_cfg])
    pipe = AnalyticsPipeline([_InjectNullTrades()], config, pass_cfg)
    try:
        pipe.run_on_multi_tables(trades=_trades_df())
        assert False, "Expected all_non_nans violation"
    except AssertionError as exc:
        assert "all_non_nans failed" in str(exc)


def test_pass_expectations_warn_mode_logs_without_raise(caplog):
    caplog.set_level(logging.WARNING)
    pass_cfg = PassConfig(
        name="p1",
        modules=["trade"],
        pass_expectations={"preserve_rows": True, "action": "warn"},
    )
    config = AnalyticsConfig(PASSES=[pass_cfg])
    pipe = AnalyticsPipeline([_DropOneRowTrades()], config, pass_cfg)
    out = pipe.run_on_multi_tables(trades=_trades_df())
    assert out.height == 1
    assert "Pass expectations failed" in caplog.text
