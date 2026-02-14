import datetime as dt
import polars as pl

from basalt.analytics_base import BaseAnalytics
from types import SimpleNamespace
from basalt.configuration import PassConfig, SideOutputConfig
from basalt.pipeline import AnalyticsPipeline


class _SidePreprocess(BaseAnalytics):
    REQUIRES = ["trades"]

    def __init__(self):
        super().__init__("side_preprocess", {})

    def compute(self) -> pl.LazyFrame:
        df = self.trades
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        side = df.select(["ListingId", "TimeBucket", "Size"])
        self.set_side_output("aggressive_orders", side.lazy())
        return pl.DataFrame().lazy()


class _UseSide(BaseAnalytics):
    REQUIRES = ["trades"]

    def __init__(self):
        super().__init__("use_side", {})

    def compute(self) -> pl.LazyFrame:
        df = self.trades
        if isinstance(df, pl.DataFrame):
            df = df.lazy()
        self.df = df
        return df


def test_side_output_context_namespace_and_resolution():
    trades = pl.DataFrame(
        {
            "ListingId": [1, 1],
            "TimeBucket": [dt.datetime(2026, 2, 1, 10, 0, 0), dt.datetime(2026, 2, 1, 10, 1, 0)],
            "Size": [100.0, 200.0],
        }
    )
    pass_cfg = PassConfig(
        name="pass1",
        modules=["use_side"],
        preprocess_modules=["side_preprocess"],
        module_inputs={"use_side": "aggressive_orders"},
        side_output_namespace="catalog",
        side_outputs={
            "aggressive_orders": SideOutputConfig(materialize="auto"),
        },
    )
    config = SimpleNamespace(EAGER_EXECUTION=False, DEFAULT_FFILL=False)
    pipe = AnalyticsPipeline(
        modules=[_UseSide()],
        preprocess_modules=[_SidePreprocess()],
        config=config,
        pass_config=pass_cfg,
        context={},
    )
    out = pipe.run_on_multi_tables(trades=trades)
    assert out.select("Size").to_series().to_list() == [100.0, 200.0]
    assert "catalog:aggressive_orders" in pipe.context


def test_side_output_context_key_override():
    trades = pl.DataFrame(
        {
            "ListingId": [1],
            "TimeBucket": [dt.datetime(2026, 2, 1, 10, 0, 0)],
            "Size": [100.0],
        }
    )
    pass_cfg = PassConfig(
        name="pass1",
        modules=["use_side"],
        preprocess_modules=["side_preprocess"],
        module_inputs={"use_side": "global:agg"},
        side_outputs={
            "aggressive_orders": SideOutputConfig(
                materialize="auto",
                context_key="global:agg",
            ),
        },
    )
    config = SimpleNamespace(EAGER_EXECUTION=False, DEFAULT_FFILL=False)
    pipe = AnalyticsPipeline(
        modules=[_UseSide()],
        preprocess_modules=[_SidePreprocess()],
        config=config,
        pass_config=pass_cfg,
        context={},
    )
    out = pipe.run_on_multi_tables(trades=trades)
    assert out.height == 1
    assert "global:agg" in pipe.context
