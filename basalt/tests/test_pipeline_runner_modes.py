from __future__ import annotations

import os
import datetime as dt

import polars as pl

from basalt.analytics_base import BaseAnalytics
from basalt.configuration import AnalyticsConfig, PassConfig
from basalt.pipeline import AnalyticsPipeline, AnalyticsRunner


class _DummyTradesModule(BaseAnalytics):
    REQUIRES = ["trades"]

    def __init__(self):
        super().__init__("dummy")

    def compute(self, **kwargs) -> pl.LazyFrame:
        return self.trades.select(["ListingId", "TimeBucket"]).with_columns(
            pl.lit(1).alias("Dummy")
        )


def test_runner_one_symbol_mode_handles_dataframe_inputs():
    pass_cfg = PassConfig(name="pass1", modules=["trade"])
    config = AnalyticsConfig(
        PASSES=[pass_cfg],
        RUN_ONE_SYMBOL_AT_A_TIME=True,
        TABLES_TO_LOAD=["trades"],
    )
    pipeline = AnalyticsPipeline([_DummyTradesModule()], config, pass_cfg)

    trades = pl.DataFrame(
        {
            "ListingId": ["A", "B", "A"],
            "TimeBucket": [
                dt.datetime(2025, 1, 1, 10, 0, 0),
                dt.datetime(2025, 1, 1, 10, 0, 0),
                dt.datetime(2025, 1, 1, 10, 1, 0),
            ],
        }
    )

    written = {}

    def _writer(df: pl.DataFrame, key: str):
        written[str(key)] = df

    runner = AnalyticsRunner(pipeline, _writer, config)
    runner.run_batch({"trades": trades})

    assert set(written.keys()) == {"A", "B"}
    assert written["A"].filter(pl.col("ListingId") == "A").height > 0
    assert written["B"].filter(pl.col("ListingId") == "B").height > 0


def test_pipeline_save_supports_dataframe_and_lazyframe(tmp_path):
    pass_cfg = PassConfig(name="pass1", modules=["trade"])
    config = AnalyticsConfig(PASSES=[pass_cfg], TABLES_TO_LOAD=["trades"])
    pipeline = AnalyticsPipeline([], config, pass_cfg)

    df = pl.DataFrame(
        {"ListingId": [1], "TimeBucket": [dt.datetime(2025, 1, 1, 10, 0, 0)]}
    )

    path_df = os.path.join(tmp_path, "out_df.parquet")
    pipeline.save(df, path_df)
    assert os.path.exists(path_df)

    path_lf = os.path.join(tmp_path, "out_lf.parquet")
    pipeline.save(df.lazy(), path_lf)
    assert os.path.exists(path_lf)
