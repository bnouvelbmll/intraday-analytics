import unittest
import polars as pl
import pandas as pd
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch

from basalt.configuration import AnalyticsConfig, PassConfig
from basalt.execution import run_metrics_pipeline, ProcessInterval
from basalt.pipeline import AnalyticsPipeline, BaseAnalytics
from basalt.analytics.trade import TradeAnalytics


# Helper class to run ProcessInterval synchronously
class SyncProcessInterval(ProcessInterval):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._exitcode = None

    def start(self):
        try:
            self.run()
            self._exitcode = 0
        except Exception:
            self._exitcode = 1
            raise

    def join(self):
        pass

    @property
    def exitcode(self):
        return self._exitcode


def mock_get_universe(date):
    return pl.DataFrame({"ListingId": ["A"], "MIC": ["X"]})


class SimpleAggregation(BaseAnalytics):
    def __init__(self, name="aggregation"):
        super().__init__(name)
        self.REQUIRES = []

    def compute(self, **kwargs) -> pl.LazyFrame:
        pass1_result = self.context["pass1"]
        return (
            pass1_result.lazy()
            .group_by("ListingId")
            .agg(pl.col("TradeVolume").mean().alias("MeanTradeVolume"))
        )


class TestMultiPassPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import sys

        sys.modules["bmll2"] = MagicMock()

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.config = AnalyticsConfig(
            START_DATE="2025-01-01",
            END_DATE="2025-01-01",
            TEMP_DIR=self.temp_dir,
            PASSES=[
                PassConfig(
                    name="pass1",
                    modules=["trade"],
                ),
                PassConfig(
                    name="pass2",
                    modules=["aggregation"],
                ),
            ],
            OUTPUT_TARGET={
                "path_template": os.path.join(
                    self.temp_dir,
                    "final_{datasetname}_{start_date}_{end_date}.parquet",
                )
            },
            TABLES_TO_LOAD=["trades"],
            CLEAN_UP_TEMP_DIR=False,
        )

        # Create dummy source files
        self.source_dir = tempfile.mkdtemp()
        self.trades_file = os.path.join(self.source_dir, "trades.parquet")
        pl.DataFrame(
            {
                "ListingId": ["A"],
                "TradeTimestamp": [pd.Timestamp("2025-01-01 10:00:00").value],
                "Size": [100],
                "Classification": ["LIT_CONTINUOUS"],
                "LocalPrice": [100.0],
                "MarketState": ["OPEN"],
                "Ticker": ["ABC"],
                "MIC": ["X"],
                "PricePoint": [0.5],
                "BMLLParticipantType": ["RETAIL"],
                "AggressorSide": [1],
                "TradeNotional": [10000.0],
                "TradeNotionalEUR": [10000.0],
            }
        ).write_parquet(self.trades_file)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        shutil.rmtree(self.source_dir)

    @patch(
        "basalt.execution.ProcessInterval", side_effect=SyncProcessInterval
    )
    @patch("basalt.execution.get_files_for_date_range")
    @patch(
        "basalt.execution.as_completed", side_effect=lambda futures: futures
    )
    @patch("basalt.execution.ProcessPoolExecutor")
    def test_selective_pass_execution(
        self,
        mock_process_pool_executor,
        mock_as_completed,
        mock_get_files,
        mock_process_interval,
    ):
        mock_get_files.return_value = [self.trades_file]

        def mock_submit(fn, *args, **kwargs):
            fn(*args, **kwargs)
            mock_future = MagicMock()
            mock_future.result.return_value = True
            return mock_future

        mock_process_pool_executor.return_value.__enter__.return_value.submit.side_effect = (
            mock_submit
        )

        self.config.PASSES = [
            PassConfig(
                name="pass1",
                modules=["trade"],
            )
        ]

        def get_pipeline(pass_config, context, symbols=None, ref=None, date=None):
            modules = [TradeAnalytics(pass_config.trade_analytics)]
            return AnalyticsPipeline(modules, self.config, pass_config, context)

        run_metrics_pipeline(
            config=self.config,
            get_universe=mock_get_universe,
            get_pipeline=get_pipeline,
        )

        # Verify output of the first pass
        expected_out_pass1 = os.path.join(
            self.temp_dir,
            "final_sample2d_pass1_2024-12-30_2025-01-01.parquet",
        )
        self.assertTrue(os.path.exists(expected_out_pass1))

        # Verify that the output of the second pass does not exist
        expected_out_pass2 = os.path.join(
            self.temp_dir,
            "final_sample2d_pass2_2024-12-30_2025-01-01.parquet",
        )
        self.assertFalse(os.path.exists(expected_out_pass2))


if __name__ == "__main__":
    unittest.main()
