import unittest
import polars as pl
import pandas as pd
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch
from intraday_analytics.execution import run_metrics_pipeline, ProcessInterval
from intraday_analytics.configuration import AnalyticsConfig, PassConfig, PrepareDataMode
from intraday_analytics.pipeline import AnalyticsPipeline
from intraday_analytics.analytics.trade import TradeAnalytics

# Capture real scan_parquet before any patching
REAL_SCAN_PARQUET = pl.scan_parquet


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


# Helper functions for pickling
def mock_get_universe(date):
    return pl.DataFrame(
        {
            "ListingId": ["A"],
            "MIC": ["X"],
            "ISIN": ["I"],
            "IsPrimary": [True],
            "IsAlive": [True],
        }
    )


class MockPipelineFactory:
    def __init__(self, config):
        self.config = config

    def __call__(self, symbols=None, ref=None, date=None):
        return AnalyticsPipeline(
            [TradeAnalytics(self.config.trade_analytics)], self.config
        )


class TestEndToEnd(unittest.TestCase):
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
                )
            ],
            FINAL_OUTPUT_PATH_TEMPLATE=os.path.join(
                self.temp_dir, "final_{datasetname}_{start_date}_{end_date}.parquet"
            ),
            TABLES_TO_LOAD=["trades"],
            CLEAN_UP_TEMP_DIR=False,
            BATCH_FREQ=None,
        )

        # Create dummy source files
        self.source_dir = tempfile.mkdtemp()
        self.trades_file = os.path.join(self.source_dir, "trades.parquet")
        pl.DataFrame(
            {
                "ListingId": ["A"],
                "TradeTimestamp": [pd.Timestamp("2025-01-01 10:00:00").value],
                "Price": [10.0],
                "Size": [100],
                "Classification": ["LIT_CONTINUOUS"],
                "LocalPrice": [10.0],
                "MarketState": ["OPEN"],
                "Ticker": ["ABC"],
                "MIC": ["X"],
                "PricePoint": [0.5],
                "BMLLParticipantType": ["RETAIL"],
                "AggressorSide": [1],
                "TradeNotionalEUR": [1000.0],
            }
        ).write_parquet(self.trades_file)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        shutil.rmtree(self.source_dir)

    @patch("intraday_analytics.execution.ProcessInterval", side_effect=SyncProcessInterval)
    @patch("intraday_analytics.execution.get_files_for_date_range")
    def test_run_pipeline(self, mock_get_files, mock_process_interval):
        mock_get_files.return_value = [self.trades_file]

        def get_pipeline(pass_config, context, symbols=None, ref=None, date=None):
            modules = [TradeAnalytics(pass_config.trade_analytics)]
            return AnalyticsPipeline(modules, self.config, pass_config, context)

        run_metrics_pipeline(self.config, get_pipeline, mock_get_universe)

        expected_out = os.path.join(
            self.temp_dir, "final_sample2d_pass1_2025-01-01_2025-01-01.parquet"
        )
        self.assertTrue(os.path.exists(expected_out))

        df = pl.read_parquet(expected_out)
        self.assertEqual(len(df), 1)
        self.assertEqual(df["ListingId"][0], "A")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    unittest.main()
