import unittest
import polars as pl
import pandas as pd
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch
from basalt.orchestrator import run_metrics_pipeline, ProcessInterval
from basalt.configuration import (
    AnalyticsConfig,
    PassConfig,
    PrepareDataMode,
)

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
            "ListingId": [1],
            "MIC": ["X"],
            "ISIN": ["I"],
            "Ticker": ["ABC"],
            "CurrencyCode": ["EUR"],
            "IsPrimary": [True],
            "IsAlive": [True],
        }
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
                    modules=["dense", "trade"],
                )
            ],
            OUTPUT_TARGET={
                "path_template": os.path.join(
                    self.temp_dir, "final_{datasetname}_{start_date}_{end_date}.parquet"
                )
            },
            CLEAN_UP_TEMP_DIR=False,
            BATCH_FREQ=None,
        )

        # Create dummy source files
        self.source_dir = tempfile.mkdtemp()
        self.trades_file = os.path.join(self.source_dir, "trades.parquet")
        self.marketstate_file = os.path.join(self.source_dir, "marketstate.parquet")
        pl.DataFrame(
            {
                "ListingId": [1],
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
                "TradeNotional": [1000.0],
                "TradeNotionalEUR": [1000.0],
            }
        ).write_parquet(self.trades_file)

        pl.DataFrame(
            {
                "ListingId": [1, 1],
                "TimestampNanoseconds": [
                    pd.Timestamp("2025-01-01 10:00:00").value,
                    pd.Timestamp("2025-01-01 10:05:00").value,
                ],
                "MarketState": ["CONTINUOUS_TRADING", "CLOSED"],
            }
        ).write_parquet(self.marketstate_file)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        shutil.rmtree(self.source_dir)

    @patch(
        "basalt.orchestrator.ProcessInterval", side_effect=SyncProcessInterval
    )
    @patch(
        "basalt.orchestrator.as_completed", side_effect=lambda futures: futures
    )
    @patch("basalt.orchestrator.get_files_for_date_range")
    @patch("basalt.orchestrator.ProcessPoolExecutor")
    def test_run_pipeline(
        self,
        mock_process_pool_executor,
        mock_get_files,
        mock_as_completed,
        mock_process_interval,
    ):
        def mock_files(sd, ed, mics, table_name, **kwargs):
            if table_name == "trades":
                return [self.trades_file]
            if table_name == "marketstate":
                return [self.marketstate_file]
            return []

        mock_get_files.side_effect = mock_files

        def mock_submit(fn, *args, **kwargs):
            fn(*args, **kwargs)
            mock_future = MagicMock()
            mock_future.result.return_value = True
            return mock_future

        mock_process_pool_executor.return_value.__enter__.return_value.submit.side_effect = (
            mock_submit
        )

        run_metrics_pipeline(config=self.config, get_universe=mock_get_universe)

        expected_out = os.path.join(
            self.temp_dir,
            "final_sample2d_pass1_2025-01-01_2025-01-01.parquet",
        )
        self.assertTrue(os.path.exists(expected_out))

        df = pl.read_parquet(expected_out)
        self.assertGreater(len(df), 0)
        self.assertEqual(df["ListingId"].dtype, pl.Int64)
        self.assertIn(1, df["ListingId"].to_list())


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    unittest.main()
