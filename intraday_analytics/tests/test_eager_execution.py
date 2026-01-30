import unittest
import polars as pl
import pandas as pd
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch
from intraday_analytics.execution import run_metrics_pipeline, ProcessInterval
from intraday_analytics.configuration import AnalyticsConfig, PrepareDataMode
from intraday_analytics.pipeline import AnalyticsPipeline
from intraday_analytics.metrics.trade import TradeAnalytics

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


class TestEagerExecution(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import sys

        if "bmll2" not in sys.modules:
            sys.modules["bmll2"] = MagicMock()

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

        # Create dummy source files
        self.source_dir = tempfile.mkdtemp()
        self.trades_file = os.path.join(self.source_dir, "trades.parquet")
        pl.DataFrame(
            {
                "ListingId": ["A"],
                "TradeTimestamp": [pd.Timestamp("2025-01-01 10:00:00").value],
                "Price": [10.0],
                "Size": [100],
                "TradeNotional": [1000.0],
                "TradeNotionalEUR": [1000.0],
                "LPrice": [10.0],
                "EPrice": [10.0],
                "PricePoint": [0.0],
                "MIC": ["X"],
                "Ticker": ["T"],
                "CurrencyCode": ["EUR"],
                "Classification": ["LIT_CONTINUOUS"],
                "BMLLTradeType": ["LIT"],
                "BMLLParticipantType": ["RETAIL"],
                "AggressorSide": [1],
                "MarketState": ["OPEN"],
            }
        ).with_columns(pl.col("TradeTimestamp").cast(pl.Datetime("ns"))).write_parquet(
            self.trades_file
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        shutil.rmtree(self.source_dir)

    def _run_pipeline_with_config(self, eager_execution: bool):
        config = AnalyticsConfig(
            START_DATE="2025-01-01",
            END_DATE="2025-01-02",
            TEMP_DIR=self.temp_dir,
            PREPARE_DATA_MODE=PrepareDataMode.S3_SHREDDING.value,
            BATCHING_STRATEGY="heuristic",
            MAX_ROWS_PER_TABLE={"trades": 1000},
            CLEAN_UP_TEMP_DIR=False,
            FINAL_OUTPUT_PATH_TEMPLATE=os.path.join(
                self.temp_dir,
                f"final_{eager_execution}_{{start_date}}_{{end_date}}.parquet",
            ),
            TABLES_TO_LOAD=["trades"],
            EAGER_EXECUTION=eager_execution,
            BATCH_FREQ=None,
            OVERWRITE_TEMP_DIR=True
        )

        with patch(
            "intraday_analytics.batching.pl.scan_parquet"
        ) as mock_scan_parquet, patch(
            "intraday_analytics.execution.ProcessInterval",
            side_effect=SyncProcessInterval,
        ), patch(
            "intraday_analytics.execution.get_files_for_date_range"
        ) as mock_get_files, patch(
            "intraday_analytics.batching.SymbolSizeEstimator"
        ) as mock_estimator_cls, patch(
            "bmll2.storage_paths"
        ) as mock_storage_paths:

            # Mock scan_parquet to strip storage_options
            def safe_scan_parquet(*args, **kwargs):
                kwargs.pop("storage_options", None)
                return REAL_SCAN_PARQUET(*args, **kwargs)

            mock_scan_parquet.side_effect = safe_scan_parquet

            # Mock S3 file listing to return our local file
            mock_get_files.return_value = [self.trades_file]

            # Mock Estimator
            mock_estimator = MagicMock()
            mock_estimator.get_estimates_for_symbols.return_value = {"trades": {"A": 1}}
            mock_estimator_cls.return_value = mock_estimator

            # Mock bmll2 storage paths
            mock_storage_paths.return_value = {"user": {"bucket": "b", "prefix": "p"}}

            # Use module-level mock_get_universe

            # Use module-level MockPipelineFactory
            mock_get_pipeline = MockPipelineFactory(config)

            run_metrics_pipeline(config, mock_get_pipeline, mock_get_universe)

            expected_out = os.path.join(
                self.temp_dir, f"final_{eager_execution}_2025-01-01_2025-01-02.parquet"
            )
            self.assertTrue(
                os.path.exists(expected_out),
                f"Final output file not found for eager={eager_execution}",
            )

            df = pl.read_parquet(expected_out)
            return df

    def test_lazy_execution(self):
        df = self._run_pipeline_with_config(eager_execution=False)
        self.assertEqual(len(df), 1)
        self.assertEqual(df["ListingId"][0], "A")

    def test_eager_execution(self):
        df = self._run_pipeline_with_config(eager_execution=True)
        self.assertEqual(len(df), 1)
        self.assertEqual(df["ListingId"][0], "A")


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    unittest.main()
