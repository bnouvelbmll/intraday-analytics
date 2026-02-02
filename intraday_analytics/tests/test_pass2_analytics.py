import unittest
import polars as pl
import pandas as pd
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch

from intraday_analytics.configuration import AnalyticsConfig, PassConfig
from intraday_analytics.execution import run_metrics_pipeline, ProcessInterval
from intraday_analytics.pipeline import AnalyticsPipeline
from intraday_analytics.analytics.trade import TradeAnalytics, TradeGenericConfig
from intraday_analytics.analytics.generic import GenericAnalytics, TalibIndicatorConfig

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
    return pl.DataFrame({
        "ListingId": ["A", "B"], 
        "MIC": ["X", "X"],
        "InstrumentId": [1, 1], # Both listings belong to same instrument
        "Ticker": ["ABC", "DEF"]
    })

class TestPass2Analytics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import sys
        if "bmll2" not in sys.modules:
            sys.modules["bmll2"] = MagicMock()

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.source_dir = tempfile.mkdtemp()
        self.trades_file = os.path.join(self.source_dir, "trades.parquet")
        
        # Create dummy trades for 2 listings of same instrument
        # Listing A: Price 10, 11, 12
        # Listing B: Price 20, 21, 22
        # Timestamps: 10:00, 10:01, 10:02
        ts_base = pd.Timestamp("2025-01-01 10:00:00")
        
        pl.DataFrame({
            "ListingId": ["A", "A", "A", "B", "B", "B"],
            "TradeTimestamp": [
                ts_base.value, (ts_base + pd.Timedelta(minutes=1)).value, (ts_base + pd.Timedelta(minutes=2)).value,
                ts_base.value, (ts_base + pd.Timedelta(minutes=1)).value, (ts_base + pd.Timedelta(minutes=2)).value
            ],
            "Price": [10.0, 11.0, 12.0, 20.0, 21.0, 22.0],
            "Size": [100, 100, 100, 100, 100, 100],
            "Classification": ["LIT_CONTINUOUS"] * 6,
            "LocalPrice": [10.0, 11.0, 12.0, 20.0, 21.0, 22.0],
            "MarketState": ["OPEN"] * 6,
            "Ticker": ["ABC", "ABC", "ABC", "DEF", "DEF", "DEF"],
            "MIC": ["X"] * 6,
            "PricePoint": [0.5] * 6,
            "BMLLParticipantType": ["RETAIL"] * 6,
            "AggressorSide": [1] * 6,
            "TradeNotionalEUR": [1000.0, 1100.0, 1200.0, 2000.0, 2100.0, 2200.0],
        }).write_parquet(self.trades_file)

        self.config = AnalyticsConfig(
            START_DATE="2025-01-01",
            END_DATE="2025-01-01",
            TEMP_DIR=self.temp_dir,
            TABLES_TO_LOAD=["trades"],
            CLEAN_UP_TEMP_DIR=False,
            BATCH_FREQ=None,
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        shutil.rmtree(self.source_dir)

    @patch("intraday_analytics.execution.get_final_s3_path")
    @patch("intraday_analytics.process.get_final_s3_path")
    @patch("intraday_analytics.execution.ProcessInterval", side_effect=SyncProcessInterval)
    @patch("intraday_analytics.execution.get_files_for_date_range")
    def test_pass2_aggregation_by_instrument(self, mock_get_files, mock_process_interval, mock_get_s3_path_process, mock_get_s3_path_exec):
        mock_get_files.return_value = [self.trades_file]
        
        # Mock S3 path to be local
        def side_effect_s3_path(sd, ed, config, pass_name):
            return os.path.join(self.temp_dir, f"final_{config.DATASETNAME}_{pass_name}_{sd.date()}_{ed.date()}.parquet")
        mock_get_s3_path_process.side_effect = side_effect_s3_path
        mock_get_s3_path_exec.side_effect = side_effect_s3_path

        # Pass 1: Trade Analytics (1 min buckets)
        # Pass 2: Aggregate by InstrumentId
        
        self.config.PASSES = [
            PassConfig(name="pass1", modules=["trade"], time_bucket_seconds=60),
            PassConfig(name="pass2", modules=["generic"], time_bucket_seconds=60)
        ]
        
        # Configure Pass 1 to output Volume and NotionalEUR
        self.config.PASSES[0].trade_analytics.generic_metrics = [
            TradeGenericConfig(measures=["Volume", "NotionalEUR"], sides="Total")
        ]
        
        self.config.PASSES[1].generic_analytics.source_pass = "pass1"
        self.config.PASSES[1].generic_analytics.group_by = ["InstrumentId", "TimeBucket"]
        self.config.PASSES[1].generic_analytics.aggregations = {
            "TradeTotalVolume": "sum",
            "TradeTotalNotionalEUR": "sum"
        }

        def get_pipeline(pass_config, context, symbols=None, ref=None, date=None):
            modules = []
            if "trade" in pass_config.modules:
                modules.append(TradeAnalytics(pass_config.trade_analytics))
            if "generic" in pass_config.modules:
                 ga = GenericAnalytics(pass_config.generic_analytics)
                 ga.ref = ref
                 modules.append(ga)
            return AnalyticsPipeline(modules, self.config, pass_config, context)

        run_metrics_pipeline(self.config, get_pipeline, mock_get_universe)

        expected_out = os.path.join(
            self.temp_dir, "final_sample2d_pass2_2025-01-01_2025-01-01.parquet"
        )
        self.assertTrue(os.path.exists(expected_out))
        
        df = pl.read_parquet(expected_out)
        # Should have 3 rows (one per minute), InstrumentId=1
        self.assertEqual(len(df), 3)
        self.assertEqual(df["InstrumentId"][0], 1)
        # Volume should be 200 (100 from A + 100 from B)
        self.assertEqual(df["TradeTotalVolume"][0], 200)

    @patch("intraday_analytics.execution.get_final_s3_path")
    @patch("intraday_analytics.process.get_final_s3_path")
    @patch("intraday_analytics.execution.ProcessInterval", side_effect=SyncProcessInterval)
    @patch("intraday_analytics.execution.get_files_for_date_range")
    def test_pass2_resampling(self, mock_get_files, mock_process_interval, mock_get_s3_path_process, mock_get_s3_path_exec):
        mock_get_files.return_value = [self.trades_file]

        # Mock S3 path to be local
        def side_effect_s3_path(sd, ed, config, pass_name):
            return os.path.join(self.temp_dir, f"final_{config.DATASETNAME}_{pass_name}_{sd.date()}_{ed.date()}.parquet")
        mock_get_s3_path_process.side_effect = side_effect_s3_path
        mock_get_s3_path_exec.side_effect = side_effect_s3_path

        self.config.PASSES = [
            PassConfig(name="pass1", modules=["trade"], time_bucket_seconds=60),
            PassConfig(name="pass2", modules=["generic"], time_bucket_seconds=60)
        ]
        
        # Configure Pass 1 to output Volume
        self.config.PASSES[0].trade_analytics.generic_metrics = [
            TradeGenericConfig(measures=["Volume"], sides="Total")
        ]
        
        self.config.PASSES[1].generic_analytics.source_pass = "pass1"
        self.config.PASSES[1].generic_analytics.group_by = ["ListingId", "TimeBucket"]
        self.config.PASSES[1].generic_analytics.resample_rule = "15m"
        self.config.PASSES[1].generic_analytics.aggregations = {
            "TradeTotalVolume": "sum"
        }

        def get_pipeline(pass_config, context, symbols=None, ref=None, date=None):
            modules = []
            if "trade" in pass_config.modules:
                modules.append(TradeAnalytics(pass_config.trade_analytics))
            if "generic" in pass_config.modules:
                 ga = GenericAnalytics(pass_config.generic_analytics)
                 ga.ref = ref
                 modules.append(ga)
            return AnalyticsPipeline(modules, self.config, pass_config, context)

        run_metrics_pipeline(self.config, get_pipeline, mock_get_universe)

        expected_out = os.path.join(
            self.temp_dir, "final_sample2d_pass2_2025-01-01_2025-01-01.parquet"
        )
        self.assertTrue(os.path.exists(expected_out))
        
        df = pl.read_parquet(expected_out)
        # Should have 2 rows (one for A, one for B), all in same 15m bucket
        self.assertEqual(len(df), 2)
        # Volume should be 300 (100 * 3 minutes)
        self.assertEqual(df["TradeTotalVolume"][0], 300)

    @patch("intraday_analytics.execution.get_final_s3_path")
    @patch("intraday_analytics.process.get_final_s3_path")
    @patch("intraday_analytics.execution.ProcessInterval", side_effect=SyncProcessInterval)
    @patch("intraday_analytics.execution.get_files_for_date_range")
    def test_pass2_talib(self, mock_get_files, mock_process_interval, mock_get_s3_path_process, mock_get_s3_path_exec):
        mock_get_files.return_value = [self.trades_file]

        # Mock S3 path to be local
        def side_effect_s3_path(sd, ed, config, pass_name):
            return os.path.join(self.temp_dir, f"final_{config.DATASETNAME}_{pass_name}_{sd.date()}_{ed.date()}.parquet")
        mock_get_s3_path_process.side_effect = side_effect_s3_path
        mock_get_s3_path_exec.side_effect = side_effect_s3_path

        self.config.PASSES = [
            PassConfig(name="pass1", modules=["trade"], time_bucket_seconds=60),
            PassConfig(name="pass2", modules=["generic"], time_bucket_seconds=60)
        ]
        
        # Configure Pass 1 to output Close
        self.config.PASSES[0].trade_analytics.generic_metrics = [
            TradeGenericConfig(measures=["OHLC"], sides="Total")
        ]
        
        self.config.PASSES[1].generic_analytics.source_pass = "pass1"
        self.config.PASSES[1].generic_analytics.group_by = ["ListingId", "TimeBucket"]
        self.config.PASSES[1].generic_analytics.talib_indicators = [
            TalibIndicatorConfig(name="SMA", input_col="Close", timeperiod=2, output_col="SMA_2")
        ]

        def get_pipeline(pass_config, context, symbols=None, ref=None, date=None):
            modules = []
            if "trade" in pass_config.modules:
                modules.append(TradeAnalytics(pass_config.trade_analytics))
            if "generic" in pass_config.modules:
                 ga = GenericAnalytics(pass_config.generic_analytics)
                 ga.ref = ref
                 modules.append(ga)
            return AnalyticsPipeline(modules, self.config, pass_config, context)

        run_metrics_pipeline(self.config, get_pipeline, mock_get_universe)

        expected_out = os.path.join(
            self.temp_dir, "final_sample2d_pass2_2025-01-01_2025-01-01.parquet"
        )
        self.assertTrue(os.path.exists(expected_out))
        
        df = pl.read_parquet(expected_out)
        # Check SMA
        # Listing A: Close 10, 11, 12. SMA(2): NaN, 10.5, 11.5
        df_a = df.filter(pl.col("ListingId") == "A").sort("TimeBucket")
        sma = df_a["SMA_2"].to_list()
        self.assertTrue(pd.isna(sma[0]))
        self.assertEqual(sma[1], 10.5)
        self.assertEqual(sma[2], 11.5)

if __name__ == "__main__":
    unittest.main()
