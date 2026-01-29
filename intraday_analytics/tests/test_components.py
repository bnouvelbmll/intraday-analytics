import unittest
import pandas as pd
import polars as pl
import polars.testing
from unittest.mock import MagicMock, patch
import datetime as dt
import tempfile
import shutil
import os

from intraday_analytics.utils import create_date_batches, ffill_with_shifts
from intraday_analytics.batching import HeuristicBatchingStrategy, SymbolSizeEstimator, S3SymbolBatcher
from intraday_analytics.metrics.l2 import L2AnalyticsLast, L2AnalyticsConfig

class TestUtils(unittest.TestCase):
    def test_create_date_batches_auto_short(self):
        # Test short range (<= 7 days) -> single batch
        batches = create_date_batches("2025-01-01", "2025-01-05")
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0][0], pd.Timestamp("2025-01-01"))
        self.assertEqual(batches[0][1], pd.Timestamp("2025-01-05"))

    def test_create_date_batches_auto_medium(self):
        # Test medium range (8-60 days) -> weekly batches
        # 2025-01-01 is a Wednesday.
        # Logic aligns to week start.
        batches = create_date_batches("2025-01-01", "2025-01-20")
        # Just check we get multiple batches and they cover the range
        self.assertTrue(len(batches) > 1)
        self.assertEqual(batches[-1][1], pd.Timestamp("2025-01-20"))

    def test_ffill_with_shifts(self):
        # Create a simple dataframe
        df = pl.DataFrame({
            "group": ["A", "A"],
            "time": [10, 20],
            "val": [1, 2]
        })
        
        # Shift by +1 and -1
        # Original: 10, 20
        # +1: 11, 21
        # -1: 9, 19
        # Total unique times: 9, 10, 11, 19, 20, 21
        
        res = ffill_with_shifts(
            df.lazy(),
            group_cols=["group"],
            time_col="time",
            value_cols=["val"],
            shifts=[1, -1]
        ).collect().sort("time")
        
        # Check expected length
        self.assertEqual(len(res), 6)
        
        # Check forward fill logic
        # At time 11, it should carry forward value from 10 (val=1)
        row_11 = res.filter(pl.col("time") == 11)
        self.assertEqual(row_11["val"][0], 1)
        
        # At time 21, it should carry forward value from 20 (val=2)
        row_21 = res.filter(pl.col("time") == 21)
        self.assertEqual(row_21["val"][0], 2)

class TestBatching(unittest.TestCase):
    def test_heuristic_batching(self):
        # Mock estimator
        estimator = MagicMock()
        # Return estimates for 3 symbols
        # SymA: 100 rows
        # SymB: 500 rows
        # SymC: 600 rows
        estimator.get_estimates_for_symbols.return_value = {
            "table1": {"SymA": 100, "SymB": 500, "SymC": 600}
        }
        
        # Max rows = 600
        # Batch 1: SymA (100) + SymB (500) = 600 (Fits)
        # Batch 2: SymC (600) (Fits in new batch)
        
        strategy = HeuristicBatchingStrategy(estimator, {"table1": 600})
        batches = strategy.create_batches(["SymA", "SymB", "SymC"])
        
        self.assertEqual(len(batches), 2)
        self.assertEqual(batches[0], ["SymA", "SymB"])
        self.assertEqual(batches[1], ["SymC"])

class TestL2Metrics(unittest.TestCase):
    def test_l2_last_calculation(self):
        # Create mock L2 data
        # Two updates in the same bucket for the same symbol
        l2_df = pl.DataFrame({
            "MIC": ["X", "X"], "ListingId": [1, 1], "Ticker": ["T", "T"], "CurrencyCode": ["EUR", "EUR"],
            "TimeBucket": [1, 1],
            "EventTimestamp": [10, 20],
            "BidPrice1": [10.0, 10.5],
            "AskPrice1": [11.0, 11.5],
            "BidQuantity1": [100, 200],
            "AskQuantity1": [100, 200],
            "BidNumOrders1": [1, 2],
            "AskNumOrders1": [1, 2],
            "MarketState": ["OPEN", "OPEN"]
        }).lazy()
        
        analytics = L2AnalyticsLast(L2AnalyticsConfig(levels=1))
        analytics.l2 = l2_df
        
        result = analytics.compute().collect()
        
        # Should have 1 row (aggregated by TimeBucket)
        self.assertEqual(len(result), 1)
        
        # Should take the LAST value (from timestamp 20)
        self.assertEqual(result["BidPrice1"][0], 10.5)
        self.assertEqual(result["AskPrice1"][0], 11.5)
        
        # Check Spread Calculation
        # Spread = 20000 * (Ask - Bid) / (Ask + Bid)
        # = 20000 * (11.5 - 10.5) / (11.5 + 10.5)
        # = 20000 * 1 / 22 = 909.09...
        expected_spread = 20000 * (11.5 - 10.5) / (11.5 + 10.5)
        self.assertAlmostEqual(result["SpreadBps"][0], expected_spread, places=4)

class TestS3SymbolBatcherIntegration(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.source_dir = tempfile.mkdtemp()
        
        # Create a dummy parquet file
        self.df = pl.DataFrame({
            "ListingId": ["A", "B"],
            "TradeTimestamp": [1, 2],
            "Price": [10.0, 20.0]
        })
        self.source_file = os.path.join(self.source_dir, "trades.parquet")
        self.df.write_parquet(self.source_file)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        shutil.rmtree(self.source_dir)

    def test_process_and_finalize(self):
        # Mock dependencies
        mock_estimator = MagicMock()
        mock_estimator.get_estimates_for_symbols.return_value = {"trades": {"A": 1, "B": 1}}
        
        # Strategy that puts A and B in separate batches
        # Max rows = 1 -> A in batch 0, B in batch 1
        strategy = HeuristicBatchingStrategy(mock_estimator, {"trades": 1})
        
        def mock_get_universe(date):
            return pl.DataFrame({"ListingId": ["A", "B"], "mic": ["X", "X"]})

        batcher = S3SymbolBatcher(
            s3_file_lists={"trades": [self.source_file]},
            transform_fns={"trades": lambda x: x},
            batching_strategy=strategy,
            temp_dir=self.temp_dir,
            storage_options={},
            date="2025-01-01",
            get_universe=mock_get_universe
        )
        
        # Run process
        batcher.process(num_workers=1)
        
        # Check if output files exist
        # We expect batch-trades-0.parquet and batch-trades-1.parquet
        out_0 = os.path.join(self.temp_dir, "batch-trades-0.parquet")
        out_1 = os.path.join(self.temp_dir, "batch-trades-1.parquet")
        
        if not os.path.exists(out_0):
            print(f"Listing {self.temp_dir}:")
            for root, dirs, files in os.walk(self.temp_dir):
                print(root, dirs, files)

        self.assertTrue(os.path.exists(out_0), "Batch 0 output missing")
        self.assertTrue(os.path.exists(out_1), "Batch 1 output missing")
        
        # Verify content
        df0 = pl.read_parquet(out_0)
        self.assertEqual(df0["ListingId"][0], "A")
        
        df1 = pl.read_parquet(out_1)
        self.assertEqual(df1["ListingId"][0], "B")

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    unittest.main()