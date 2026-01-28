import unittest
import pandas as pd
import polars as pl
import polars.testing
from unittest.mock import MagicMock, patch
import datetime as dt

from intraday_analytics.utils import create_date_batches, ffill_with_shifts
from intraday_analytics.batching import HeuristicBatchingStrategy, SymbolSizeEstimator
from intraday_analytics.metrics.l2 import L2AnalyticsLast

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
        
        analytics = L2AnalyticsLast(N=1)
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

if __name__ == '__main__':
    unittest.main()
