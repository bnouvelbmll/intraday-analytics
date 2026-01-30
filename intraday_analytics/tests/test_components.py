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
from intraday_analytics.batching import (
    HeuristicBatchingStrategy,
    SymbolSizeEstimator,
    S3SymbolBatcher,
)
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
        df = pl.DataFrame({"group": ["A", "A"], "time": [10, 20], "val": [1, 2]})

        # Shift by +1 and -1
        # Original: 10, 20
        # +1: 11, 21
        # -1: 9, 19
        # Total unique times: 9, 10, 11, 19, 20, 21

        res = (
            ffill_with_shifts(
                df.lazy(),
                group_cols=["group"],
                time_col="time",
                value_cols=["val"],
                shifts=[1, -1],
            )
            .collect()
            .sort("time")
        )

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
        l2_df = pl.DataFrame(
            {
                "MIC": ["X", "X"],
                "ListingId": [1, 1],
                "Ticker": ["T", "T"],
                "CurrencyCode": ["EUR", "EUR"],
                "TimeBucket": [1, 1],
                "EventTimestamp": [10, 20],
                "BidPrice1": [10.0, 10.5],
                "AskPrice1": [11.0, 11.5],
                "BidQuantity1": [100, 200],
                "AskQuantity1": [100, 200],
                "BidNumOrders1": [1, 2],
                "AskNumOrders1": [1, 2],
                "MarketState": ["OPEN", "OPEN"],
            }
        ).lazy()

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
        self.df = pl.DataFrame(
            {"ListingId": ["A", "B"], "TradeTimestamp": [1, 2], "Price": [10.0, 20.0]}
        )
        self.source_file = os.path.join(self.source_dir, "trades.parquet")
        self.df.write_parquet(self.source_file)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        shutil.rmtree(self.source_dir)

    def test_process_and_finalize(self):
        # Mock dependencies
        mock_estimator = MagicMock()
        mock_estimator.get_estimates_for_symbols.return_value = {
            "trades": {"A": 1, "B": 1}
        }

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
            get_universe=mock_get_universe,
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


class TestAnalyticsModuleOutputs(unittest.TestCase):
    def setUp(self):
        """Set up mock DataFrames for testing."""
        base_time = dt.datetime(2025, 1, 1, 9, 30)

        # Mock L2 data
        l2_data = {
            "TimeBucket": [base_time, base_time, base_time + dt.timedelta(minutes=1)],
            "ListingId": [101, 102, 101],
            "EventTimestamp": [
                base_time + dt.timedelta(seconds=10),
                base_time + dt.timedelta(seconds=11),
                base_time + dt.timedelta(minutes=1, seconds=20),
            ],
            "MIC": ["XNYS", "XNYS", "XNYS"],
            "Ticker": ["TICKA", "TICKB", "TICKA"],
            "CurrencyCode": ["USD", "USD", "USD"],
            "MarketState": [
                "CONTINUOUS_TRADING",
                "CONTINUOUS_TRADING",
                "CONTINUOUS_TRADING",
            ],
        }
        for i in range(1, 11):
            l2_data[f"BidPrice{i}"] = [100.0 - i, 101.0 - i, 100.5 - i]
            l2_data[f"AskPrice{i}"] = [100.0 + i, 101.0 + i, 100.5 + i]
            l2_data[f"BidQuantity{i}"] = [100, 200, 150]
            l2_data[f"AskQuantity{i}"] = [100, 200, 150]
            l2_data[f"BidNumOrders{i}"] = [1, 2, 1]
            l2_data[f"AskNumOrders{i}"] = [1, 2, 1]
        self.l2_df = pl.DataFrame(l2_data).lazy()

        # Mock trades data
        self.trades_df = pl.DataFrame(
            {
                "TimeBucket": [
                    base_time,
                    base_time,
                    base_time + dt.timedelta(minutes=1),
                ],
                "ListingId": [101, 102, 101],
                "Price": [100.0, 101.0, 100.5],
                "Quantity": [10, 20, 15],
                "TradeDate": [
                    dt.date(2025, 1, 1),
                    dt.date(2025, 1, 1),
                    dt.date(2025, 1, 1),
                ],
                "Classification": [
                    "LIT_CONTINIOUS",
                    "LIT_CONTINIOUS",
                    "LIT_CONTINIOUS",
                ],
                "LPrice": [100.0, 101.0, 100.5],
                "Size": [10, 20, 15],
                "MIC": ["XNYS", "XNYS", "XNYS"],
                "Ticker": ["TICKA", "TICKB", "TICKA"],
                "PricePoint": [1, 1, 1],
                "MarketState": [
                    "CONTINUOUS_TRADING",
                    "CONTINUOUS_TRADING",
                    "CONTINUOUS_TRADING",
                ],
                "BMLLParticipantType": ["X", "Y", "X"],
                "TradeNotionalEUR": [1000.0, 2020.0, 1507.5],
                "AggressorSide": [1, 2, 1],
                "BMLLTradeType": ["LIT", "LIT", "LIT"],
            }
        ).lazy()

        # Mock L3 data
        self.l3_df = pl.DataFrame(
            {
                "TimeBucket": [
                    base_time,
                    base_time,
                    base_time + dt.timedelta(minutes=1),
                ],
                "ListingId": [101, 102, 101],
                "Side": [1, 2, 1],  # 1 for Bid, 2 for Ask
                "LobAction": [2, 3, 4],  # 2:Insert, 3:Remove, 4:Update
                "Size": [100, 50, 120],
                "OldSize": [0, 50, 100],
                "ExecutionSize": [0, 0, 0],
                "ExecutionPrice": [0.0, 0.0, 0.0],
            }
        ).lazy()

        # Mock market state data
        self.marketstate_df = pl.DataFrame(
            {
                "ListingId": [101, 101, 101, 102, 102, 102],
                "EventTimestamp": [
                    base_time,
                    base_time + dt.timedelta(seconds=5),
                    base_time + dt.timedelta(hours=1),
                    base_time,
                    base_time + dt.timedelta(seconds=5),
                    base_time + dt.timedelta(hours=1),
                ],
                "MarketState": [
                    "OPEN",
                    "CONTINUOUS_TRADING",
                    "CLOSED",
                    "OPEN",
                    "CONTINUOUS_TRADING",
                    "CLOSED",
                ],
            }
        ).lazy()

    def test_module_output_columns(self):
        """Verify that each analytics module includes TimeBucket and ListingId."""
        from intraday_analytics.metrics.dense import (
            DenseAnalytics,
            DenseAnalyticsConfig,
        )
        from intraday_analytics.metrics.l2 import (
            L2AnalyticsLast,
            L2AnalyticsTW,
            L2AnalyticsConfig,
        )
        from intraday_analytics.metrics.trade import (
            TradeAnalytics,
            TradeAnalyticsConfig,
        )
        from intraday_analytics.metrics.l3 import L3Analytics, L3AnalyticsConfig
        from intraday_analytics.metrics.execution import (
            ExecutionAnalytics,
            ExecutionAnalyticsConfig,
        )

        mock_ref = pl.DataFrame({"ListingId": [101, 102]})

        modules = {
            "Dense": DenseAnalytics(
                mock_ref, DenseAnalyticsConfig(time_bucket_seconds=60)
            ),
            "L2Last": L2AnalyticsLast(L2AnalyticsConfig(levels=10)),
            "L2TW": L2AnalyticsTW(L2AnalyticsConfig(time_bucket_seconds=60, levels=10)),
            "Trade": TradeAnalytics(TradeAnalyticsConfig()),
            "L3": L3Analytics(L3AnalyticsConfig()),
            "Execution": ExecutionAnalytics(ExecutionAnalyticsConfig()),
        }

        for name, module in modules.items():
            with self.subTest(name=name):
                # Assign the specific mock data needed by each module
                if hasattr(module, "REQUIRES"):
                    if "l2" in module.REQUIRES:
                        module.l2 = self.l2_df
                    if "trades" in module.REQUIRES:
                        module.trades = self.trades_df
                    if "l3" in module.REQUIRES:
                        module.l3 = self.l3_df

                # DenseAnalytics also uses marketstate
                if name == "Dense":
                    module.marketstate = self.marketstate_df

                # ExecutionAnalytics also uses l2
                if name == "Execution":
                    module.l2 = self.l2_df
                    module.trades = self.trades_df

                result_df = module.compute().collect()

                self.assertIn("TimeBucket", result_df.columns)
                self.assertIn("ListingId", result_df.columns)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    unittest.main()
