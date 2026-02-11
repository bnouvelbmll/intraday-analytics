import unittest
import datetime as dt
import polars as pl

from basalt.analytics.l2 import (
    L2AnalyticsLast,
    L2AnalyticsConfig,
    L2OHLCConfig,
)


class TestL2Ohlc(unittest.TestCase):
    def _base_df(self):
        bucket1 = dt.datetime(2025, 1, 1, 0, 1, 0)
        bucket2 = dt.datetime(2025, 1, 1, 0, 2, 0)
        bucket3 = dt.datetime(2025, 1, 1, 0, 3, 0)

        df = pl.DataFrame(
            {
                "MIC": ["X", "X", "X"],
                "ListingId": [1, 1, 1],
                "Ticker": ["T", "T", "T"],
                "CurrencyCode": ["EUR", "EUR", "EUR"],
                "TimeBucket": [bucket1, bucket1, bucket3],
                "EventTimestamp": [1, 2, 3],
                "BidPrice1": [10.0, 10.5, 11.0],
                "AskPrice1": [11.0, 11.5, 12.0],
                "BidQuantity1": [100, 200, 100],
                "AskQuantity1": [200, 100, 200],
                "BidNumOrders1": [1, 2, 1],
                "AskNumOrders1": [1, 2, 1],
                "MarketState": ["OPEN", "OPEN", "OPEN"],
            }
        ).lazy()

        return df, bucket1, bucket2, bucket3

    def test_weighted_mid_ohlc_event(self):
        df, bucket1, _, bucket3 = self._base_df()
        # Use only first two rows (bucket1) and third row for bucket3
        analytics = L2AnalyticsLast(
            L2AnalyticsConfig(
                time_bucket_seconds=60,
                ohlc=[L2OHLCConfig(source="WeightedMid", open_mode="event")],
            )
        )
        analytics.l2 = df
        result = analytics.compute().collect().sort("TimeBucket")

        # Expected WeightedMid for bucket1 rows
        wm1 = (10.0 * 200 + 11.0 * 100) / (200 + 100)
        wm2 = (10.5 * 100 + 11.5 * 200) / (100 + 200)

        # Bucket1 OHLC
        row1 = result.filter(pl.col("TimeBucket") == bucket1).row(0)
        col_index = {c: i for i, c in enumerate(result.columns)}
        self.assertAlmostEqual(row1[col_index["WeightedMidOpen"]], wm1, places=6)
        self.assertAlmostEqual(
            row1[col_index["WeightedMidHigh"]], max(wm1, wm2), places=6
        )
        self.assertAlmostEqual(
            row1[col_index["WeightedMidLow"]], min(wm1, wm2), places=6
        )
        self.assertAlmostEqual(row1[col_index["WeightedMidClose"]], wm2, places=6)

        # Bucket3 single row
        wm3 = (11.0 * 200 + 12.0 * 100) / (200 + 100)
        row3 = result.filter(pl.col("TimeBucket") == bucket3).row(0)
        self.assertAlmostEqual(row3[col_index["WeightedMidOpen"]], wm3, places=6)
        self.assertAlmostEqual(row3[col_index["WeightedMidHigh"]], wm3, places=6)
        self.assertAlmostEqual(row3[col_index["WeightedMidLow"]], wm3, places=6)
        self.assertAlmostEqual(row3[col_index["WeightedMidClose"]], wm3, places=6)

    def test_ohlc_prev_close_open(self):
        df, bucket1, bucket2, bucket3 = self._base_df()
        analytics = L2AnalyticsLast(
            L2AnalyticsConfig(
                time_bucket_seconds=60,
                ohlc=[L2OHLCConfig(source="Mid", open_mode="prev_close")],
            )
        )
        analytics.l2 = df
        result = analytics.compute().collect().sort("TimeBucket")

        # Bucket2 should exist and be filled from bucket1 close
        mid1 = (10.0 + 11.0) / 2
        mid2 = (10.5 + 11.5) / 2
        expected_close_bucket1 = mid2

        row2 = result.filter(pl.col("TimeBucket") == bucket2).row(0)
        col_index = {c: i for i, c in enumerate(result.columns)}
        self.assertAlmostEqual(
            row2[col_index["MidOpen"]], expected_close_bucket1, places=6
        )
        self.assertAlmostEqual(
            row2[col_index["MidHigh"]], expected_close_bucket1, places=6
        )
        self.assertAlmostEqual(
            row2[col_index["MidLow"]], expected_close_bucket1, places=6
        )
        self.assertAlmostEqual(
            row2[col_index["MidClose"]], expected_close_bucket1, places=6
        )

    def test_metric_prefix_applies_to_ohlc(self):
        df, _, _, _ = self._base_df()
        analytics = L2AnalyticsLast(
            L2AnalyticsConfig(
                time_bucket_seconds=60,
                metric_prefix="P_",
                ohlc=[L2OHLCConfig(source="Mid", open_mode="event")],
            )
        )
        analytics.l2 = df
        result = analytics.compute().collect()

        self.assertIn("P_MidOpen", result.columns)
        self.assertIn("P_MidClose", result.columns)
        self.assertIn("P_MarketState", result.columns)
        self.assertNotIn("MidOpen", result.columns)
        self.assertNotIn("MarketState", result.columns)
