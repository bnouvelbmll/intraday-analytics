import unittest
import polars as pl
import pandas as pd
import logging
from intraday_analytics.metrics.dense import DenseAnalytics, DenseAnalyticsConfig
from intraday_analytics.utils import SYMBOL_COL


class TestDenseAnalytics(unittest.TestCase):
    def setUp(self):
        self.config = DenseAnalyticsConfig(
            mode="adaptative",
            time_bucket_seconds=60,
            time_interval=["09:00:00", "10:00:00"],
        )
        self.ref = pl.DataFrame(
            {
                SYMBOL_COL: [1, 2],
                "MIC": ["XAMS", "XPAR"],
                "Ticker": ["ABC", "DEF"],
                "CurrencyCode": ["EUR", "EUR"],
            }
        )

    def test_adaptive_mode_with_failures(self):
        # Listing 1 has valid continuous trading intervals
        # Listing 2 has NO continuous trading intervals (simulating failure/missing data)
        marketstate = pl.DataFrame(
            {
                SYMBOL_COL: [1, 1, 1, 2, 2],
                "EventTimestamp": [
                    pd.Timestamp("2025-01-01 08:55:00"),
                    pd.Timestamp("2025-01-01 09:00:00"),
                    pd.Timestamp("2025-01-01 17:30:00"),
                    pd.Timestamp("2025-01-01 08:00:00"),
                    pd.Timestamp("2025-01-01 18:00:00"),
                ],
                "MarketState": [
                    "PRE_OPEN",
                    "CONTINUOUS_TRADING",
                    "POST_CLOSE",
                    "PRE_OPEN",  # Never enters CONTINUOUS_TRADING
                    "POST_CLOSE",
                ],
            }
        ).lazy()

        da = DenseAnalytics(self.ref, self.config)
        da.marketstate = marketstate

        # We expect a warning log for Listing 2
        with self.assertLogs("intraday_analytics.metrics.dense", level="WARNING") as cm:
            result_lf = da.compute()
            result_df = result_lf.collect()

        # Verify log message
        self.assertTrue(
            any("Could not determine continuous intervals" in o for o in cm.output)
        )
        self.assertTrue(
            any("2" in o for o in cm.output)
        )  # Listing ID 2 should be in the log

        # Verify result contains data for Listing 1 but not Listing 2
        self.assertIn(1, result_df[SYMBOL_COL].to_list())
        self.assertNotIn(2, result_df[SYMBOL_COL].to_list())

        # Verify schema
        self.assertIn("TimeBucket", result_df.columns)
        self.assertIn(SYMBOL_COL, result_df.columns)

    def test_adaptive_mode_all_failures(self):
        # All listings fail
        marketstate = pl.DataFrame(
            {
                SYMBOL_COL: [1],
                "EventTimestamp": [pd.Timestamp("2025-01-01 09:00:00")],
                "MarketState": ["PRE_OPEN"],
            }
        ).lazy()

        da = DenseAnalytics(self.ref.filter(pl.col(SYMBOL_COL) == 1), self.config)
        da.marketstate = marketstate

        with self.assertLogs("intraday_analytics.metrics.dense", level="WARNING") as cm:
            result_lf = da.compute()
            result_df = result_lf.collect()

        self.assertTrue(
            any("Could not determine continuous intervals" in o for o in cm.output)
        )

        # Should return empty DataFrame with correct schema
        self.assertTrue(result_df.is_empty())
        self.assertIn("TimeBucket", result_df.columns)
        self.assertIn(SYMBOL_COL, result_df.columns)


if __name__ == "__main__":
    unittest.main()
