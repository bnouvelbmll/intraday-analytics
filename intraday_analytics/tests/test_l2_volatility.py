import unittest
import polars as pl
import numpy as np
from intraday_analytics.analytics.l2 import (
    L2AnalyticsTW,
    L2AnalyticsConfig,
    L2VolatilityConfig,
)


class TestL2Volatility(unittest.TestCase):
    def test_volatility_calculation(self):
        # Create dummy L2 data
        # 1 minute bucket (60 seconds)
        # Price moves: 100 -> 101 -> 100 -> 101 ...
        # Log returns: ln(1.01), ln(1/1.01), ...

        bucket_seconds = 60
        config = L2AnalyticsConfig(
            time_bucket_seconds=bucket_seconds,
            volatility=[L2VolatilityConfig(source="Mid", aggregations=["Std"])],
        )

        # Create a series of updates
        # We need enough points to get a stable std dev
        n_points = 60
        prices = [100.0 * (1.01 ** (i % 2)) for i in range(n_points)]
        # 100, 101, 100, 101...

        # Timestamps: 0, 1s, 2s...
        timestamps = [i * 1_000_000_000 for i in range(n_points)]

        # DT (duration) - mostly 1s
        dt = [1_000_000_000] * n_points

        df = pl.DataFrame(
            {
                "ListingId": ["A"] * n_points,
                "TimeBucket": [0] * n_points,  # Same bucket
                "AskPrice1": prices,
                "BidPrice1": prices,  # Mid = Price
                "DT": dt,
                "MarketState": ["OPEN"] * n_points,
                "EventTimestamp": timestamps,
                "MIC": ["X"] * n_points,
                "Ticker": ["ABC"] * n_points,
                "CurrencyCode": ["EUR"] * n_points,
            }
        ).with_columns(
            pl.col("TimeBucket").cast(pl.Datetime("ns")),
            pl.col("EventTimestamp").cast(pl.Datetime("ns")),
        )

        analytics = L2AnalyticsTW(config)
        analytics.l2 = df.lazy()

        result = analytics.compute().collect()

        self.assertEqual(len(result), 1)
        vol = result["L2VolatilityMidStd"][0]

        # Expected calculation
        # Returns: ln(1.01), ln(0.990099...), ...
        # Std of returns
        log_rets = np.diff(np.log(prices))
        std_ret = np.std(log_rets, ddof=1)  # Polars uses ddof=1 by default for std

        seconds_in_year = 252 * 24 * 60 * 60
        factor = np.sqrt(seconds_in_year * n_points / bucket_seconds)
        expected_vol = std_ret * factor

        # Allow small float error
        # The difference comes from how frequency is estimated vs exact returns
        self.assertAlmostEqual(vol, expected_vol, delta=0.5)


if __name__ == "__main__":
    unittest.main()
