import unittest
import polars as pl
import pandas as pd
from basalt.analytics.trade import (
    TradeAnalytics,
    TradeAnalyticsConfig,
    TradeImpactConfig,
)


class TestTCAMetrics(unittest.TestCase):
    def test_tca_reference_price_config(self):
        # Setup data
        # Trade at 100.
        # PreTradeMid = 99.
        # MidPrice = 98 (Alternative).
        # PostTradeMid = 101.
        # Side = 1 (Ask/Buy).

        # Effective Spread (using PreTradeMid) = 2 * |100 - 99| = 2.
        # Effective Spread (using MidPrice) = 2 * |100 - 98| = 4.

        df = pl.DataFrame(
            {
                "ListingId": ["A"],
                "TimeBucket": [1],
                "Classification": ["LIT_CONTINUOUS"],
                "MarketState": ["OPEN"],
                "LocalPrice": [100.0],
                "AggressorSide": [1],  # Buy
                "PreTradeMid": [99.0],
                "MidPrice": [98.0],
                "PostTradeMid": [101.0],
                "MIC": ["X"],
                "Ticker": ["ABC"],
                "BMLLParticipantType": ["P"],
                "TradeNotionalEUR": [1000.0],
            }
        ).lazy()

        # Case 1: Default (PreTradeMid)
        config_default = TradeAnalyticsConfig(
            impact_metrics=[
                TradeImpactConfig(
                    variant=["EffectiveSpread"],
                    horizon="1s",
                    # reference_price_col defaults to "PreTradeMid"
                )
            ]
        )

        analytics = TradeAnalytics(config_default)
        analytics.trades = df
        result = analytics.compute().collect()

        self.assertEqual(result["EffectiveSpread1s"][0], 2.0)

        # Case 2: Custom (MidPrice)
        config_custom = TradeAnalyticsConfig(
            impact_metrics=[
                TradeImpactConfig(
                    variant=["EffectiveSpread"],
                    horizon="1s",
                    reference_price_col="MidPrice",
                )
            ]
        )

        analytics = TradeAnalytics(config_custom)
        analytics.trades = df
        result = analytics.compute().collect()

        self.assertEqual(result["EffectiveSpread1s"][0], 4.0)


if __name__ == "__main__":
    unittest.main()
