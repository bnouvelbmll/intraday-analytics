import polars as pl
import unittest
from intraday_analytics import BaseAnalytics
from intraday_analytics.analytics.l3 import (
    L3Analytics,
    L3AnalyticsConfig,
    L3MetricConfig,
    L3AdvancedConfig,
)
from datetime import datetime


class TestL3Analytics(unittest.TestCase):

    def setUp(self):
        # Common mock L3 data for testing
        self.mock_l3_data = pl.LazyFrame(
            {
                "ListingId": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                "TimeBucket": pl.Series(
                    [
                        datetime(2026, 1, 30, 9, 0, 0),
                        datetime(2026, 1, 30, 9, 0, 0),
                        datetime(2026, 1, 30, 9, 0, 0),
                        datetime(2026, 1, 30, 9, 0, 0),
                        datetime(2026, 1, 30, 9, 0, 0),
                        datetime(2026, 1, 30, 9, 0, 0),
                        datetime(2026, 1, 30, 9, 0, 0),
                        datetime(2026, 1, 30, 9, 0, 0),
                        datetime(2026, 1, 30, 9, 0, 0),
                        datetime(2026, 1, 30, 9, 0, 0),
                    ],
                    dtype=pl.Datetime("ns"),
                ),
                "LobAction": [
                    2,
                    1,
                    4,
                    3,
                    2,
                    3,
                    4,
                    1,
                    2,
                    3,
                ],  # Insert, Exec, Update, Remove, Insert, Remove, Update, Exec, Insert, Remove
                "Side": [1, 1, 1, 1, 2, 2, 2, 2, 1, 2],  # 1=Bid, 2=Ask
                "Size": [100, 70, 120, 50, 80, 30, 90, 60, 110, 40],
                "OldSize": [0, 0, 100, 50, 0, 30, 70, 0, 0, 0],
                "ExecutionSize": [0, 70, 0, 0, 0, 30, 0, 60, 0, 0],
                "OrderID": [101, 101, 103, 104, 201, 201, 203, 204, 105, 205],
                "EventTimestamp": pl.Series(
                    [
                        datetime(2026, 1, 30, 9, 0, 1, 0),
                        datetime(2026, 1, 30, 9, 0, 2, 0),
                        datetime(2026, 1, 30, 9, 0, 3, 0),
                        datetime(2026, 1, 30, 9, 0, 4, 0),
                        datetime(2026, 1, 30, 9, 0, 5, 0),
                        datetime(2026, 1, 30, 9, 0, 6, 0),
                        datetime(2026, 1, 30, 9, 0, 7, 0),
                        datetime(2026, 1, 30, 9, 0, 8, 0),
                        datetime(
                            2026, 1, 30, 9, 0, 3, 0
                        ),  # Changed from 9:00:09 to 9:00:03
                        datetime(2026, 1, 30, 9, 0, 10, 0),
                    ],
                    dtype=pl.Datetime("ns"),
                ),
                "Price": [10.0, 10.0, 10.1, 10.0, 10.2, 10.2, 10.3, 10.2, 10.0, 10.2],
                "OldPrice": [0.0, 0.0, 10.0, 0.0, 0.0, 10.2, 0.0, 0.0, 0.0, 0.0],
                "SizeAhead": [
                    10,
                    10,
                    20,
                    0,
                    15,
                    0,
                    25,
                    15,
                    12,
                    0,
                ],  # For Queue Positioning
            }
        )

    def test_arrival_flow_imbalance(self):
        config = L3AnalyticsConfig(
            advanced_metrics=[L3AdvancedConfig(variant="ArrivalFlowImbalance")]
        )
        analytics = L3Analytics(config)
        analytics.l3 = self.mock_l3_data
        result_df = analytics.compute().collect()

        # Expected values based on mock_l3_data
        # InsertVolumeBid: (100 + 110) = 210
        # InsertVolumeAsk: (80) = 80
        # AFI = (210 - 80) / (210 + 80) = 130 / 290 = 0.44827586...
        expected_afi = (210 - 80) / (210 + 80 + 1e-9)
        self.assertAlmostEqual(
            result_df["ArrivalFlowImbalance"][0], expected_afi, places=6
        )

    def test_cancel_to_trade_ratio(self):
        config = L3AnalyticsConfig(
            advanced_metrics=[L3AdvancedConfig(variant="CancelToTradeRatio")]
        )
        analytics = L3Analytics(config)
        analytics.l3 = self.mock_l3_data
        result_df = analytics.compute().collect()

        # Expected values based on mock_l3_data
        # RemoveCountBid: 1
        # RemoveCountAsk: 2
        # ExecCount: 2
        # CTR = (1 + 2) / 2 = 3 / 2 = 1.5
        expected_ctr = (1 + 2) / (3 + 1e-9)
        self.assertAlmostEqual(
            result_df["CancelToTradeRatio"][0], expected_ctr, places=6
        )

    def test_queue_positioning(self):
        config = L3AnalyticsConfig(
            advanced_metrics=[L3AdvancedConfig(variant="AvgQueuePosition")]
        )
        analytics = L3Analytics(config)
        analytics.l3 = self.mock_l3_data
        result_df = analytics.compute().collect()

        # Expected values based on mock_l3_data
        # Executions have SizeAhead: 10 (Bid), 15 (Ask)
        # AvgQueuePosition = (10 + 15) / 2 = 12.5
        expected_avg_queue_pos = (10 + 0 + 15) / 3
        self.assertAlmostEqual(
            result_df["AvgQueuePosition"][0], expected_avg_queue_pos, places=6
        )

    def test_order_resting_time_and_fleeting_liquidity(self):
        config = L3AnalyticsConfig(
            advanced_metrics=[
                L3AdvancedConfig(variant="AvgRestingTime"),
                L3AdvancedConfig(
                    variant="FleetingLiquidityRatio", fleeting_threshold_ms=5000
                ),
            ]
        )
        analytics = L3Analytics(config)
        analytics.l3 = self.mock_l3_data
        result_df = analytics.compute().collect()

        # Expected values for Resting Time
        # Order 101 (Insert) -> 102 (Remove): DurationNs = 1s
        # Order 103 (Update) -> No direct end event in this simplified data for resting time
        # Order 105 (Insert) -> No direct end event
        # Order 201 (Insert) -> 202 (Remove): DurationNs = 1s
        # Order 203 (Update) -> No direct end event
        # AvgRestingTime = (1s + 1s) / 2 = 1s = 1_000_000_000 ns
        expected_avg_resting_time = 1_000_000_000
        self.assertAlmostEqual(
            result_df["AvgRestingTime"][0], expected_avg_resting_time, places=6
        )

        # Expected values for Fleeting Liquidity
        # Fleeting threshold = 5s. Both orders have 1s duration, so they are fleeting.
        # FleetingVolume = InsertSize for 101 (100) + InsertSize for 201 (80) = 180
        # Total Insert Volume = InsertVolumeBid (210) + InsertVolumeAsk (80) = 290
        # FleetingLiquidityRatio = 180 / 290 = 0.620689...
        expected_fleeting_volume = 100 + 80
        expected_total_insert_volume = 210 + 80
        expected_fleeting_liquidity_ratio = expected_fleeting_volume / (
            expected_total_insert_volume + 1e-9
        )
        self.assertAlmostEqual(
            result_df["FleetingLiquidityRatio"][0],
            expected_fleeting_liquidity_ratio,
            places=6,
        )

    def test_replacement_latency(self):
        config = L3AnalyticsConfig(
            advanced_metrics=[L3AdvancedConfig(variant="AvgReplacementLatency")]
        )
        analytics = L3Analytics(config)
        analytics.l3 = self.mock_l3_data
        result_df = analytics.compute().collect()

        # Expected values for Replacement Latency
        # Exec (LobAction 1) at 9:00:04 followed by Insert (LobAction 2) at 9:00:05 (Side 2) -> Latency = 1s
        # Exec (LobAction 1) at 9:00:08 followed by Insert (LobAction 2) at 9:00:09 (Side 1) -> Latency = 1s
        # AvgReplacementLatency = (1s + 1s) / 2 = 1s = 1_000_000_000 ns
        expected_avg_replacement_latency = 1_000_000_000
        self.assertAlmostEqual(
            result_df["AvgReplacementLatency"][0],
            expected_avg_replacement_latency,
            places=6,
        )


if __name__ == "__main__":
    unittest.main()
