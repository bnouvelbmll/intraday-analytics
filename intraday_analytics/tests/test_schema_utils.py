import unittest
import polars as pl
from intraday_analytics.configuration import AnalyticsConfig, PassConfig
from intraday_analytics.analytics.l2 import (
    L2AnalyticsConfig,
    L2ImbalanceConfig,
    L2LiquidityConfig,
    L2SpreadConfig,
    L2VolatilityConfig,
)
from intraday_analytics.analytics.l3 import (
    L3AnalyticsConfig,
    L3AdvancedConfig,
    L3MetricConfig,
)
from intraday_analytics.analytics.trade import (
    TradeAnalyticsConfig,
    TradeDiscrepancyConfig,
    TradeFlagConfig,
    TradeImpactConfig,
    TradeGenericConfig,
)
from intraday_analytics.analytics.execution import (
    ExecutionAnalyticsConfig,
    ExecutionDerivedConfig,
    L3ExecutionConfig,
    TradeBreakdownConfig,
)
from intraday_analytics.schema_utils import get_output_schema


class TestSchemaUtils(unittest.TestCase):

    def test_default_config_schema(self):
        """Test schema generation with default configuration."""
        config = AnalyticsConfig(PASSES=[PassConfig(name="pass1")])
        schema = get_output_schema(config.PASSES[0])

        # Verify keys exist
        self.assertIn("l2_last", schema)
        self.assertIn("l2_tw", schema)
        self.assertIn("trade", schema)
        self.assertIn("l3", schema)
        self.assertIn("execution", schema)

        # Check for some expected default columns
        self.assertIn("BidPrice1", schema["l2_last"])
        self.assertIn("SpreadRelTWA", schema["l2_tw"])
        self.assertIn("VWAP", schema["trade"])

        print("\n--- Default Config Column Counts ---")
        for k, v in schema.items():
            print(f"{k}: {len(v)} columns")

    def test_full_config_schema(self):
        """Test schema generation with a comprehensive configuration."""

        # 1. L2 Config
        l2_config = L2AnalyticsConfig(
            liquidity=[
                L2LiquidityConfig(
                    sides=["Bid", "Ask"],
                    levels=list(range(1, 11)),  # 10 levels
                    measures=[
                        "Price",
                        "Quantity",
                        "NumOrders",
                        "CumQuantity",
                        "CumOrders",
                        "CumNotional",
                    ],
                    aggregations=["Last", "TWA"],
                )
            ],
            spreads=[
                L2SpreadConfig(variant=["Abs", "BPS"], aggregations=["Last", "TWA"])
            ],
            imbalances=[
                L2ImbalanceConfig(
                    levels=list(range(1, 11)),
                    measure=["CumQuantity", "Orders", "CumNotional"],
                    aggregations=["Last", "TWA"],
                )
            ],
            volatility=[L2VolatilityConfig(source=["Mid", "WeightedMid"])],
        )

        # 2. L3 Config
        l3_config = L3AnalyticsConfig(
            generic_metrics=[
                L3MetricConfig(
                    sides=["Bid", "Ask"],
                    actions=["Insert", "Remove", "Update"],
                    measures=["Count", "Volume"],
                )
            ],
            advanced_metrics=[
                L3AdvancedConfig(variant="ArrivalFlowImbalance"),
                L3AdvancedConfig(variant="CancelToTradeRatio"),
                L3AdvancedConfig(variant="AvgQueuePosition"),
                L3AdvancedConfig(variant="AvgRestingTime"),
                L3AdvancedConfig(variant="FleetingLiquidityRatio"),
                L3AdvancedConfig(variant="AvgReplacementLatency"),
            ],
        )

        # 3. Trade Config
        trade_config = TradeAnalyticsConfig(
            generic_metrics=[
                TradeGenericConfig(
                    sides=["Total", "Bid", "Ask"],
                    measures=["Volume", "Count", "VWAP", "OHLC"],
                )
            ],
            discrepancy_metrics=[
                TradeDiscrepancyConfig(references=["MidAtPrimary", "EBBO"])
            ],
            flag_metrics=[
                TradeFlagConfig(
                    flags=["NegotiatedTrade", "BlockTrade"],
                    measures=["Volume", "Count"],
                )
            ],
            impact_metrics=[
                TradeImpactConfig(variant=["PriceImpact"], horizon=["1s", "10s"])
            ],
        )

        # 4. Execution Config
        exec_config = ExecutionAnalyticsConfig(
            l3_execution=[
                L3ExecutionConfig(
                    sides=["Bid", "Ask"], measures=["ExecutedVolume", "VWAP"]
                )
            ],
            trade_breakdown=[
                TradeBreakdownConfig(
                    trade_types=["LIT", "DARK"],
                    aggressor_sides=["Buy", "Sell", "Unknown"],
                    measures=["Volume", "VWAP", "VWPP"],
                )
            ],
            derived_metrics=[ExecutionDerivedConfig(variant="TradeImbalance")],
        )

        # Assemble Master Config
        pass1_config = PassConfig(
            name="pass1",
            l2_analytics=l2_config,
            l3_analytics=l3_config,
            trade_analytics=trade_config,
            execution_analytics=exec_config,
        )
        config = AnalyticsConfig(PASSES=[pass1_config])

        schema = get_output_schema(config.PASSES[0])

        print("\n--- Full Config Column Counts (10 Levels) ---")
        total_cols = 0
        for k, v in schema.items():
            count = len(v)
            print(f"{k}: {count} columns")
            total_cols += count

            # Basic validation
            if "error" in k:
                self.fail(f"Schema generation failed for {k}: {v}")

        print(f"TOTAL ESTIMATED COLUMNS: {total_cols}")

        # Assertions for specific generated columns (PascalCase)
        self.assertIn("BidCumNotional10", schema["l2_last"])
        self.assertIn("SpreadAbsTWA", schema["l2_tw"])
        self.assertIn("BidQuantity1TWA", schema["l2_tw"])
        self.assertIn("ImbalanceCumQuantity1TWA", schema["l2_tw"])

        self.assertIn("ArrivalFlowImbalance", schema["l3"])
        self.assertIn("LitVolumeSellAggressor", schema["execution"])

        # Trade assertions
        self.assertIn("DiscrepancyToMidAtPrimary", schema["trade"])
        self.assertIn("NegotiatedTradeVolume", schema["trade"])


if __name__ == "__main__":
    unittest.main()
