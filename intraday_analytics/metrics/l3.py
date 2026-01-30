import polars as pl
from intraday_analytics import BaseAnalytics, dc
from dataclasses import dataclass


@dataclass
class L3AnalyticsConfig:
    """
    These need review before they can be enabled
    """

    enable_fleeting_liquidity: bool = False
    enable_arrival_flow_imbalance: bool = False
    enable_replacement_latency: bool = False
    enable_queue_positioning: bool = False
    enable_cancel_to_trade_ratio: bool = False
    enable_order_resting_time: bool = False
    fleeting_threshold_ms: int = 100


class L3Analytics(BaseAnalytics):
    """
    Computes L3 order book event metrics, such as counts and volumes for
    insert, remove, and update events on both the bid and ask sides.

    Metrics computed:
    - Insert, Remove, and Update Counts for Bid and Ask sides.
    - Insert, Remove, and Update Volumes for Bid and Ask sides.
    - Optional advanced metrics: Fleeting Liquidity, Arrival Flow Imbalance,
      Replacement Latency, Queue Positioning, Cancel-to-Trade Ratio, Order Resting Time.
    """

    REQUIRES = ["l3"]

    def __init__(self, config: L3AnalyticsConfig):
        self.config = config

        fill_cols = {
            "InsertCountBid": "zero",
            "RemoveCountBid": "zero",
            "UpdateCountBid": "zero",
            "InsertVolumeBid": "zero",
            "RemoveVolumeBid": "zero",
            # "UpdateVolumeBid":"zero",
            "UpdateInsertedVolumeBid": "zero",
            "UpdateRemovedVolumeBid": "zero",
            "InsertCountAsk": "zero",
            "RemoveCountAsk": "zero",
            "UpdateCountAsk": "zero",
            "InsertVolumeAsk": "zero",
            "RemoveVolumeAsk": "zero",
            # "UpdateVolumeAsk":"zero",
            "UpdateInsertedVolumeAsk": "zero",
            "UpdateRemovedVolumeAsk": "zero",
        }

        if self.config.enable_arrival_flow_imbalance:
            fill_cols["ArrivalFlowImbalance"] = "zero"

        if self.config.enable_cancel_to_trade_ratio:
            fill_cols["CancelToTradeRatio"] = "zero"

        if self.config.enable_queue_positioning:
            fill_cols["AvgQueuePosition"] = "zero"

        if self.config.enable_order_resting_time:
            fill_cols["AvgRestingTime"] = "zero"

        if self.config.enable_fleeting_liquidity:
            fill_cols["FleetingLiquidityRatio"] = "zero"

        if self.config.enable_replacement_latency:
            fill_cols["AvgReplacementLatency"] = "zero"

        super().__init__("l3", fill_cols)

    def compute(self) -> pl.LazyFrame:
        # Basic Aggregations
        df_bid = (
            self.l3.filter(pl.col("Side") == 1)
            .group_by(["ListingId", "TimeBucket"])
            .agg(
                (pl.when(pl.col("LobAction") == 2).then(pl.lit(1)).otherwise(pl.lit(0)))
                .sum()
                .alias("InsertCountBid"),
                (pl.when(pl.col("LobAction") == 3).then(pl.lit(1)).otherwise(pl.lit(0)))
                .sum()
                .alias("RemoveCountBid"),
                (pl.when(pl.col("LobAction") == 4).then(pl.lit(1)).otherwise(pl.lit(0)))
                .sum()
                .alias("UpdateCountBid"),
                (
                    pl.when(pl.col("LobAction") == 2)
                    .then(pl.col("Size"))
                    .otherwise(pl.lit(0))
                )
                .sum()
                .alias("InsertVolumeBid"),
                (
                    pl.when(pl.col("LobAction") == 3)
                    .then(pl.col("OldSize"))
                    .otherwise(pl.lit(0))
                )
                .sum()
                .alias("RemoveVolumeBid"),
                (
                    pl.when(pl.col("LobAction") == 4)
                    .then(pl.col("Size"))
                    .otherwise(pl.lit(0))
                )
                .sum()
                .alias("UpdateInsertedVolumeBid"),
                (
                    pl.when(pl.col("LobAction") == 4)
                    .then(pl.col("OldSize"))
                    .otherwise(pl.lit(0))
                )
                .sum()
                .alias("UpdateRemovedVolumeBid"),
            )
        )
        df_ask = (
            self.l3.filter(pl.col("Side") == 2)
            .group_by(["ListingId", "TimeBucket"])
            .agg(
                (pl.when(pl.col("LobAction") == 2).then(pl.lit(1)).otherwise(pl.lit(0)))
                .sum()
                .alias("InsertCountAsk"),
                (pl.when(pl.col("LobAction") == 3).then(pl.lit(1)).otherwise(pl.lit(0)))
                .sum()
                .alias("RemoveCountAsk"),
                (pl.when(pl.col("LobAction") == 4).then(pl.lit(1)).otherwise(pl.lit(0)))
                .sum()
                .alias("UpdateCountAsk"),
                (
                    pl.when(pl.col("LobAction") == 2)
                    .then(pl.col("Size"))
                    .otherwise(pl.lit(0))
                )
                .sum()
                .alias("InsertVolumeAsk"),
                (
                    pl.when(pl.col("LobAction") == 3)
                    .then(pl.col("OldSize"))
                    .otherwise(pl.lit(0))
                )
                .sum()
                .alias("RemoveVolumeAsk"),
                (
                    pl.when(pl.col("LobAction") == 4)
                    .then(pl.col("Size"))
                    .otherwise(pl.lit(0))
                )
                .sum()
                .alias("UpdateInsertedVolumeAsk"),
                (
                    pl.when(pl.col("LobAction") == 4)
                    .then(pl.col("OldSize"))
                    .otherwise(pl.lit(0))
                )
                .sum()
                .alias("UpdateRemovedVolumeAsk"),
            )
        )

        base_df = dc(
            df_bid.join(
                df_ask, on=["ListingId", "TimeBucket"], how="full", suffix="_ask"
            ),
            "_ask",
        )

        # --- Advanced Metrics ---

        # --- Advanced Metrics ---

        # 1. Arrival Flow Imbalance
        if self.config.enable_arrival_flow_imbalance:
            base_df = self._compute_arrival_flow_imbalance(base_df)

        # 2. Cancel-to-Trade Ratio
        if self.config.enable_cancel_to_trade_ratio:
            base_df = self._compute_cancel_to_trade_ratio(base_df)

        # 3. Queue Positioning Efficiency
        if self.config.enable_queue_positioning:
            base_df = self._compute_queue_positioning(base_df)

        # 4. Order Resting Time & 5. Fleeting Liquidity
        if (
            self.config.enable_order_resting_time
            or self.config.enable_fleeting_liquidity
        ):
            base_df = self._compute_order_resting_time_and_fleeting_liquidity(base_df)

        # 6. Replacement Latency
        if self.config.enable_replacement_latency:
            base_df = self._compute_replacement_latency(base_df)

        self.df = base_df
        return base_df

    def _compute_arrival_flow_imbalance(self, base_df: pl.LazyFrame) -> pl.LazyFrame:
        return base_df.with_columns(
            (
                (pl.col("InsertVolumeBid") - pl.col("InsertVolumeAsk"))
                / (pl.col("InsertVolumeBid") + pl.col("InsertVolumeAsk") + 1e-9)
            ).alias("ArrivalFlowImbalance")
        )

        # 2. Cancel-to-Trade Ratio
        if self.config.enable_cancel_to_trade_ratio:
            base_df = self._compute_cancel_to_trade_ratio(base_df)

    def _compute_cancel_to_trade_ratio(self, base_df: pl.LazyFrame) -> pl.LazyFrame:
        # Executions are identified by ExecutionSize > 0
        exec_counts = (
            self.l3.filter(pl.col("ExecutionSize") > 0)
            .group_by(["ListingId", "TimeBucket"])
            .agg(pl.len().alias("ExecCount"))
        )
        base_df = base_df.join(
            exec_counts, on=["ListingId", "TimeBucket"], how="left"
        ).with_columns(pl.col("ExecCount").fill_null(pl.lit(0)))

        base_df = base_df.with_columns(
            (
                (pl.col("RemoveCountBid") + pl.col("RemoveCountAsk"))
                / (pl.col("ExecCount") + 1e-9)
            ).alias("CancelToTradeRatio")
        ).drop("ExecCount")
        return base_df

        # 3. Queue Positioning Efficiency
        if self.config.enable_queue_positioning:
            base_df = self._compute_queue_positioning(base_df)

    def _compute_queue_positioning(self, base_df: pl.LazyFrame) -> pl.LazyFrame:
        # Executions are identified by ExecutionSize > 0
        queue_pos = (
            self.l3.filter(pl.col("ExecutionSize") > 0)
            .group_by(["ListingId", "TimeBucket"])
            .agg(pl.col("SizeAhead").mean().alias("AvgQueuePosition"))
        )
        base_df = base_df.join(queue_pos, on=["ListingId", "TimeBucket"], how="left")
        return base_df

        # 4. Order Resting Time & 5. Fleeting Liquidity
        if (
            self.config.enable_order_resting_time
            or self.config.enable_fleeting_liquidity
        ):
            base_df = self._compute_order_resting_time_and_fleeting_liquidity(base_df)

    def _compute_order_resting_time_and_fleeting_liquidity(
        self, base_df: pl.LazyFrame
    ) -> pl.LazyFrame:
        # We need to join inserts with removes/execs
        inserts = (
            self.l3.filter(pl.col("LobAction") == 2)
            .select(["ListingId", "OrderID", "EventTimestamp", "Size"])
            .rename({"EventTimestamp": "InsertTime", "Size": "InsertSize"})
        )

        end_events = (
            self.l3.filter(pl.col("LobAction").is_in([1, 3]))
            .select(["ListingId", "OrderID", "EventTimestamp", "TimeBucket"])
            .rename({"EventTimestamp": "EndTime"})
        )

        # Join on OrderID and ListingId
        # Note: This only captures orders that start AND end in the current batch/window
        lifetimes = inserts.join(end_events, on=["ListingId", "OrderID"], how="inner")

        lifetimes = lifetimes.with_columns(
            (pl.col("EndTime") - pl.col("InsertTime"))
            .dt.total_nanoseconds()
            .alias("DurationNs")
        )

        if self.config.enable_order_resting_time:
            resting_metrics = lifetimes.group_by(["ListingId", "TimeBucket"]).agg(
                pl.col("DurationNs").mean().alias("AvgRestingTime")
            )
            base_df = base_df.join(
                resting_metrics, on=["ListingId", "TimeBucket"], how="left"
            )

        if self.config.enable_fleeting_liquidity:
            threshold_ns = self.config.fleeting_threshold_ms * 1_000_000
            fleeting_metrics = lifetimes.group_by(["ListingId", "TimeBucket"]).agg(
                (
                    pl.when(pl.col("DurationNs") < threshold_ns)
                    .then(pl.col("InsertSize"))
                    .otherwise(pl.lit(0))
                )
                .sum()
                .alias("FleetingVolume")
            )

            # Join back to get Total Insert Volume for ratio
            base_df = base_df.join(
                fleeting_metrics, on=["ListingId", "TimeBucket"], how="left"
            )
            base_df = base_df.with_columns(
                (
                    pl.col("FleetingVolume")
                    / (pl.col("InsertVolumeBid") + pl.col("InsertVolumeAsk") + 1e-9)
                ).alias("FleetingLiquidityRatio")
            ).drop("FleetingVolume")
        return base_df

        # 6. Replacement Latency
        if self.config.enable_replacement_latency:
            base_df = self._compute_replacement_latency(base_df)

    def _compute_replacement_latency(self, base_df: pl.LazyFrame) -> pl.LazyFrame:
        # Filter for Inserts (LobAction == 2) and Executions (ExecutionSize > 0)
        events = (
            self.l3.filter((pl.col("LobAction") == 2) | (pl.col("ExecutionSize") > 0))
            .select(
                [
                    "ListingId",
                    "TimeBucket",
                    "Side",
                    "LobAction",
                    "EventTimestamp",
                    "ExecutionSize",
                ]
            )
            .sort(["ListingId", "Side", "EventTimestamp"])
        )

        # Calculate time to next insert after an execution
        events = events.with_columns(
            pl.col("EventTimestamp")
            .shift(-1)
            .over(["ListingId", "Side"])
            .alias("NextEventTime"),
            pl.col("LobAction")
            .shift(-1)
            .over(["ListingId", "Side"])
            .alias("NextLobAction"),
            pl.col("ExecutionSize")
            .shift(-1)
            .over(["ListingId", "Side"])
            .alias("NextExecutionSize"),
        )

        # Filter for Exec followed by Insert
        latency = (
            events.filter(
                (pl.col("ExecutionSize") > 0) & (pl.col("NextLobAction") == 2)
            )
            .with_columns(
                (pl.col("NextEventTime") - pl.col("EventTimestamp"))
                .dt.total_nanoseconds()
                .alias("LatencyNs")
            )
            .group_by(["ListingId", "TimeBucket"])
            .agg(pl.col("LatencyNs").mean().alias("AvgReplacementLatency"))
        )

        base_df = base_df.join(latency, on=["ListingId", "TimeBucket"], how="left")
        return base_df
