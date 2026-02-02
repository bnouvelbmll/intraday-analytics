import polars as pl
from intraday_analytics.bases import BaseAnalytics
from intraday_analytics.utils import dc
from pydantic import BaseModel, Field
from typing import List, Union, Literal, Dict
from .common import CombinatorialMetricConfig, Side, AggregationMethod

# --- Configuration Models ---

# 1. L3 Execution Metrics (from L3 data)
L3ExecutionMeasure = Literal["ExecutedVolume", "VWAP"]


class L3ExecutionConfig(CombinatorialMetricConfig):
    """
    Metrics derived from L3 execution events.
    """

    metric_type: Literal["L3_Execution"] = "L3_Execution"

    sides: Union[Side, List[Side]] = Field(
        ..., description="Side of the execution (Bid/Ask)."
    )
    measures: Union[L3ExecutionMeasure, List[L3ExecutionMeasure]] = Field(
        ..., description="Measure to compute."
    )


# 2. Trade Breakdown Metrics (from Trades data)
TradeType = Literal["LIT", "DARK"]
AggressorSideType = Literal["Buy", "Sell", "Unknown"]
TradeBreakdownMeasure = Literal["Volume", "VWAP", "VWPP"]


class TradeBreakdownConfig(CombinatorialMetricConfig):
    """
    Metrics derived from Trades, broken down by Trade Type and Aggressor Side.
    """

    metric_type: Literal["Trade_Breakdown"] = "Trade_Breakdown"

    trade_types: Union[TradeType, List[TradeType]] = Field(
        ..., description="Trade classification (LIT, DARK)."
    )
    aggressor_sides: Union[AggressorSideType, List[AggressorSideType]] = Field(
        ..., description="Aggressor side (Buy, Sell, Unknown)."
    )
    measures: Union[TradeBreakdownMeasure, List[TradeBreakdownMeasure]] = Field(
        ..., description="Measure (Volume, VWAP, VWPP)."
    )


# 3. Derived Metrics
DerivedMetricVariant = Literal["TradeImbalance"]


class ExecutionDerivedConfig(CombinatorialMetricConfig):
    """
    Metrics derived from other execution metrics.
    """

    metric_type: Literal["Execution_Derived"] = "Execution_Derived"

    variant: Union[DerivedMetricVariant, List[DerivedMetricVariant]] = Field(
        ..., description="Derived metric name."
    )


class ExecutionAnalyticsConfig(BaseModel):
    ENABLED: bool = True
    l3_execution: List[L3ExecutionConfig] = Field(default_factory=list)
    trade_breakdown: List[TradeBreakdownConfig] = Field(default_factory=list)
    derived_metrics: List[ExecutionDerivedConfig] = Field(default_factory=list)


class ExecutionAnalytics(BaseAnalytics):
    """
    Computes execution-related metrics.
    Fully configurable via ExecutionAnalyticsConfig.
    """

    REQUIRES = ["trades", "l3"]

    def __init__(self, config: ExecutionAnalyticsConfig):
        self.config = config
        super().__init__("execution", {})

    def compute(self) -> pl.LazyFrame:
        l3 = self.l3.with_columns(
            pl.col("TimeBucket").cast(pl.Int64).alias("TimeBucketInt")
        )
        trades = self.trades.with_columns(
            pl.col("TimeBucket").cast(pl.Int64).alias("TimeBucketInt")
        )

        # --- 1. L3 Execution Metrics ---
        l3_exprs = []

        # If config is empty, define default set
        l3_configs = self.config.l3_execution
        if (
            not l3_configs
            and not self.config.trade_breakdown
            and not self.config.derived_metrics
        ):
            # Default behavior: All L3 metrics
            l3_configs = [
                L3ExecutionConfig(
                    sides=["Bid", "Ask"], measures=["ExecutedVolume", "VWAP"]
                )
            ]

        for req in l3_configs:
            for variant in req.expand():
                side = variant["sides"]
                measure = variant["measures"]

                side_val = 1 if side == "Bid" else 2

                # Filter condition
                cond = pl.col("Side") == side_val

                if measure == "ExecutedVolume":
                    expr = (
                        pl.when(cond).then(pl.col("ExecutionSize")).otherwise(0)
                    ).sum()
                elif measure == "VWAP":
                    # Sum(Price*Size) / Sum(Size)
                    # We calculate components here.
                    # Note: If we just output VWAP directly, we need to be careful about division by zero.
                    # Polars handles nulls, but let's be explicit.
                    num = (
                        pl.when(cond)
                        .then(pl.col("ExecutionPrice") * pl.col("ExecutionSize"))
                        .otherwise(0)
                    ).sum()
                    den = (
                        pl.when(cond).then(pl.col("ExecutionSize")).otherwise(0)
                    ).sum()
                    expr = num / den
                else:
                    continue

                # Naming: ExecutedVolumeBid, VwapBid (Legacy naming convention)
                # We can use output_name_pattern if provided, else legacy defaults
                if req.output_name_pattern:
                    alias = req.output_name_pattern.format(**variant)
                else:
                    if measure == "ExecutedVolume":
                        alias = f"ExecutedVolume{side}"
                    elif measure == "VWAP":
                        alias = f"Vwap{side}"  # Legacy used "VwapBid" not "VWAPBid"
                    else:
                        alias = f"{measure}{side}"

                l3_exprs.append(expr.alias(alias))

        # Execute L3 Aggregation
        if l3_exprs:
            l3_agg = l3.group_by(["ListingId", "TimeBucketInt"]).agg(l3_exprs)
        else:
            l3_agg = None

        # --- 2. Trade Breakdown Metrics ---
        trade_exprs = []

        trade_configs = self.config.trade_breakdown
        if (
            not self.config.l3_execution
            and not trade_configs
            and not self.config.derived_metrics
        ):
            # Default behavior: All Trade metrics
            trade_configs = [
                TradeBreakdownConfig(
                    trade_types=["LIT", "DARK"],
                    aggressor_sides=["Buy", "Sell", "Unknown"],
                    measures=["Volume", "VWAP", "VWPP"],
                )
            ]

        for req in trade_configs:
            for variant in req.expand():
                t_type = variant["trade_types"]
                agg_side_str = variant["aggressor_sides"]
                measure = variant["measures"]

                # Map Aggressor Side
                # 1=Buy, 2=Sell, 0=Unknown
                agg_map = {"Buy": 1, "Sell": 2, "Unknown": 0}
                agg_side_val = agg_map.get(agg_side_str)

                # Filter condition
                cond = (pl.col("BMLLTradeType") == t_type) & (
                    pl.col("AggressorSide") == agg_side_val
                )

                if measure == "Volume":
                    expr = (pl.when(cond).then(pl.col("Size")).otherwise(0)).sum()
                elif measure == "VWAP":
                    num = (
                        pl.when(cond)
                        .then(pl.col("LocalPrice") * pl.col("Size"))
                        .otherwise(0)
                    ).sum()
                    den = (pl.when(cond).then(pl.col("Size")).otherwise(0)).sum()
                    expr = num / den
                elif measure == "VWPP":
                    # VolumeWeightedPricePlacement
                    # (PricePoint.clip(0,1)*2 - 1) * Size
                    val = (pl.col("PricePoint").clip(0, 1) * 2 - 1) * pl.col("Size")
                    num = (pl.when(cond).then(val).otherwise(0)).sum()
                    den = (pl.when(cond).then(pl.col("Size")).otherwise(0)).sum()
                    expr = num / den
                else:
                    continue

                # Naming: LitVolumeSellAggressor
                if req.output_name_pattern:
                    alias = req.output_name_pattern.format(**variant)
                else:
                    # Legacy naming
                    # TradeType (Title Case?) -> Legacy used "LIT" -> "Lit" (Title case)
                    # Actually legacy used "LITVolume..."? No, "LitVolume..."
                    t_type_str = t_type.title()  # LIT -> Lit
                    measure_str = (
                        "VolumeWeightedPricePlacement" if measure == "VWPP" else measure
                    )
                    alias = f"{t_type_str}{measure_str}{agg_side_str}Aggressor"

                trade_exprs.append(expr.alias(alias))

        # Execute Trade Aggregation
        if trade_exprs:
            trades_agg = trades.group_by(["ListingId", "TimeBucketInt"]).agg(
                trade_exprs
            )
        else:
            trades_agg = None

        # --- Join ---
        if l3_agg is not None and trades_agg is not None:
            df = l3_agg.join(trades_agg, on=["ListingId", "TimeBucketInt"], how="full")
        elif l3_agg is not None:
            df = l3_agg
        elif trades_agg is not None:
            df = trades_agg
        else:
            # Nothing computed
            # Return empty frame with keys
            df = l3.select(["ListingId", "TimeBucketInt"]).unique()

        # Restore TimeBucket
        df = df.with_columns(
            pl.col("TimeBucketInt").cast(pl.Datetime("ns")).alias("TimeBucket")
        ).drop("TimeBucketInt")

        # --- 3. Derived Metrics ---
        derived_configs = self.config.derived_metrics
        if (
            not self.config.l3_execution
            and not self.config.trade_breakdown
            and not derived_configs
        ):
            # Default behavior
            derived_configs = [ExecutionDerivedConfig(variant="TradeImbalance")]

        for req in derived_configs:
            for variant in req.expand():
                v_name = variant["variant"]

                if v_name == "TradeImbalance":
                    # Requires ExecutedVolumeAsk and ExecutedVolumeBid
                    # Check if they exist
                    cols = df.collect_schema().names()
                    if "ExecutedVolumeAsk" in cols and "ExecutedVolumeBid" in cols:
                        df = df.with_columns(
                            (
                                (
                                    pl.col("ExecutedVolumeAsk")
                                    - pl.col("ExecutedVolumeBid")
                                )
                                / (
                                    pl.col("ExecutedVolumeAsk")
                                    + pl.col("ExecutedVolumeBid")
                                )
                            ).alias("TradeImbalance")
                        )
                    else:
                        # Warn or skip?
                        # For now, we assume dependencies are met or we skip
                        pass

        self.df = df
        return df
