# Analytics Reference

This is a reference manual for analytics currently supported by the framework. It describes what each module computes, required tables, and key configuration knobs. The internal dense row-generation module is intentionally omitted.

Terminology:
- ListingId: listing-level identifier.
- TimeBucket: time bucket column (derived from timestamps).
- All modules produce metrics grouped by ListingId and TimeBucket unless noted.

Time bucket semantics (pass-level):
- `time_bucket_anchor`: `"end"` (default) or `"start"`. `"end"` attaches the bucket timestamp to the end of the interval to avoid lookahead bias.
- `time_bucket_closed`: `"right"` (default) or `"left"`. Default is right-closed, left-open intervals.

## Module Index

- trade
- l2 (last snapshot)
- l2tw (time-weighted)
- l3
- execution
- iceberg
- cbbo
- generic

---

## trade

Purpose: compute trade-level aggregates from the trades table.

Required tables:
- trades

Key config:
- `TradeAnalyticsConfig.generic_metrics` (TradeGenericConfig)
  - measures: Volume, Count, NotionalEUR, NotionalUSD, RetailCount, BlockCount, AuctionNotional, OTCVolume, VWAP, OHLC, AvgPrice, MedianPrice
  - sides: Bid, Ask, Total, Unknown
- `TradeAnalyticsConfig.discrepancy_metrics` (TradeDiscrepancyConfig)
  - references: MidAtPrimary, EBBO, PreTradeMid, MidPrice, BestBid, BestAsk, BestBidAtVenue, BestAskAtVenue, BestBidAtPrimary, BestAskAtPrimary
  - sides: Bid, Ask, Total, Unknown
  - aggregations: First, Last, Min, Max, Mean, Sum, Median, Std (default: Mean)
- `TradeAnalyticsConfig.flag_metrics` (TradeFlagConfig)
  - flags: NegotiatedTrade, OddLotTrade, BlockTrade, CrossTrade, AlgorithmicTrade, IcebergExecution
  - measures: Volume, Count, AvgNotional
- `TradeAnalyticsConfig.change_metrics` (TradeChangeConfig)
  - measures: PreTradeElapsedTimeChg, PostTradeElapsedTimeChg, PricePoint
  - scopes: Local, Primary, Venue
- `TradeAnalyticsConfig.impact_metrics` (TradeImpactConfig)
  - variant: EffectiveSpread, RealizedSpread, PriceImpact
  - horizon: e.g., 100ms, 1s, 5s
  - reference_price_col: default PreTradeMid

Notes:
- Aggregations are per bucket and side (Bid/Ask/Total as configured).
- If no metrics are configured, defaults are computed.
- OHLC is event-based by default (Open=first, Close=last within bucket). For non-naive OHLC with forward-fill behavior, use L2 OHLC or compute in pass2.

---

## l2 (last snapshot)

Purpose: compute L2 order book metrics based on the last snapshot in each bucket.

Required tables:
- l2

Key config:
- `L2AnalyticsConfig.liquidity` (L2LiquidityConfig)
  - sides: Bid, Ask
  - levels: int or list[int]
  - measures: Quantity, CumQuantity, CumNotional, Price, InsertAge, LastMod, SizeAhead, NumOrders, CumOrders
- `L2AnalyticsConfig.spreads` (L2SpreadConfig)
  - variant: Abs, BPS
- `L2AnalyticsConfig.imbalances` (L2ImbalanceConfig)
  - levels: int or list[int]
  - measure: Quantity, CumQuantity, CumNotional, Orders
- `L2AnalyticsConfig.volatility` (L2VolatilityConfig)
  - source: Mid, Bid, Ask, WeightedMid
  - aggregations: First, Last, Min, Max, Mean, Sum, Median, Std
- `L2AnalyticsConfig.ohlc` (L2OHLCConfig)
  - source: Mid, Bid, Ask, WeightedMid
  - open_mode: event | prev_close
    - event: Open/High/Low/Close from events within the bucket
    - prev_close: uses previous Close for empty buckets and fills OHLC consistently

---

## l2tw (time-weighted)

Purpose: time-weighted L2 metrics (similar measures to l2 but time-weighted within bucket).

Required tables:
- l2

Key config:
- Same as `L2AnalyticsConfig`, computed using the time-weighted analytics base.

---

## l3

Purpose: compute L3 order book analytics (e.g., order lifetimes, queue metrics, etc.).

Required tables:
- l3

Key config:
- `L3AnalyticsConfig.generic_metrics` (L3MetricConfig)
  - sides: Bid, Ask
  - actions: Insert, Remove, Update, UpdateInserted, UpdateRemoved
  - measures: Count, Volume
  - aggregations: First, Last, Min, Max, Mean, Sum, Median, Std (default: Sum)
- `L3AnalyticsConfig.advanced_metrics` (L3AdvancedConfig)
  - variant: ArrivalFlowImbalance, CancelToTradeRatio, AvgQueuePosition, AvgRestingTime, FleetingLiquidityRatio, AvgReplacementLatency
  - fleeting_threshold_ms: threshold for fleeting liquidity ratio

Notes:
- L3 metrics rely on per-order event sequences; ensure l3 data is complete for the time range.

---

## Materialization Plots

Use `intraday_analytics.plotting` to visualize Dagster materializations.

```python
from dagster import DagsterInstance, AssetKey
from intraday_analytics.plotting import materialization_dashboard

instance = DagsterInstance.get()
asset_keys = [AssetKey(["BMLL", "l2"])]
materialization_dashboard(instance, asset_keys, metric_key="size_bytes")
```

This shows:
- a calendar heatmap of universe count per day
- a calendar heatmap of total size per day
- a universe/date heatmap of `size_bytes`

## execution

Purpose: execution-level metrics derived from trades and L3.

Required tables:
- trades
- l3

Key config:
- `ExecutionAnalyticsConfig.l3_execution` (L3ExecutionConfig)
  - sides: Bid, Ask
  - measures: ExecutedVolume, VWAP
- `ExecutionAnalyticsConfig.trade_breakdown` (TradeBreakdownConfig)
  - trade_types: LIT, DARK
  - aggressor_sides: Buy, Sell, Unknown
  - measures: Volume, VWAP, VWPP
- `ExecutionAnalyticsConfig.derived_metrics` (ExecutionDerivedConfig)
  - variant: TradeImbalance

Notes:
- If config is empty, default sets of L3 and trade breakdown metrics are computed.

---

## iceberg

Purpose: detect iceberg executions from L3 events and optionally tag trades for reuse in trade analytics.

Required tables:
- l3
- trades (needed for trade tagging)

Key config:
- `IcebergAnalyticsConfig.metrics` (IcebergMetricConfig)
  - measures: ExecutionVolume, ExecutionCount, AverageSize, RevealedPeakCount, AveragePeakCount, OrderImbalance
  - sides: Bid, Ask, Total
- `IcebergAnalyticsConfig.tag_trades`: enable iceberg trade tagging (default: true)
- `IcebergAnalyticsConfig.trade_tag_context_key`: context key for tagged trades (default: trades_iceberg)
- `IcebergAnalyticsConfig.match_tolerance_seconds`: asof join tolerance for trade tagging
- `IcebergAnalyticsConfig.price_match`: require exact price match when tagging trades

Output:
- IcebergExecutionVolume{Side}
- IcebergExecutionCount{Side}
- IcebergAverageSize{Side}
- IcebergRevealedPeakCount{Side}
- IcebergAveragePeakCount{Side}
- IcebergOrderImbalance

Notes:
- Heuristic: iceberg executions are inferred from L3 remove events with `ExecutionSize > 0` and `RevealedQty = max(ExecutionSize - OldSize, 0)`; optional trade tagging uses an asof join within `match_tolerance_seconds` and optional price match.
- Trade-match heuristic (optional): trades filtered to LIT/DARK (configurable) are matched to nearby L3 executions by time/price/side; trades that exactly match a book execution can be excluded by size tolerance. If side is missing, it is inferred via Lee-Ready against mid.
- Peak linking (optional): iceberg refills are approximated by linking a remove execution to the next insert at the same side/price within `peak_link_tolerance_seconds`, and cumulative peak counts per iceberg are averaged in `IcebergAveragePeakCount` (null if linking disabled).
- Tagged trades include `IcebergExecution` (bool) and `IcebergTag` (L3_REVEALED, TRADE_MATCH, BOTH).
- Trade analytics can reuse iceberg tags via `TradeAnalyticsConfig.use_tagged_trades` + `tagged_trades_context_key`.

---

## cbbo

Purpose: compare L2 top-of-book to millisecond CBBO, joined by InstrumentId, and compute time-at and quantity-at CBBO.

Required tables:
- l2
- cbbo

Key config:
- `CBBOAnalyticsConfig.measures`
  - TimeAtCBB, TimeAtCBO, QuantityAtCBB, QuantityAtCBO

Output:
- TimeAtCBB
- TimeAtCBO
- QuantityAtCBB
- QuantityAtCBO
- QuantityAtCBBMin
- QuantityAtCBBMax
- QuantityAtCBBMedian
- QuantityAtCBOMin
- QuantityAtCBOMax
- QuantityAtCBOMedian

Notes:
- CBBO is joined to L2 on InstrumentId and millisecond EventTimestamp.
- TimeAt* is computed as a time-weighted share within the TimeBucket.
- QuantityAt* variants are computed on quantity-at-CBBO (0 when not matching): TWMean uses time-weighted mean over the full bucket, Min/Max/Median use event-based values.

---

## retail_imbalance

Purpose: compute retail trade imbalance as a separate module.

Required tables:
- trades

Output:
- RetailTradeImbalance

---

## generic

Purpose: post-process output of a previous pass (aggregation, resampling, TA-Lib indicators).

Required tables:
- none (reads from context)

Key config:
- `GenericAnalyticsConfig.source_pass`: pass name to read from (default: pass1)
- `GenericAnalyticsConfig.group_by`: group keys (e.g., ListingId, InstrumentId, TimeBucket)
- `GenericAnalyticsConfig.resample_rule`: resampling rule (e.g., 15m)
- `GenericAnalyticsConfig.aggregations`: column -> aggregation method (sum, mean, first, last, max, min, count)
- `GenericAnalyticsConfig.talib_indicators` (TalibIndicatorConfig)
  - name: SMA, RSI, etc.
  - input_col: column used as indicator input
  - timeperiod: indicator window
  - output_col: optional custom name

Notes:
- Join with ref is automatic if the module has a ref (e.g., to access InstrumentId).
- If TA-Lib is not installed, indicators are skipped with a warning.

---

## Output conventions

- Output column names are determined by the metric configurations (some modules use explicit output name patterns).
- Results are sorted by ListingId and TimeBucket during final write.

For full configuration options, see the Pydantic configs in `intraday_analytics/analytics/*` and `intraday_analytics/configuration.py`.
