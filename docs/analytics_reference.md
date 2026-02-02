# Analytics Reference

This is a reference manual for analytics currently supported by the framework. It describes what each module computes, required tables, and key configuration knobs. The internal dense row-generation module is intentionally omitted.

Terminology:
- ListingId: listing-level identifier.
- TimeBucket: time bucket column (derived from timestamps).
- All modules produce metrics grouped by ListingId and TimeBucket unless noted.

## Module Index

- trade
- l2 (last snapshot)
- l2tw (time-weighted)
- l3
- execution
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
  - references: MidAtPrimary, EBBO, BestBid, BestAsk, BestBidAtVenue, BestAskAtVenue, BestBidAtPrimary, BestAskAtPrimary
- `TradeAnalyticsConfig.flag_metrics` (TradeFlagConfig)
  - flags: NegotiatedTrade, OddLotTrade, BlockTrade, CrossTrade, AlgorithmicTrade
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
  - source: Mid, Bid, Ask, Last, WeightedMid
  - aggregations: Std

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
- `L3AnalyticsConfig` (module-specific metrics defined in l3 analytics implementation)

Notes:
- L3 metrics rely on per-order event sequences; ensure l3 data is complete for the time range.

---

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
