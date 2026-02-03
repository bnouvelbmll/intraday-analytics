# Intro Tutorial

This tutorial shows how to configure and run the Intraday Analytics framework, plus the minimum data you need to get results.
It focuses on user-facing analytics modules and does not cover the internal dense row-generation module.

## 1) Concepts in 60 seconds

- You run one or more Passes. Each pass has its own modules and time bucket size.
- Modules compute metrics from raw tables (trades, l2, l3, marketstate) or from previous passes.
- Output of pass N can be used by pass N+1 (via the context).

## 2) Minimal end-to-end example

This example runs a single pass with trade analytics only.

```python
import polars as pl
from intraday_analytics.configuration import AnalyticsConfig, PassConfig
from intraday_analytics.execution import run_metrics_pipeline


def get_universe(date_str: str) -> pl.DataFrame:
    # ListingId should be numeric for DenseAnalytics.
    return pl.DataFrame(
        {
            "ListingId": [101, 102],
            "MIC": ["X", "X"],
            "Ticker": ["AAA", "BBB"],
            "CurrencyCode": ["EUR", "EUR"],
        }
    )

config = AnalyticsConfig(
    START_DATE="2025-01-01",
    END_DATE="2025-01-01",
    TEMP_DIR="/tmp/ia_tmp",
    PASSES=[
        PassConfig(
            name="pass1",
            modules=["trade"],
            time_bucket_seconds=60,
        )
    ],
    CLEAN_UP_TEMP_DIR=False,
    BATCH_FREQ=None,
)

run_metrics_pipeline(config=config, get_universe=get_universe)
```

Notes:
- `TABLES_TO_LOAD` is auto-derived from the modules. You can still set it explicitly and it will be unioned with the derived list.
- ListingId should be numeric in the raw tables and universe for best compatibility.

## 3) Multi-pass example (trade -> generic)

Pass 1 computes trade metrics. Pass 2 re-aggregates them (e.g., per InstrumentId) or resamples.

```python
from intraday_analytics.analytics.trade import TradeGenericConfig
from intraday_analytics.analytics.generic import TalibIndicatorConfig

config = AnalyticsConfig(
    START_DATE="2025-01-01",
    END_DATE="2025-01-01",
    TEMP_DIR="/tmp/ia_tmp",
    PASSES=[
        PassConfig(name="pass1", modules=["trade"], time_bucket_seconds=60),
        PassConfig(name="pass2", modules=["generic"], time_bucket_seconds=60),
    ],
    CLEAN_UP_TEMP_DIR=False,
    BATCH_FREQ=None,
)

# Pass 1 metrics
config.PASSES[0].trade_analytics.generic_metrics = [
    TradeGenericConfig(measures=["Volume", "VWAP"], sides="Total")
]

# Pass 2 re-aggregation and indicators
config.PASSES[1].generic_analytics.source_pass = "pass1"
config.PASSES[1].generic_analytics.group_by = ["ListingId", "TimeBucket"]
config.PASSES[1].generic_analytics.aggregations = {"TradeTotalVolume": "sum"}
config.PASSES[1].generic_analytics.talib_indicators = [
    TalibIndicatorConfig(name="SMA", input_col="VWAP", timeperiod=5)
]
```

## 4) Common pitfalls

- ListingId type: some analytics assume numeric ListingId (Int64). Use numeric IDs in the universe and raw tables.
- Context between passes: Pass N outputs are used by Pass N+1. If outputs are written to S3, transient errors can be retried.

## 5) Where to go next

- See docs/analytics_reference.md for what each module computes.
- See docs/multi_pass_analytics.md for chaining passes.
- See docs/shredding_and_batching.md for data preparation modes.
 - Enumerate all potential pass1 metric columns:
   - `python -m intraday_analytics.schema_utils`
   - `python -m intraday_analytics.schema_utils --config /path/to/config.json`
   - `python -m intraday_analytics.schema_utils --levels 5 --impact-horizons 1s,5s --json`
