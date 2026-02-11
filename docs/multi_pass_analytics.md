# Multi-Pass Analytics

The Basalt pipeline supports multi-pass execution, allowing you to chain analytics modules where the output of one pass serves as the input for subsequent passes. This is particularly useful for:

1.  **Aggregation**: Computing metrics at a granular level (e.g., per trade or 1-minute buckets) in the first pass, and then aggregating them to a higher level (e.g., per instrument or 15-minute buckets) in a second pass.
2.  **Derived Metrics**: Calculating complex indicators (like TA-Lib technical indicators) on pre-aggregated data.
3.  **Cross-Sectional Analytics**: (Future capability) Computing metrics that depend on the entire universe of symbols after an initial symbol-level processing pass.

## Configuration

Multi-pass execution is configured via the `PASSES` list in `AnalyticsConfig`. Each `PassConfig` object defines a single pass.

```python
from basalt.configuration import AnalyticsConfig, PassConfig, GenericAnalyticsConfig, TradeGenericConfig

config = AnalyticsConfig(
    # ... other config ...
    PASSES=[
        # Pass 1: Compute 1-minute trade metrics per Listing
        PassConfig(
            name="pass1",
            modules=["trade"],
            time_bucket_seconds=60,
            trade_analytics=TradeAnalyticsConfig(
                generic_metrics=[
                    TradeGenericConfig(measures=["Volume", "NotionalEUR"], sides="Total"),
                    TradeGenericConfig(measures=["OHLC"], sides="Total")
                ]
            )
        ),
        # Pass 2: Aggregate to 15-minute buckets per Instrument
        PassConfig(
            name="pass2",
            modules=["generic"],
            time_bucket_seconds=60, # Base bucket size for this pass
            generic_analytics=GenericAnalyticsConfig(
                source_pass="pass1",
                group_by=["InstrumentId", "TimeBucket"],
                resample_rule="15m",
                aggregations={
                    "TradeTotalVolume": "sum",
                    "TradeTotalNotionalEUR": "sum",
                    "Close": "last"
                },
                talib_indicators=[
                    {"name": "SMA", "input_col": "Close", "timeperiod": 14, "output_col": "SMA_14"}
                ]
            )
        )
    ]
)
```

## Generic Analytics Module

The `generic` analytics module (`GenericAnalytics`) is designed specifically for Pass 2+ operations. It does not load raw data tables (like `trades` or `l2`) directly. Instead, it consumes the output of a previous pass.

## Tips

- Use `basalt pipeline config <pipeline.py>` for a guided editor that reflects pass type, module dependencies, and advanced options.
- Output targets can be set globally or per-pass (Parquet/Delta/SQL).

### Key Features

*   **Source Pass**: Specifies which previous pass to use as input (`source_pass`).
*   **Grouping**: Allows re-grouping data by different keys (e.g., `["InstrumentId", "TimeBucket"]`) via `group_by`. Note that you may need to join with reference data to get these new keys.
*   **Resampling**: Supports temporal resampling (e.g., `resample_rule="15m"`) to aggregate data into larger time buckets.
*   **Aggregation**: Defines how to aggregate columns (`aggregations`). Supported methods include `sum`, `mean`, `first`, `last`, `max`, `min`, `count`.
*   **TA-Lib Integration**: Applies Technical Analysis Library (TA-Lib) indicators to the data (`talib_indicators`).

### Configuration (`GenericAnalyticsConfig`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `source_pass` | `str` | Name of the previous pass to use as input. |
| `group_by` | `List[str]` | Columns to group by (e.g., `["ListingId", "TimeBucket"]`). |
| `resample_rule` | `Optional[str]` | Pandas-style offset string for resampling (e.g., "15m", "1h"). |
| `aggregations` | `Dict[str, str]` | Mapping of column names to aggregation methods. |
| `talib_indicators` | `List[TalibIndicatorConfig]` | List of TA-Lib indicators to compute. |

### TA-Lib Configuration (`TalibIndicatorConfig`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `name` | `str` | Name of the TA-Lib function (e.g., "SMA", "RSI"). |
| `input_col` | `str` | Column to use as input for the indicator. |
| `timeperiod` | `int` | Time period for the indicator (default: 14). |
| `output_col` | `Optional[str]` | Name of the output column. Defaults to `{name}_{timeperiod}`. |

## Execution Flow

1.  **Pass 1**: The pipeline runs the configured modules (e.g., `trade`, `l2`) for the first pass. The results are written to an intermediate Parquet file (or S3).
2.  **Context Update**: The execution engine loads the output of Pass 1 (as a LazyFrame) into a shared context dictionary.
3.  **Pass 2**: The `generic` module in Pass 2 retrieves the Pass 1 data from the context. It applies the configured transformations (join, resample, aggregate, indicators) and produces a new result.
4.  **Output**: The result of Pass 2 is written to its own output location.

## Naming Conventions

All output columns from analytics modules follow **PascalCase** naming convention (e.g., `TradeTotalVolume`, `TradeBidCount`). This ensures consistency across different modules and passes.
