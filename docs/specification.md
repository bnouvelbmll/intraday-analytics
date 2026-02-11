# Basalt Framework Specification

This document outlines the functional and non-functional requirements for the Basalt Framework. It serves as a specification for developers intending to implement a system with equivalent capabilities.

## 1. Introduction

The Basalt Framework is a high-performance, configuration-driven system designed to process large-scale financial market data (Trades, L2, L3). It computes granular metrics, aggregates them over time and instruments, and supports multi-pass execution flows. The core design philosophy emphasizes lazy evaluation, parallel processing, and modular extensibility.

## 2. System Architecture

The system follows a pipeline architecture where data flows through a series of configurable stages:

1.  **Data Ingestion**: Lazy loading of partitioned Parquet data from S3 or local storage.
2.  **Batching**: Grouping of symbols/instruments to manage memory usage during processing.
3.  **Pipeline Execution**: Sequential execution of analytics modules within a "Pass".
4.  **Multi-Pass Orchestration**: Chaining multiple passes where the output of Pass $N$ becomes the input for Pass $N+1$.
5.  **Output**: Writing results to Parquet, Delta Lake, or SQL targets.

## 3. Functional Requirements

### 3.1 Data Ingestion & Abstraction
*   **Table Abstraction**: The system must define a generic interface (e.g., `DataTable`) for different data types (Trades, L2, L3, MarketState).
*   **Lazy Loading**: Data loading must be lazy (using libraries like Polars or Arrow) to handle datasets larger than memory.
*   **Partitioning**: Support for reading data partitioned by Date and Market/MIC.
*   **Reference Data**: Ability to load and join with a "Universe" or Reference dataset (e.g., mapping `ListingId` to `InstrumentId`, `Ticker`, etc.).

### 3.2 Configuration
*   **Declarative Configuration**: The entire pipeline execution, including date ranges, selected modules, and specific metrics, must be defined via a structured configuration object (e.g., JSON/YAML or Pydantic models).
*   **Validation**: Configuration must be validated at runtime before execution starts.
*   **Schema UI**: A schema-driven UI should be available to edit configs safely.

### 3.3 Pipeline & Execution
*   **Multi-Pass Support**: The system must support defining multiple sequential "Passes".
    *   **Context Propagation**: Results from Pass $N$ must be accessible to Pass $N+1$ via a shared context.
    *   **Independent Configuration**: Each pass must have its own configuration (time buckets, active modules).
*   **Parallel Execution**: Processing must be parallelizable across dates and/or batches of symbols.
*   **Batching Strategies**:
    *   **Heuristic**: Batching based on estimated data size.
    *   **Streaming**: Processing batches sequentially to respect memory limits.

### 3.4 Analytics Modules
The framework must support pluggable analytics modules inheriting from a common base class.

#### 3.4.1 Trade Analytics
*   **Filtering**: Filter trades by attributes (e.g., `Classification`, `MarketState`).
*   **Metrics**:
    *   **Generic**: Volume, Count, Notional (EUR/USD), VWAP, OHLC.
    *   **Discrepancy**: Price deviation from reference prices (e.g., Primary Mid, EBBO).
    *   **Flags**: Metrics filtered by trade flags (e.g., Auction, OTC, Dark).
*   **Dimensions**: Aggregation by Side (Bid, Ask, Total).

#### 3.4.2 Generic Analytics (Pass 2+)
*   **Input Agnostic**: Ability to process the output of any previous pass.
*   **Resampling**: Temporal resampling (e.g., 1-minute to 15-minute buckets).
*   **Re-grouping**: Aggregation by arbitrary keys (e.g., `InstrumentId` instead of `ListingId`).
*   **Technical Indicators**: Integration with libraries like TA-Lib to compute indicators (SMA, RSI, etc.) on aggregated series.

### 3.5 Data Transformation
*   **Time Bucketing**: normalizing timestamps into fixed-width buckets (e.g., 1 minute).
*   **Forward Filling**: Optional forward filling of metrics across time buckets for continuous time series.
*   **Naming Convention**: All output columns must follow **PascalCase** (e.g., `TradeTotalVolume`, `Sma14`).

### 3.6 Orchestration & Remote Execution
*   **Dagster Integration**: Assets and schedules can be generated programmatically, with optional auto-materialization.
*   **BMLL Jobs**: Pipelines can run on BMLL instance jobs with bootstraps and log collection.
*   **CLI Control**: A single CLI should expose analytics listing, pipeline runs, remote jobs, and Dagster helpers.

## 4. Non-Functional Requirements

### 4.1 Performance
*   **Vectorization**: All computations must be vectorized (no row-wise iteration in Python).
*   **Memory Efficiency**: The system must handle datasets significantly larger than RAM by streaming data in batches.
*   **Lazy Evaluation**: Operations should be deferred until the final write step where possible.

### 4.2 Scalability
*   **Horizontal Scaling**: The architecture must support distribution across multiple cores (multiprocessing) and potentially multiple nodes (though current scope is single-node multiprocessing).
*   **Storage**: Must efficiently handle S3 object storage for input and output.

### 4.3 Extensibility
*   **Modular Design**: Adding a new metric or analytics module should not require modifying the core execution engine.
*   **Interface-Driven**: Components should interact through well-defined interfaces.

### 4.4 Reliability
*   **Error Handling**: Failures in processing one batch or date should be logged and handled gracefully without crashing the entire pipeline (unless configured to fail fast).
*   **Idempotency**: Re-running the pipeline on the same data/config should produce identical results.

## 5. Data Models

### 5.1 Core Entities
*   **Listing**: A specific tradable entity on a specific venue (identified by `ListingId`).
*   **Instrument**: A fungible financial instrument that may trade on multiple venues (identified by `InstrumentId`).
*   **TimeBucket**: The normalized time interval for aggregation.

### 5.2 Configuration Schema (Reference)
*   `AnalyticsConfig`: Global settings (dates, paths, resources).
*   `PassConfig`: Settings for a specific pass (modules, bucket size).
*   `MetricConfig`: Definition of a specific metric calculation.
