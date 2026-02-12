import polars as pl
import datetime as dt
import unittest
import logging
from unittest.mock import MagicMock, patch
import tempfile
import shutil
import os
import pyarrow.dataset as ds
from pathlib import Path
import polars.testing

# Now that bmll2 imports are moved inside functions, we can import normally
from basalt.pipeline import AnalyticsPipeline, AnalyticsRunner
from basalt.batching import (
    S3SymbolBatcher,
    HeuristicBatchingStrategy,
    SymbolSizeEstimator,
)
from basalt.time.dense import DenseAnalytics, DenseAnalyticsConfig
from basalt.analytics.l2 import (
    L2AnalyticsLast,
    L2AnalyticsTW,
    L2AnalyticsConfig,
)
from basalt.analytics.trade import TradeAnalytics, TradeAnalyticsConfig
from basalt.analytics.l3 import L3Analytics, L3AnalyticsConfig
from basalt.analytics.execution import ExecutionAnalytics
from basalt.utils import SYMBOL_COL
from basalt.configuration import AnalyticsConfig, PassConfig


# --- Mock Data Generation ---
def create_mock_ref(listing_id: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ListingId": [listing_id],
            "ISIN": ["XS1234567890"],
            "BloombergTicker": ["ABC NA Equity"],
            "Exchange": ["XAMS"],
            "CurrencyCode": ["EUR"],
            "MIC": ["XAMS"],
            "Ticker": ["ABC"],
        }
    )


def create_mock_trades(
    listing_id: int, date_str: str, time_bucket_seconds: int
) -> pl.DataFrame:
    start_dt = dt.datetime.strptime(date_str + " 09:00:00", "%Y-%m-%d %H:%M:%S")
    end_dt = dt.datetime.strptime(date_str + " 10:00:00", "%Y-%m-%d %H:%M:%S")
    times = pl.datetime_range(
        start=start_dt,
        end=end_dt,
        interval="5m",
        eager=True,
    ).cast(
        pl.Datetime("ns")
    )  # Explicitly cast to nanoseconds
    classifications = ["LIT_CONTINUOUS"]
    return pl.DataFrame(
        {
            SYMBOL_COL: [listing_id] * len(times),
            "TradeTimestamp": times,
            "TradeDate": [dt.date(start_dt.year, start_dt.month, start_dt.day)]
            * len(times),
            "Price": [100.0 + i * 0.1 for i in range(len(times))],
            "Size": [100 + i * 10 for i in range(len(times))],
            "TradeNotional": [
                (100.0 + i * 0.1) * (100 + i * 10) for i in range(len(times))
            ],  # Added TradeNotional
            "LocalPrice": [
                100.0 + i * 0.1 for i in range(len(times))
            ],  # Added LocalPrice
            "PriceEUR": [100.0 + i * 0.1 for i in range(len(times))],  # Added PriceEUR
            "PricePoint": [
                i % 2 / 1.0 for i in range(len(times))
            ],  # Added PricePoint (values 0 or 0.5)
            "MIC": ["XAMS"] * len(times),
            "Ticker": ["ABC"] * len(times),
            "CurrencyCode": ["EUR"] * len(times),
            "TimeBucket": times.dt.truncate(f"{int(time_bucket_seconds)}s"),
            "Classification": [
                classifications[i % len(classifications)] for i in range(len(times))
            ],  # Added Classification
            "BMLLTradeType": [
                "LIT" if i % 2 == 0 else "DARK" for i in range(len(times))
            ],  # Added BMLLTradeType
            "BMLLParticipantType": ["RETAIL"]
            * len(times),  # Added for TradeAnalytics df2
            "AggressorSide": [1] * len(times),  # Added for TradeAnalytics df2
            "TradeNotionalEUR": [
                (100.0 + i * 0.1) * (100 + i * 10) for i in range(len(times))
            ],  # Added for TradeAnalytics df2
            "MarketState": ["CONTINUOUS_TRADING"] * len(times),  # Added MarketState
        }
    )


def create_mock_l2(
    listing_id: int, date_str: str, time_bucket_seconds: int
) -> pl.DataFrame:
    start_dt = dt.datetime.strptime(date_str + " 09:00:00", "%Y-%m-%d %H:%M:%S")
    end_dt = dt.datetime.strptime(date_str + " 10:00:00", "%Y-%m-%d %H:%M:%S")
    times = pl.datetime_range(
        start=start_dt,
        end=end_dt,
        interval="5m",
        eager=True,
    ).cast(
        pl.Datetime("ns")
    )  # Explicitly cast to nanoseconds
    # Generate data for L2_LEVELS = 1
    return pl.DataFrame(
        {
            SYMBOL_COL: [listing_id] * len(times),
            "EventTimestamp": times,
            "BidPrice1": [99.9 - i * 0.05 for i in range(len(times))],
            "AskPrice1": [100.1 + i * 0.05 for i in range(len(times))],
            "BidQuantity1": [
                50 + i * 5 for i in range(len(times))
            ],  # Renamed from BidSize
            "AskQuantity1": [
                60 + i * 5 for i in range(len(times))
            ],  # Renamed from AskSize
            "BidNumOrders1": [5 + i for i in range(len(times))],  # Added
            "AskNumOrders1": [6 + i for i in range(len(times))],  # Added
            "Level": [1]
            * len(
                times
            ),  # This column might not be needed if we have level-specific columns
            "MIC": ["XAMS"] * len(times),
            "Ticker": ["ABC"] * len(times),
            "CurrencyCode": ["EUR"] * len(times),
            "TimeBucket": times.dt.truncate(f"{int(time_bucket_seconds)}s"),
            "MarketState": ["CONTINUOUS_TRADING"]
            * len(times),  # Added for L2AnalyticsLast
        }
    )


def create_mock_l3(
    listing_id: int, date_str: str, time_bucket_seconds: int
) -> pl.DataFrame:
    start_dt = dt.datetime.strptime(date_str + " 09:00:00", "%Y-%m-%d %H:%M:%S")
    end_dt = dt.datetime.strptime(date_str + " 10:00:00", "%Y-%m-%d %H:%M:%S")
    times = pl.datetime_range(
        start=start_dt,
        end=end_dt,
        interval="5m",
        eager=True,
    ).cast(
        pl.Datetime("ns")
    )  # Explicitly cast to nanoseconds
    sides = [1, 2, 0]  # 1 for BID, 2 for ASK, 0 for UNKNOWN
    return pl.DataFrame(
        {
            SYMBOL_COL: [listing_id] * len(times),
            "EventTimestamp": times,
            "LobAction": [2] * len(times),  # Changed to integer values (2 for INSERT)
            "Price": [100.0] * len(times),
            "Size": [100] * len(times),
            "OldSize": [90] * len(times),  # Added OldSize
            "ExecutionPrice": [100.0] * len(times),  # Added ExecutionPrice
            "ExecutionSize": [100] * len(times),  # Added ExecutionSize
            "MIC": ["XAMS"] * len(times),
            "Ticker": ["ABC"] * len(times),
            "CurrencyCode": ["EUR"] * len(times),
            "TimeBucket": times.dt.truncate(f"{int(time_bucket_seconds)}s"),
            "Side": [
                sides[i % len(sides)] for i in range(len(times))
            ],  # Added Side, now as int
        }
    )


def create_mock_marketstate(
    listing_id: int, date_str: str, time_bucket_seconds: int
) -> pl.DataFrame:
    start_dt = dt.datetime.strptime(date_str + " 09:00:00", "%Y-%m-%d %H:%M:%S")
    end_dt = dt.datetime.strptime(date_str + " 10:00:00", "%Y-%m-%d %H:%M:%S")
    times = pl.datetime_range(
        start=start_dt,
        end=end_dt,
        interval="15m",
        eager=True,
    ).cast(
        pl.Datetime("ns")
    )  # Explicitly cast to nanoseconds
    market_states = ["PRE_OPEN", "CONTINUOUS_TRADING", "POST_CLOSE"]
    return pl.DataFrame(
        {
            SYMBOL_COL: [listing_id] * len(times),
            "EventTimestamp": times,
            "MarketState": [
                market_states[i % len(market_states)] for i in range(len(times))
            ],
            "MIC": ["XAMS"] * len(times),
            "Ticker": ["ABC"] * len(times),
            "CurrencyCode": ["EUR"] * len(times),
            "TimeBucket": times.dt.truncate(f"{int(time_bucket_seconds)}s"),
        }
    )


# --- Configuration ---
NAIVE_CONFIG = AnalyticsConfig(
    PASSES=[
        PassConfig(
            name="pass1",
            time_bucket_seconds=60,
            dense_analytics=DenseAnalyticsConfig(
                mode="uniform",
                time_interval=["09:00:00", "10:00:00"],
            ),
            l2_analytics=L2AnalyticsConfig(levels=1),
        )
    ],
    DEFAULT_FFILL=True,
    ENABLE_PERFORMANCE_LOGS=False,
    LOGGING_LEVEL="INFO",
    RUN_ONE_SYMBOL_AT_A_TIME=False,
    TABLES_TO_LOAD=["trades", "l2", "l3", "marketstate"],
)


class TestPipelineEquivalence(unittest.TestCase):

    def setUp(self):
        self.listing_id = 12345
        self.date_str = "2025-11-01"
        self.pass_config = NAIVE_CONFIG.PASSES[0]
        self.time_bucket_seconds = self.pass_config.time_bucket_seconds

        self.ref_df = create_mock_ref(self.listing_id)
        self.trades_df = create_mock_trades(
            self.listing_id, self.date_str, self.time_bucket_seconds
        )
        self.l2_df = create_mock_l2(
            self.listing_id, self.date_str, self.time_bucket_seconds
        )
        self.l3_df = create_mock_l3(
            self.listing_id, self.date_str, self.time_bucket_seconds
        )
        self.marketstate_df = create_mock_marketstate(
            self.listing_id, self.date_str, self.time_bucket_seconds
        )

        self.modules = [
            DenseAnalytics(self.ref_df, self.pass_config.dense_analytics),
            L2AnalyticsLast(self.pass_config.l2_analytics),
            L2AnalyticsTW(self.pass_config.l2_analytics),
            TradeAnalytics(self.pass_config.trade_analytics),
            L3Analytics(self.pass_config.l3_analytics),
            ExecutionAnalytics(self.pass_config.execution_analytics),
        ]

        self.pipeline = AnalyticsPipeline(self.modules, NAIVE_CONFIG, self.pass_config)

        self.tables_for_pipeline = {
            "trades": self.trades_df,
            "l2": self.l2_df,
            "l3": self.l3_df,
            "marketstate": self.marketstate_df,
        }

        # Create temporary directories
        self.temp_dir = tempfile.mkdtemp()
        self.source_dir = tempfile.mkdtemp()

        # Write mock data to "source" directory (simulating S3)
        self.source_files = {}
        for name, df in self.tables_for_pipeline.items():
            file_path = os.path.join(self.source_dir, f"{name}.parquet")
            df.write_parquet(file_path)
            self.source_files[name] = [file_path]

        # Calculate naive_result once in setUp
        logging.info("Running naive path in setUp...")
        self.naive_result = self.pipeline.run_on_multi_tables(
            **self.tables_for_pipeline
        )
        logging.debug(f"Type of naive_result in setUp: {type(self.naive_result)}")
        logging.info("Naive path in setUp complete.")

    def tearDown(self):
        # Clean up the temporary directories
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        if os.path.exists(self.source_dir):
            shutil.rmtree(self.source_dir)

    def test_equivalence(self):
        # self.naive_result is already calculated in setUp
        logging.info("Running sophisticated path (simulated via PipelineDispatcher)...")
        mock_out_writer_results = {}

        def mock_out_writer(df: pl.DataFrame, key: str):
            mock_out_writer_results[key] = df

        dispatcher = AnalyticsRunner(self.pipeline, mock_out_writer, NAIVE_CONFIG)
        dispatcher.run_batch(self.tables_for_pipeline)
        logging.info("Sophisticated path complete.")

        sophisticated_result = mock_out_writer_results.get("batch")

        self.assertIsNotNone(
            sophisticated_result, "Sophisticated path did not produce a result."
        )
        pl.testing.assert_frame_equal(
            self.naive_result, sophisticated_result, check_row_order=False
        )
        logging.info("Both paths produced equivalent results.")

    @patch("bmll.time_series.query")
    def test_s3_batcher_equivalence(self, mock_bmll_query):
        logging.info("Running S3Batcher-driven path...")

        # Mock bmll.time_series.query to return an empty DataFrame
        # This is needed for SymbolSizeEstimator, but we're not actually using BMLL data
        mock_bmll_query.return_value = pl.DataFrame(
            {"ObjectId": [], "TradeCount|Lit": []},
            schema={"ObjectId": pl.Int64, "TradeCount|Lit": pl.Int64},
        ).to_pandas()

        transform_fns = {name: lambda x: x for name in self.source_files.keys()}

        # Mock get_universe for SymbolSizeEstimator
        def mock_get_universe(date):
            return pl.DataFrame({SYMBOL_COL: [self.listing_id], "MIC": ["XAMS"]})

        # Instantiate SymbolSizeEstimator and HeuristicBatchingStrategy
        estimator = SymbolSizeEstimator(self.date_str, mock_get_universe)
        max_rows_per_table = {
            name: 1_000_000 for name in self.source_files.keys()
        }  # Dummy max rows
        batching_strategy = HeuristicBatchingStrategy(estimator, max_rows_per_table)

        # Instantiate S3SymbolBatcher with LOCAL files
        s3_batcher = S3SymbolBatcher(
            s3_file_lists=self.source_files,
            transform_fns=transform_fns,
            batching_strategy=batching_strategy,
            temp_dir=self.temp_dir,
            storage_options={},  # Empty storage options for local files
            date=self.date_str,
            get_universe=mock_get_universe,
        )

        # Run the ACTUAL shredding process
        # Use num_workers=1 to avoid multiprocessing complexity in tests,
        # but still exercise the threading logic in S3SymbolBatcher
        s3_batcher.process(num_workers=1)

        # Read the output from the temp directory
        # S3SymbolBatcher writes to: temp_dir/batch-{name}-{b_id}.parquet
        s3_batcher_output_tables = {}
        batch_id = 0  # We expect one batch

        for table_name in self.tables_for_pipeline.keys():
            path = Path(self.temp_dir) / f"batch-{table_name}-{batch_id}.parquet"
            if path.exists():
                s3_batcher_output_tables[table_name] = pl.read_parquet(path)
            else:
                logging.warning(f"Output file not found: {path}")
                s3_batcher_output_tables[table_name] = pl.DataFrame()

        # Run the pipeline with the S3Batcher's output
        s3_batcher_result = self.pipeline.run_on_multi_tables(
            **s3_batcher_output_tables
        )
        logging.info("S3Batcher-driven path complete.")

        self.assertIsNotNone(
            s3_batcher_result, "S3Batcher path did not produce a result."
        )

        # Sort both results before comparing to ensure row order doesn't matter
        # (though check_row_order=False should handle it, sorting is safer for debugging)
        naive_sorted = self.naive_result.sort(["ListingId", "TimeBucket"])
        s3_sorted = s3_batcher_result.sort(["ListingId", "TimeBucket"])

        pl.testing.assert_frame_equal(naive_sorted, s3_sorted, check_row_order=False)
        logging.info("Naive and S3Batcher paths produced equivalent results.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    unittest.main()
