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
from intraday_analytics.pipeline import AnalyticsPipeline
from intraday_analytics.batching import PipelineDispatcher, S3SymbolBatcher, HeuristicBatchingStrategy, SymbolSizeEstimator
from intraday_analytics.metrics.dense import DenseAnalytics
from intraday_analytics.metrics.l2 import L2AnalyticsLast, L2AnalyticsTW
from intraday_analytics.metrics.trade import TradeAnalytics
from intraday_analytics.metrics.l3 import L3Analytics
from intraday_analytics.metrics.execution import ExecutionAnalytics
from intraday_analytics.utils import SYMBOL_COL

# --- Mock Data Generation ---
def create_mock_ref(listing_id: int) -> pl.DataFrame:
    return pl.DataFrame({
        "ListingId": [listing_id],
        "ISIN": ["XS1234567890"],
        "BloombergTicker": ["ABC NA Equity"],
        "Exchange": ["XAMS"],
        "CurrencyCode": ["EUR"],
        "MIC": ["XAMS"],
        "Ticker": ["ABC"],
    })

def create_mock_trades(listing_id: int, date_str: str, time_bucket_seconds: int) -> pl.DataFrame:
    start_dt = dt.datetime.strptime(date_str + " 09:00:00", "%Y-%m-%d %H:%M:%S")
    end_dt = dt.datetime.strptime(date_str + " 10:00:00", "%Y-%m-%d %H:%M:%S")
    times = pl.datetime_range(
        start=start_dt,
        end=end_dt,
        interval="5m",
        eager=True,
    ).cast(pl.Datetime("ns")) # Explicitly cast to nanoseconds
    classifications = [ "LIT_CONTINUOUS"]
    return pl.DataFrame({
        SYMBOL_COL: [listing_id] * len(times),
        "TradeTimestamp": times,
        "TradeDate": [dt.date(start_dt.year, start_dt.month, start_dt.day)] * len(times),
        "Price": [100.0 + i * 0.1 for i in range(len(times))],
        "Size": [100 + i * 10 for i in range(len(times))],
        "TradeNotional": [(100.0 + i * 0.1) * (100 + i * 10) for i in range(len(times))], # Added TradeNotional
        "LPrice": [100.0 + i * 0.1 for i in range(len(times))], # Added LPrice
        "EPrice": [100.0 + i * 0.1 for i in range(len(times))], # Added EPrice
        "PricePoint": [i % 2 / 1.0 for i in range(len(times))], # Added PricePoint (values 0 or 0.5)
        "MIC": ["XAMS"] * len(times),
        "Ticker": ["ABC"] * len(times),
        "CurrencyCode": ["EUR"] * len(times),
        "TimeBucket": times.dt.truncate(f"{time_bucket_seconds}s"),
        "Classification": [classifications[i % len(classifications)] for i in range(len(times))], # Added Classification
        "BMLLTradeType": ["LIT" if i % 2 == 0 else "DARK" for i in range(len(times))], # Added BMLLTradeType
        "BMLLParticipantType": ["RETAIL"] * len(times), # Added for TradeAnalytics df2
        "AggressorSide": [1] * len(times), # Added for TradeAnalytics df2
        "TradeNotionalEUR": [(100.0 + i * 0.1) * (100 + i * 10) for i in range(len(times))], # Added for TradeAnalytics df2
        "MarketState": ["CONTINUOUS_TRADING"] * len(times), # Added MarketState
    })

def create_mock_l2(listing_id: int, date_str: str, time_bucket_seconds: int) -> pl.DataFrame:
    start_dt = dt.datetime.strptime(date_str + " 09:00:00", "%Y-%m-%d %H:%M:%S")
    end_dt = dt.datetime.strptime(date_str + " 10:00:00", "%Y-%m-%d %H:%M:%S")
    times = pl.datetime_range(
        start=start_dt,
        end=end_dt,
        interval="5m",
        eager=True,
    ).cast(pl.Datetime("ns")) # Explicitly cast to nanoseconds
    # Generate data for L2_LEVELS = 1
    return pl.DataFrame({
        SYMBOL_COL: [listing_id] * len(times),
        "EventTimestamp": times,
        "BidPrice1": [99.9 - i * 0.05 for i in range(len(times))],
        "AskPrice1": [100.1 + i * 0.05 for i in range(len(times))],
        "BidQuantity1": [50 + i * 5 for i in range(len(times))], # Renamed from BidSize
        "AskQuantity1": [60 + i * 5 for i in range(len(times))], # Renamed from AskSize
        "BidNumOrders1": [5 + i for i in range(len(times))], # Added
        "AskNumOrders1": [6 + i for i in range(len(times))], # Added
        "Level": [1] * len(times), # This column might not be needed if we have level-specific columns
        "MIC": ["XAMS"] * len(times),
        "Ticker": ["ABC"] * len(times),
        "CurrencyCode": ["EUR"] * len(times),
        "TimeBucket": times.dt.truncate(f"{time_bucket_seconds}s"),
        "MarketState": ["CONTINUOUS_TRADING"] * len(times), # Added for L2AnalyticsLast
    })

def create_mock_l3(listing_id: int, date_str: str, time_bucket_seconds: int) -> pl.DataFrame:
    start_dt = dt.datetime.strptime(date_str + " 09:00:00", "%Y-%m-%d %H:%M:%S")
    end_dt = dt.datetime.strptime(date_str + " 10:00:00", "%Y-%m-%d %H:%M:%S")
    times = pl.datetime_range(
        start=start_dt,
        end=end_dt,
        interval="5m",
        eager=True,
    ).cast(pl.Datetime("ns")) # Explicitly cast to nanoseconds
    sides = [1, 2, 0] # 1 for BID, 2 for ASK, 0 for UNKNOWN
    return pl.DataFrame({
        SYMBOL_COL: [listing_id] * len(times),
        "EventTimestamp": times,
        "LobAction": [2] * len(times), # Changed to integer values (2 for INSERT)
        "Price": [100.0] * len(times),
        "Size": [100] * len(times),
        "OldSize": [90] * len(times), # Added OldSize
        "ExecutionPrice": [100.0] * len(times), # Added ExecutionPrice
        "ExecutionSize": [100] * len(times), # Added ExecutionSize
        "MIC": ["XAMS"] * len(times),
        "Ticker": ["ABC"] * len(times),
        "CurrencyCode": ["EUR"] * len(times),
        "TimeBucket": times.dt.truncate(f"{time_bucket_seconds}s"),
        "Side": [sides[i % len(sides)] for i in range(len(times))], # Added Side, now as int
    })

def create_mock_marketstate(listing_id: int, date_str: str, time_bucket_seconds: int) -> pl.DataFrame:
    start_dt = dt.datetime.strptime(date_str + " 09:00:00", "%Y-%m-%d %H:%M:%S")
    end_dt = dt.datetime.strptime(date_str + " 10:00:00", "%Y-%m-%d %H:%M:%S")
    times = pl.datetime_range(
        start=start_dt,
        end=end_dt,
        interval="15m",
        eager=True,
    ).cast(pl.Datetime("ns")) # Explicitly cast to nanoseconds
    market_states = ["PRE_OPEN", "CONTINUOUS_TRADING", "POST_CLOSE"]
    return pl.DataFrame({
        SYMBOL_COL: [listing_id] * len(times),
        "EventTimestamp": times,
        "MarketState": [market_states[i % len(market_states)] for i in range(len(times))],
        "MIC": ["XAMS"] * len(times),
        "Ticker": ["ABC"] * len(times),
        "CurrencyCode": ["EUR"] * len(times),
        "TimeBucket": times.dt.truncate(f"{time_bucket_seconds}s"),
    })


# --- Configuration ---
NAIVE_CONFIG = {
    "TIME_BUCKET_SECONDS": 60,
    "L2_LEVELS": 1, # Keep it simple for mock data
    "DENSE_OUTPUT": True,
    "DENSE_OUTPUT_MODE": "uniform",
    "DENSE_OUTPUT_TIME_INTERVAL": ["09:00:00", "10:00:00"],
    "DEFAULT_FFILL": True,
    "PROFILE": False,
    "ENABLE_PERFORMANCE_LOGS": False,
    "LOGGING_LEVEL": "INFO",
    "RUN_ONE_SYMBOL_AT_A_TIME": False, # For PipelineDispatcher
}



class TestPipelineEquivalence(unittest.TestCase):

    def setUp(self):
        self.listing_id = 12345
        self.date_str = "2025-11-01"
        self.time_bucket_seconds = NAIVE_CONFIG["TIME_BUCKET_SECONDS"]

        self.ref_df = create_mock_ref(self.listing_id)
        self.trades_df = create_mock_trades(self.listing_id, self.date_str, self.time_bucket_seconds)
        self.l2_df = create_mock_l2(self.listing_id, self.date_str, self.time_bucket_seconds)
        self.l3_df = create_mock_l3(self.listing_id, self.date_str, self.time_bucket_seconds)
        self.marketstate_df = create_mock_marketstate(self.listing_id, self.date_str, self.time_bucket_seconds)

        self.modules = [
            DenseAnalytics(self.ref_df, NAIVE_CONFIG),
            L2AnalyticsLast(NAIVE_CONFIG["L2_LEVELS"]),
            L2AnalyticsTW(NAIVE_CONFIG["L2_LEVELS"], NAIVE_CONFIG),
            TradeAnalytics(),
            L3Analytics(),
            ExecutionAnalytics(), # Re-added
        ]

        self.pipeline = AnalyticsPipeline(self.modules, NAIVE_CONFIG)

        self.tables_for_pipeline = {
            "trades": self.trades_df,
            "l2": self.l2_df,
            "l3": self.l3_df,
            "marketstate": self.marketstate_df,
        }

        # Create a temporary directory for S3SymbolBatcher output
        self.temp_dir = tempfile.mkdtemp()
        self._create_mock_s3_batcher_output()

        # Calculate naive_result once in setUp
        logging.info("Running naive path in setUp...")
        self.naive_result = self.pipeline.run_on_multi_tables(**self.tables_for_pipeline)
        logging.debug(f"Type of naive_result in setUp: {type(self.naive_result)}")
        logging.info("Naive path in setUp complete.")

    def tearDown(self):
        # Clean up the temporary directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def _create_mock_s3_batcher_output(self):
        """
        Simulates the output of S3SymbolBatcher by writing mock dataframes
        to a temporary directory in the expected partitioned format.
        """
        batch_id = 0 # For simplicity, we'll use a single batch

        for table_name, df in self.tables_for_pipeline.items():
            # Create directory structure: temp_dir/table_name/batch_id=X/
            output_path = Path(self.temp_dir) / table_name / f"batch_id={batch_id}"
            output_path.mkdir(parents=True, exist_ok=True)

            # Write the dataframe as a parquet file
            df.write_parquet(output_path / "part-0.parquet")

    def test_equivalence(self):
        # self.naive_result is already calculated in setUp
        logging.info("Running sophisticated path (simulated via PipelineDispatcher)...")
        mock_out_writer_results = {}
        def mock_out_writer(df: pl.DataFrame, key: str):
            mock_out_writer_results[key] = df

        dispatcher = PipelineDispatcher(self.pipeline, mock_out_writer, NAIVE_CONFIG)
        dispatcher.run_batch(self.tables_for_pipeline)
        logging.info("Sophisticated path complete.")

        sophisticated_result = mock_out_writer_results.get("batch")

        self.assertIsNotNone(sophisticated_result, "Sophisticated path did not produce a result.")
        pl.testing.assert_frame_equal(self.naive_result, sophisticated_result, check_row_order=False)
        logging.info("Both paths produced equivalent results.")

    @patch('bmll.time_series.query')
    def test_s3_batcher_equivalence(self, mock_bmll_query):
        logging.info("Running S3Batcher-driven path...")

        # Mock bmll.time_series.query to return an empty DataFrame
        # This is needed for SymbolSizeEstimator, but we're not actually using BMLL data
        mock_bmll_query.return_value = pl.DataFrame({
            "ObjectId": [], "TradeCount|Lit": []
        }, schema={"ObjectId": pl.Int64, "TradeCount|Lit": pl.Int64}).to_pandas()

        # Create dummy S3 file lists (they won't be read, as we're mocking the output)
        s3_file_lists = {
            "trades": ["s3://dummy/trades.parquet"],
            "l2": ["s3://dummy/l2.parquet"],
            "l3": ["s3://dummy/l3.parquet"],
            "marketstate": ["s3://dummy/marketstate.parquet"],
        }
        transform_fns = {name: lambda x: x for name in s3_file_lists.keys()}
        
        # Mock get_universe for SymbolSizeEstimator
        def mock_get_universe(date):
            return pl.DataFrame({SYMBOL_COL: [self.listing_id], "mic": ["XAMS"]})

        # Instantiate SymbolSizeEstimator and HeuristicBatchingStrategy
        estimator = SymbolSizeEstimator(self.date_str, mock_get_universe)
        max_rows_per_table = {name: 1_000_000 for name in s3_file_lists.keys()} # Dummy max rows
        batching_strategy = HeuristicBatchingStrategy(estimator, max_rows_per_table)

        # Instantiate S3SymbolBatcher
        s3_batcher = S3SymbolBatcher(
            s3_file_lists=s3_file_lists,
            transform_fns=transform_fns,
            batching_strategy=batching_strategy,
            temp_dir=self.temp_dir,
            storage_options={},
            date=self.date_str,
            get_universe=mock_get_universe,
        )

        # Manually simulate the output of S3SymbolBatcher.process()
        # by reading from the pre-populated temp_dir
        s3_batcher_output_tables = {}
        batch_id = 0 # We only have one batch in our mock setup
        for table_name in self.tables_for_pipeline.keys():
            path = Path(self.temp_dir) / table_name / f"batch_id={batch_id}"
            if path.exists():
                # Read the local parquet shards
                lf = pl.scan_parquet(str(path / "*.parquet"))
                s3_batcher_output_tables[table_name] = lf.collect()
            else:
                s3_batcher_output_tables[table_name] = pl.DataFrame()

        # Run the pipeline with the S3Batcher's simulated output
        s3_batcher_result = self.pipeline.run_on_multi_tables(**s3_batcher_output_tables)
        logging.info("S3Batcher-driven path complete.")

        self.assertIsNotNone(s3_batcher_result, "S3Batcher path did not produce a result.")
        pl.testing.assert_frame_equal(self.naive_result, s3_batcher_result, check_row_order=False)
        logging.info("Naive and S3Batcher paths produced equivalent results.")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    unittest.main()