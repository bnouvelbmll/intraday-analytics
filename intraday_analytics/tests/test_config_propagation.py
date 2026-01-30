import unittest
import polars as pl
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch, ANY
from intraday_analytics.execution import ProcessInterval, process_batch_task
from intraday_analytics.configuration import AnalyticsConfig, PrepareDataMode

class TestConfigPropagation(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        import sys
        if "bmll2" not in sys.modules:
            sys.modules["bmll2"] = MagicMock()

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.custom_table_name = "my_custom_table"

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_config_propagation_to_process_pool(self):
        """
        Verifies that config is correctly passed to process_batch_task via ProcessPoolExecutor.
        We run ProcessInterval.run() synchronously in the test process to intercept the call.
        """
        # Create a config with a non-default TABLES_TO_LOAD
        config = AnalyticsConfig(
            START_DATE="2025-01-01",
            END_DATE="2025-01-01", # Single day
            TEMP_DIR=self.temp_dir,
            PREPARE_DATA_MODE=PrepareDataMode.NAIVE.value,
            BATCHING_STRATEGY="heuristic",
            MAX_ROWS_PER_TABLE={"trades": 1000},
            CLEAN_UP_TEMP_DIR=False,
            FINAL_OUTPUT_PATH_TEMPLATE=os.path.join(self.temp_dir, "final.parquet"),
            TABLES_TO_LOAD=[self.custom_table_name], # <--- The custom config
            NUM_WORKERS=1
        )

        # Mock dependencies
        mock_get_pipeline = MagicMock()
        mock_get_universe = MagicMock()
        mock_get_universe.return_value = pl.DataFrame({"ListingId": ["A"], "MIC": ["X"]})

        # We need to patch:
        # 1. preload: to avoid actual data loading
        # 2. SymbolBatcherStreaming: to avoid actual batching logic
        # 3. glob: to simulate finding batch files
        # 4. ProcessPoolExecutor: to capture the submit call
        # 5. aggregate_and_write_final_output: to avoid final step
        
        mock_table = MagicMock(timestamp_col="ts")
        mock_table.name = self.custom_table_name

        with patch('intraday_analytics.execution.preload') as mock_preload, \
             patch('intraday_analytics.execution.SymbolBatcherStreaming') as mock_sbs, \
             patch('intraday_analytics.execution.glob.glob') as mock_glob, \
             patch('intraday_analytics.execution.ProcessPoolExecutor') as mock_executor_cls, \
             patch('intraday_analytics.execution.as_completed') as mock_as_completed, \
             patch('intraday_analytics.execution.aggregate_and_write_final_output'), \
             patch('intraday_analytics.execution.ALL_TABLES', {self.custom_table_name: mock_table}):

            # Mock preload
            mock_preload.return_value = {
                self.custom_table_name: pl.DataFrame({
                    "ListingId": [], 
                    "ts": []
                })
            }
            
            # Mock SBS
            mock_sbs_instance = MagicMock()
            mock_sbs_instance.stream_batches.return_value = [] # No batches written by SBS
            mock_sbs.return_value = mock_sbs_instance
            
            # Mock glob to return one batch file
            # This simulates that a batch file exists (even if SBS didn't write it, we pretend it's there)
            mock_glob.return_value = [os.path.join(self.temp_dir, "batch-trades-0.parquet")]
            
            # Mock Executor
            mock_executor = MagicMock()
            mock_executor_cls.return_value.__enter__.return_value = mock_executor
            
            # Mock Future
            mock_future = MagicMock()
            mock_executor.submit.return_value = mock_future
            
            # Mock as_completed to return the list of futures immediately
            mock_as_completed.side_effect = lambda futures: futures
            
            # Instantiate ProcessInterval directly
            p = ProcessInterval(
                sd=pd.Timestamp("2025-01-01"),
                ed=pd.Timestamp("2025-01-01"),
                config=config,
                get_pipeline=mock_get_pipeline,
                get_universe=mock_get_universe
            )
            
            # Run it synchronously
            p.run()
            
            # Verify that executor.submit was called with the correct config
            # Call signature: submit(process_batch_task, i, temp_dir, current_date, config, pipe)
            
            # We expect one call because glob returned one file (batch 0)
            self.assertTrue(mock_executor.submit.called, "ProcessPoolExecutor.submit was not called")
            
            args, _ = mock_executor.submit.call_args
            
            # args[0] is the function (process_batch_task)
            # args[1] is i (0)
            # args[2] is temp_dir
            # args[3] is current_date
            # args[4] is config <--- This is what we want to check
            
            passed_func = args[0]
            passed_config = args[4]
            
            self.assertEqual(passed_func, process_batch_task)
            self.assertEqual(passed_config.TABLES_TO_LOAD, [self.custom_table_name])
            print("Config correctly propagated to process_batch_task!")

import pandas as pd
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    unittest.main()