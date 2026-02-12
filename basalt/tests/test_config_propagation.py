import unittest
import polars as pl
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch, ANY
from basalt.orchestrator import ProcessInterval
from basalt.configuration import (
    AnalyticsConfig,
    PassConfig,
    PrepareDataMode,
)


class MockPipeline:
    def __init__(self):
        self.context = {}


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
            END_DATE="2025-01-01",  # Single day
            TEMP_DIR=self.temp_dir,
            PASSES=[
                PassConfig(
                    name="pass1",
                    modules=["trade"],
                )
            ],
            PREPARE_DATA_MODE=PrepareDataMode.NAIVE.value,
            BATCHING_STRATEGY="heuristic",
            MAX_ROWS_PER_TABLE={"trades": 1000},
            CLEAN_UP_TEMP_DIR=False,
            OUTPUT_TARGET={"path_template": os.path.join(self.temp_dir, "final.parquet")},
            TABLES_TO_LOAD=[self.custom_table_name],  # <--- The custom config
            NUM_WORKERS=1,
        )

        # Mock dependencies
        mock_get_pipeline = MagicMock()
        mock_get_pipeline.return_value = MockPipeline()
        mock_get_universe = MagicMock()
        mock_get_universe.return_value = pl.DataFrame(
            {"ListingId": ["A"], "MIC": ["X"]}
        )

        mock_table = MagicMock(timestamp_col="ts")
        mock_table.name = self.custom_table_name

        with patch("basalt.orchestrator.preload") as mock_preload, patch(
            "basalt.orchestrator.SymbolBatcherStreaming"
        ) as mock_sbs, patch(
            "basalt.orchestrator.glob.glob"
        ) as mock_glob, patch(
            "basalt.orchestrator.process_batch_task"
        ) as mock_process_batch_task, patch(
            "basalt.orchestrator.aggregate_and_write_final_output"
        ), patch(
            "basalt.orchestrator.ALL_TABLES",
            {self.custom_table_name: mock_table},
        ), patch(
            "basalt.utils.ALL_TABLES",
            {self.custom_table_name: mock_table},
        ):
            mock_preload.return_value = {
                self.custom_table_name: pl.DataFrame({"ListingId": [], "ts": []})
            }
            mock_sbs_instance = MagicMock()
            mock_sbs_instance.stream_batches.return_value = [
                {"my_custom_table": pl.DataFrame()}
            ]
            mock_sbs.return_value = mock_sbs_instance
            mock_glob.return_value = [
                os.path.join(self.temp_dir, "batch-my_custom_table-0.parquet")
            ]

            p = ProcessInterval(
                sd=pd.Timestamp("2025-01-01"),
                ed=pd.Timestamp("2025-01-01"),
                config=config,
                pass_config=config.PASSES[0],
                get_pipeline=mock_get_pipeline,
                get_universe=mock_get_universe,
                context_path=os.path.join(self.temp_dir, "context.pkl"),
            )

            p.run()

            self.assertTrue(mock_process_batch_task.called)
            args, _ = mock_process_batch_task.call_args
            passed_config = args[3]
            self.assertEqual(passed_config.TABLES_TO_LOAD, [self.custom_table_name])


import pandas as pd

if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    unittest.main()
