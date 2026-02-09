import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import polars as pl
import pandas as pd

from intraday_analytics.execution import run_metrics_pipeline, ProcessInterval
from intraday_analytics.configuration import (
    AnalyticsConfig,
    PassConfig,
    OutputTarget,
    OutputType,
)


# Helper class to run ProcessInterval synchronously
class SyncProcessInterval(ProcessInterval):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._exitcode = None

    def start(self):
        try:
            self.run()
            self._exitcode = 0
        except Exception:
            self._exitcode = 1
            raise

    def join(self):
        pass

    @property
    def exitcode(self):
        return self._exitcode


# Helper functions for pickling
def mock_get_universe(date):
    return pl.DataFrame(
        {
            "ListingId": [1],
            "MIC": ["X"],
            "ISIN": ["I"],
            "Ticker": ["ABC"],
            "CurrencyCode": ["EUR"],
            "IsPrimary": [True],
            "IsAlive": [True],
        }
    )


class TestOutputTargets(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import sys

        sys.modules["bmll2"] = MagicMock()

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.source_dir = tempfile.mkdtemp()
        self.trades_file = os.path.join(self.source_dir, "trades.parquet")
        self.marketstate_file = os.path.join(self.source_dir, "marketstate.parquet")

        pl.DataFrame(
            {
                "ListingId": [1],
                "TradeTimestamp": [pd.Timestamp("2025-01-01 10:00:00").value],
                "Price": [10.0],
                "Size": [100],
                "Classification": ["LIT_CONTINUOUS"],
                "LocalPrice": [10.0],
                "MarketState": ["OPEN"],
                "Ticker": ["ABC"],
                "MIC": ["X"],
                "PricePoint": [0.5],
                "BMLLParticipantType": ["RETAIL"],
                "AggressorSide": [1],
                "TradeNotional": [1000.0],
                "TradeNotionalEUR": [1000.0],
            }
        ).write_parquet(self.trades_file)

        pl.DataFrame(
            {
                "ListingId": [1, 1],
                "TimestampNanoseconds": [
                    pd.Timestamp("2025-01-01 10:00:00").value,
                    pd.Timestamp("2025-01-01 10:05:00").value,
                ],
                "MarketState": ["CONTINUOUS_TRADING", "CLOSED"],
            }
        ).write_parquet(self.marketstate_file)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        shutil.rmtree(self.source_dir)

    def _run_pipeline(self, config):
        def mock_files(sd, ed, mics, table_name, **kwargs):
            if table_name == "trades":
                return [self.trades_file]
            if table_name == "marketstate":
                return [self.marketstate_file]
            return []

        def mock_submit(fn, *args, **kwargs):
            fn(*args, **kwargs)
            mock_future = MagicMock()
            mock_future.result.return_value = True
            return mock_future

        with patch(
            "intraday_analytics.execution.ProcessInterval",
            side_effect=SyncProcessInterval,
        ), patch(
            "intraday_analytics.execution.as_completed",
            side_effect=lambda futures: futures,
        ), patch(
            "intraday_analytics.execution.get_files_for_date_range"
        ) as mock_get_files, patch(
            "intraday_analytics.execution.ProcessPoolExecutor"
        ) as mock_pool:
            mock_get_files.side_effect = mock_files
            mock_pool.return_value.__enter__.return_value.submit.side_effect = (
                mock_submit
            )
            run_metrics_pipeline(config=config, get_universe=mock_get_universe)

    def test_multi_pass_delta_output(self):
        try:
            import deltalake  # noqa: F401
        except Exception:
            self.skipTest("deltalake not installed")

        delta_template = os.path.join(
            self.temp_dir, "delta_{datasetname}_{start_date}_{end_date}"
        )
        config = AnalyticsConfig(
            START_DATE="2025-01-01",
            END_DATE="2025-01-01",
            TEMP_DIR=self.temp_dir,
            PASSES=[
                PassConfig(name="pass1", modules=["dense", "trade"]),
                PassConfig(name="pass2", modules=["trade"]),
            ],
            OUTPUT_TARGET=OutputTarget(
                type=OutputType.DELTA, path_template=delta_template
            ),
            CLEAN_UP_TEMP_DIR=False,
            BATCH_FREQ=None,
        )

        self._run_pipeline(config)

        for pass_name in ("pass1", "pass2"):
            path = delta_template.format(
                datasetname=f"{config.DATASETNAME}_{pass_name}",
                start_date="2025-01-01",
                end_date="2025-01-01",
                bucket="",
                prefix="",
                universe="all",
            )
            self.assertTrue(os.path.exists(os.path.join(path, "_delta_log")))

    def test_multi_pass_sqlite_output(self):
        try:
            import sqlalchemy  # noqa: F401
        except Exception:
            self.skipTest("sqlalchemy not installed")

        db_path = os.path.join(self.temp_dir, "analytics.db")
        conn = f"sqlite:///{db_path}"

        config = AnalyticsConfig(
            START_DATE="2025-01-01",
            END_DATE="2025-01-01",
            TEMP_DIR=self.temp_dir,
            PASSES=[
                PassConfig(
                    name="pass1",
                    modules=["dense", "trade"],
                    output=OutputTarget(
                        type=OutputType.SQL,
                        sql_connection=conn,
                        sql_table="pass1_metrics",
                        sql_if_exists="replace",
                    ),
                ),
                PassConfig(
                    name="pass2",
                    modules=["trade"],
                    output=OutputTarget(
                        type=OutputType.SQL,
                        sql_connection=conn,
                        sql_table="pass2_metrics",
                        sql_if_exists="replace",
                    ),
                ),
            ],
            CLEAN_UP_TEMP_DIR=False,
            BATCH_FREQ=None,
        )

        self._run_pipeline(config)

        from sqlalchemy import create_engine, inspect

        engine = create_engine(conn)
        inspector = inspect(engine)
        self.assertIn("pass1_metrics", inspector.get_table_names())
        self.assertIn("pass2_metrics", inspector.get_table_names())

        from sqlalchemy import text as sql_text

        with engine.connect() as conn_handle:
            res1 = conn_handle.execute(
                sql_text("SELECT COUNT(*) FROM pass1_metrics")
            ).fetchone()[0]
            res2 = conn_handle.execute(
                sql_text("SELECT COUNT(*) FROM pass2_metrics")
            ).fetchone()[0]
            self.assertGreater(res1, 0)
            self.assertGreater(res2, 0)


if __name__ == "__main__":
    unittest.main()
