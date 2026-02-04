import os
import shutil
import tempfile
import unittest
import importlib
from unittest.mock import MagicMock, patch

import polars as pl

from intraday_analytics.execution import run_metrics_pipeline, ProcessInterval
from intraday_analytics.configuration import AnalyticsConfig, PassConfig


FIXTURE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "sample_fixture")
)

TABLE_TO_FILE = {
    "trades": "trades-plus.parquet",
    "l2": "l2.parquet",
    "l3": "l3.parquet",
    "marketstate": "market-state.parquet",
    "cbbo": "cbbo.parquet",
}


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


def fixture_universe(_date):
    return pl.read_parquet(os.path.join(FIXTURE_DIR, "reference.parquet"))


def fixture_files(_sd, _ed, _mics, table_name, exclude_weekends=True):
    fname = TABLE_TO_FILE.get(table_name)
    if not fname:
        return []
    return [os.path.join(FIXTURE_DIR, fname)]


def _run_with_fixtures(config: AnalyticsConfig, get_pipeline=None):
    with patch("intraday_analytics.execution.ProcessInterval", SyncProcessInterval), patch(
        "intraday_analytics.execution.get_files_for_date_range",
        side_effect=fixture_files,
    ), patch(
        "intraday_analytics.execution.get_final_s3_path",
        side_effect=lambda sd, ed, cfg, pass_name: os.path.join(
            cfg.TEMP_DIR, f"final_{pass_name}_{sd.date()}_{ed.date()}.parquet"
        ),
    ), patch(
        "intraday_analytics.process.get_final_s3_path",
        side_effect=lambda sd, ed, cfg, pass_name: os.path.join(
            cfg.TEMP_DIR, f"final_{pass_name}_{sd.date()}_{ed.date()}.parquet"
        ),
    ):
        run_metrics_pipeline(
            config=config,
            get_universe=fixture_universe,
            get_pipeline=get_pipeline,
        )


class TestDemoAndMainFixtures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import sys

        sys.modules.setdefault("bmll2", MagicMock())
        sys.modules.setdefault("bmll", MagicMock())
        sys.modules.setdefault("bmll.reference", MagicMock())

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_demo_01_ohlcv(self):
        demo01 = importlib.import_module("demo.01_ohlcv_bars")

        cfg = AnalyticsConfig(
            **{
                **demo01.USER_CONFIG,
                "START_DATE": "2025-01-02",
                "END_DATE": "2025-01-02",
                "TEMP_DIR": self.temp_dir,
                "PREPARE_DATA_MODE": "naive",
                "CLEAN_UP_TEMP_DIR": False,
                "BATCH_FREQ": None,
            }
        )
        _run_with_fixtures(cfg)

    def test_demo_03_custom_metric(self):
        demo03 = importlib.import_module("demo.03_custom_metric")

        cfg = AnalyticsConfig(
            **{
                **demo03.USER_CONFIG,
                "START_DATE": "2025-01-02",
                "END_DATE": "2025-01-02",
                "TEMP_DIR": self.temp_dir,
                "PREPARE_DATA_MODE": "naive",
                "CLEAN_UP_TEMP_DIR": False,
                "BATCH_FREQ": None,
            }
        )
        _run_with_fixtures(cfg, get_pipeline=demo03.get_pipeline)

    def test_demo_04_market_impact(self):
        demo04 = importlib.import_module("demo.04_market_impact")

        cfg = AnalyticsConfig(
            **{
                **demo04.USER_CONFIG,
                "START_DATE": "2025-01-02",
                "END_DATE": "2025-01-02",
                "TEMP_DIR": self.temp_dir,
                "PREPARE_DATA_MODE": "naive",
                "CLEAN_UP_TEMP_DIR": False,
                "BATCH_FREQ": None,
            }
        )
        _run_with_fixtures(cfg)

    def test_main_config(self):
        import main as main_script

        cfg = AnalyticsConfig(
            **{
                **main_script.USER_CONFIG,
                "START_DATE": "2025-01-02",
                "END_DATE": "2025-01-02",
                "TEMP_DIR": self.temp_dir,
                "PREPARE_DATA_MODE": "naive",
                "CLEAN_UP_TEMP_DIR": False,
                "BATCH_FREQ": None,
                "EAGER_EXECUTION": True,
                "NUM_WORKERS": 1,
            }
        )
        _run_with_fixtures(cfg)
