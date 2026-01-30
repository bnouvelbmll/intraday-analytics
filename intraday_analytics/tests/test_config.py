import unittest
from intraday_analytics.configuration import AnalyticsConfig, PrepareDataMode
from intraday_analytics.metrics.dense import DenseAnalyticsConfig
from intraday_analytics.metrics.l2 import L2AnalyticsConfig


class TestConfiguration(unittest.TestCase):
    def test_default_config_valid(self):
        """Ensure the default configuration is valid."""
        config = AnalyticsConfig()
        config.validate()
        self.assertEqual(config.PREPARE_DATA_MODE, PrepareDataMode.S3_SHREDDING.value)
        self.assertEqual(config.TIME_BUCKET_SECONDS, 60)

    def test_invalid_prepare_data_mode(self):
        """Ensure invalid PREPARE_DATA_MODE raises ValueError."""
        config = AnalyticsConfig(PREPARE_DATA_MODE="invalid_mode")
        with self.assertRaises(ValueError):
            config.validate()

    def test_valid_prepare_data_mode_enum(self):
        """Ensure valid PREPARE_DATA_MODE enum works (by value)."""
        config = AnalyticsConfig(PREPARE_DATA_MODE=PrepareDataMode.NAIVE.value)
        config.validate()
        self.assertEqual(config.PREPARE_DATA_MODE, "naive")

    def test_valid_prepare_data_mode_string(self):
        """Ensure valid PREPARE_DATA_MODE string works."""
        config = AnalyticsConfig(PREPARE_DATA_MODE="naive")
        config.validate()
        self.assertEqual(config.PREPARE_DATA_MODE, "naive")

    def test_max_rows_per_table_validation(self):
        """Ensure MAX_ROWS_PER_TABLE is a dict."""
        config = AnalyticsConfig(MAX_ROWS_PER_TABLE="not_a_dict")
        with self.assertRaises(ValueError):
            config.validate()

    def test_nested_config_from_dict(self):
        """
        Tests that AnalyticsConfig correctly initializes nested dataclasses
        from dictionaries and propagates global settings.
        """
        config_data = {
            "TIME_BUCKET_SECONDS": 30,
            "dense_analytics": {"mode": "uniform", "time_interval": ["08:00", "16:00"]},
            "l2_analytics": {"levels": 5},
        }

        config = AnalyticsConfig(**config_data)

        # Check that the dict was converted to a dataclass instance
        self.assertIsInstance(config.dense_analytics, DenseAnalyticsConfig)
        self.assertIsInstance(config.l2_analytics, L2AnalyticsConfig)

        # Check that values from the dict were set correctly
        self.assertEqual(config.dense_analytics.mode, "uniform")
        self.assertEqual(config.l2_analytics.levels, 5)

        # Check that the global setting was propagated by __post_init__
        self.assertEqual(config.dense_analytics.time_bucket_seconds, 30)
        self.assertEqual(config.l2_analytics.time_bucket_seconds, 30)

    def test_nested_config_from_instance(self):
        """
        Tests that AnalyticsConfig works correctly when nested configs are
        already dataclass instances.
        """
        config_data = {
            "TIME_BUCKET_SECONDS": 10,
            "dense_analytics": DenseAnalyticsConfig(mode="uniform"),
        }

        config = AnalyticsConfig(**config_data)
        self.assertIsInstance(config.dense_analytics, DenseAnalyticsConfig)
        self.assertEqual(config.dense_analytics.mode, "uniform")
        self.assertEqual(config.dense_analytics.time_bucket_seconds, 10)


if __name__ == "__main__":
    unittest.main()
