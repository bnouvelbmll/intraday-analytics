import unittest
from pydantic import ValidationError
from intraday_analytics.configuration import (
    AnalyticsConfig,
    PassConfig,
    PrepareDataMode,
)
from intraday_analytics.analytics.dense import DenseAnalyticsConfig
from intraday_analytics.analytics.l2 import L2AnalyticsConfig


class TestConfiguration(unittest.TestCase):
    def test_default_config_valid(self):
        """Ensure the default configuration is valid."""
        config = AnalyticsConfig(PASSES=[PassConfig(name="default")])
        self.assertEqual(config.PREPARE_DATA_MODE, PrepareDataMode.S3_SHREDDING)
        self.assertEqual(config.PASSES[0].time_bucket_seconds, 60)

    def test_invalid_prepare_data_mode(self):
        """Ensure invalid PREPARE_DATA_MODE raises ValidationError."""
        with self.assertRaises(ValidationError):
            AnalyticsConfig(PREPARE_DATA_MODE="invalid_mode")

    def test_valid_prepare_data_mode_enum(self):
        """Ensure valid PREPARE_DATA_MODE enum works (by value)."""
        config = AnalyticsConfig(PREPARE_DATA_MODE=PrepareDataMode.NAIVE)
        self.assertEqual(config.PREPARE_DATA_MODE, "naive")

    def test_valid_prepare_data_mode_string(self):
        """Ensure valid PREPARE_DATA_MODE string works."""
        config = AnalyticsConfig(PREPARE_DATA_MODE="naive")
        self.assertEqual(config.PREPARE_DATA_MODE, "naive")

    def test_max_rows_per_table_validation(self):
        """Ensure MAX_ROWS_PER_TABLE is a dict."""
        with self.assertRaises(ValidationError):
            AnalyticsConfig(MAX_ROWS_PER_TABLE="not_a_dict")

    def test_nested_config_from_dict(self):
        """
        Tests that AnalyticsConfig correctly initializes nested models
        from dictionaries and propagates global settings.
        """
        config_data = {
            "PASSES": [
                {
                    "name": "pass1",
                    "time_bucket_seconds": 30,
                    "dense_analytics": {
                        "mode": "uniform",
                        "time_interval": ["08:00", "16:00"],
                    },
                    "l2_analytics": {"levels": 5},
                }
            ]
        }

        config = AnalyticsConfig(**config_data)
        pass_config = config.PASSES[0]

        # Check that the dict was converted to a model instance
        self.assertIsInstance(pass_config.dense_analytics, DenseAnalyticsConfig)
        self.assertIsInstance(pass_config.l2_analytics, L2AnalyticsConfig)

        # Check that values from the dict were set correctly
        self.assertEqual(pass_config.dense_analytics.mode, "uniform")
        self.assertEqual(pass_config.l2_analytics.levels, 5)

        # Check that the global setting was propagated by the model_validator
        self.assertEqual(pass_config.dense_analytics.time_bucket_seconds, 30)
        self.assertEqual(pass_config.l2_analytics.time_bucket_seconds, 30)

    def test_nested_config_from_instance(self):
        """
        Tests that AnalyticsConfig works correctly when nested configs are
        already model instances.
        """
        config_data = {
            "PASSES": [
                {
                    "name": "pass1",
                    "time_bucket_seconds": 10,
                    "dense_analytics": DenseAnalyticsConfig(mode="uniform"),
                }
            ]
        }

        config = AnalyticsConfig(**config_data)
        pass_config = config.PASSES[0]
        self.assertIsInstance(pass_config.dense_analytics, DenseAnalyticsConfig)
        self.assertEqual(pass_config.dense_analytics.mode, "uniform")
        self.assertEqual(pass_config.dense_analytics.time_bucket_seconds, 10)


if __name__ == "__main__":
    unittest.main()
