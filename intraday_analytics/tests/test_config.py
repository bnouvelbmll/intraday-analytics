import unittest
from intraday_analytics.configuration import AnalyticsConfig, PrepareDataMode

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

if __name__ == '__main__':
    unittest.main()