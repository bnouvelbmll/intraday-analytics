from .configuration import AnalyticsConfig

# Create the default configuration
_config_model = AnalyticsConfig()

# Export as a dictionary for compatibility
DEFAULT_CONFIG = _config_model.to_dict()
