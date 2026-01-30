from .configuration import AnalyticsConfig

# Create the default configuration
_config_model = AnalyticsConfig()
_config_model.validate()

# Export as a dictionary for compatibility
DEFAULT_CONFIG = _config_model.to_dict()
