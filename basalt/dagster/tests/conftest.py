import importlib.util
import pytest


if importlib.util.find_spec("dagster") is None:
    pytest.skip("dagster is not installed", allow_module_level=True)
