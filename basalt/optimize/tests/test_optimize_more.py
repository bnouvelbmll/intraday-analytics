from __future__ import annotations

import pytest

from basalt.optimize.core import _load_callable, _parse_path, sample_params


def test_parse_path_with_indexes():
    assert _parse_path("a.b[2].c") == ["a", "b", 2, "c"]


def test_load_callable_errors():
    with pytest.raises(ValueError, match="format module:function"):
        _load_callable("bad")
    with pytest.raises(ValueError, match="Invalid callable target"):
        _load_callable("basalt.optimize.core:not_found")


def test_sample_params_unknown_type():
    with pytest.raises(ValueError, match="Unknown param type"):
        sample_params({"params": {"x": {"type": "unknown"}}}, rng=__import__("random").Random(1))
