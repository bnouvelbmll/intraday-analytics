from __future__ import annotations

from types import SimpleNamespace

import polars as pl
import pytest

import basalt.pipeline as p
from basalt.configuration import PassConfig


def test_order_modules_for_timeline():
    out = p._order_modules_for_timeline(["trade", "dense", "external_events", "l2"])
    assert out[:2] == ["dense", "external_events"]


def test_resolve_module_inputs_variants():
    module = SimpleNamespace(name="trade", REQUIRES=["l2"])
    pass_cfg = PassConfig(name="p1", module_inputs={"trade": "x"})
    assert p._resolve_module_inputs(pass_cfg, module) == {"l2": "x"}

    module2 = SimpleNamespace(name="trade", REQUIRES=["l2", "reference"])
    pass_cfg2 = PassConfig(name="p1", module_inputs={"trade": {"l2": "a", "reference": "b"}})
    assert p._resolve_module_inputs(pass_cfg2, module2) == {"l2": "a", "reference": "b"}

    pass_cfg3 = SimpleNamespace(module_inputs={"trade": 1})
    with pytest.raises(ValueError, match="string or mapping"):
        p._resolve_module_inputs(pass_cfg3, module)


def test_to_df_and_reference_input_df(monkeypatch):
    cfg = SimpleNamespace()
    df = pl.DataFrame({"x": [1]})
    assert p._to_df(df, cfg).height == 1
    assert p._to_df(df.lazy(), cfg).height == 1
    assert p._to_df(None, cfg) is None

    class _M:
        name = "m"
        REQUIRES = ["l2"]

    pass_cfg = PassConfig(name="p1", module_inputs={"m": "ctx"})
    out = p._reference_input_df(
        pass_cfg,
        [_M()],
        tables_for_sym={},
        context={"ctx": df},
        config=cfg,
    )
    assert out is not None and out.height == 1


def test_validate_pass_expectations_raise():
    pass_cfg = PassConfig(name="p1")
    pass_cfg.pass_expectations.preserve_rows = True
    pass_cfg.pass_expectations.action = "raise"
    ref = pl.DataFrame({"ListingId": [1, 2]})
    out = pl.DataFrame({"ListingId": [1]})
    with pytest.raises(AssertionError, match="preserve_rows failed"):
        p._validate_pass_expectations(pass_cfg, out, ref)
