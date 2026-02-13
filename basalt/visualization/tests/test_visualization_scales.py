from __future__ import annotations

import numpy as np
import pandas as pd

from basalt.visualization.scales import choose_scale, transform_series


def test_choose_scale_prefers_sgnlog_for_mixed_sign_wide_range():
    s = pd.Series([-10_000.0, -100.0, -1.0, 0.0, 1.0, 100.0, 20_000.0])
    d = choose_scale(s)
    assert d.selected == "sgnlog"


def test_choose_scale_prefers_sqrt_for_positive_heavy_tail():
    s = pd.Series([1.0] * 100 + [10_000.0, 20_000.0, 50_000.0])
    d = choose_scale(s)
    assert d.selected == "sqrt"


def test_transform_series_percentile_between_zero_one():
    s = pd.Series([1.0, 2.0, 3.0, 4.0])
    t = transform_series(s, "percentile")
    assert float(t.min()) >= 0.0
    assert float(t.max()) <= 1.0


def test_transform_series_sgnlog_is_odd():
    s = pd.Series([-10.0, -1.0, 0.0, 1.0, 10.0])
    t = transform_series(s, "sgnlog").to_numpy()
    assert np.isclose(t[0], -t[-1])
    assert np.isclose(t[1], -t[-2])
