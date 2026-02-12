from __future__ import annotations

import basalt.dense_analytics as compat


def test_dense_compat_import_exposes_dense_analytics():
    assert hasattr(compat, "DenseAnalytics")
