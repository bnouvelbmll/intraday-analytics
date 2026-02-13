from __future__ import annotations

import time

import polars as pl

from basalt.visualization.background import BackgroundFrameLoader


def _slow_loader(payload):
    delay = float(payload.get("delay", 0.0))
    time.sleep(delay)
    value = int(payload.get("value", 0))
    return pl.DataFrame({"v": [value]})


def test_background_loader_returns_dataframe():
    loader = BackgroundFrameLoader(_slow_loader)
    loader.submit("req1", {"value": 7, "delay": 0.01})
    for _ in range(60):
        res = loader.poll()
        if res is not None and res.status == "ok":
            break
        time.sleep(0.01)
    df = loader.consume_dataframe()
    assert df is not None
    assert df["v"].to_list() == [7]


def test_background_loader_cancels_previous_request():
    loader = BackgroundFrameLoader(_slow_loader)
    loader.submit("req1", {"value": 1, "delay": 0.3})
    loader.submit("req2", {"value": 2, "delay": 0.01})
    for _ in range(120):
        res = loader.poll()
        if res is not None and res.request_id == "req2" and res.status == "ok":
            break
        time.sleep(0.01)
    df = loader.consume_dataframe()
    assert df is not None
    assert df["v"].to_list() == [2]
