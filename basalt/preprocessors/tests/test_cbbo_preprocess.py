from datetime import datetime

import polars as pl

from basalt.preprocessors.cbbo_preprocess import CBBOPreprocess, CBBOPreprocessConfig


def _make_ref():
    return pl.DataFrame(
        {
            "ListingId": [1, 2],
            "InstrumentId": [10, 10],
            "MIC": ["X", "Y"],
            "IsPrimary": [True, False],
        }
    )


def _make_l2(marketstate_y="CONTINUOUS_TRADING"):
    ts = datetime(2025, 1, 1, 9, 0, 0)
    return pl.DataFrame(
        {
            "ListingId": [1, 2],
            "InstrumentId": [10, 10],
            "MIC": ["X", "Y"],
            "EventTimestamp": [ts, ts],
            "TimeBucket": [ts, ts],
            "MarketState": ["CONTINUOUS_TRADING", marketstate_y],
            "BidPrice1": [100.0, 100.0],
            "BidQuantity1": [10.0, 7.0],
            "BidNumOrders1": [1, 2],
            "AskPrice1": [101.0, 102.0],
            "AskQuantity1": [5.0, 6.0],
            "AskNumOrders1": [1, 1],
        }
    )


def test_cbbo_preprocess_combines_levels():
    ref = _make_ref()
    l2 = _make_l2()
    cfg = CBBOPreprocessConfig(top_n=2, levels_in=1)
    module = CBBOPreprocess(ref, cfg)
    module.l2 = l2
    out = module.compute().collect()
    assert out.shape[0] == 1
    row = out.row(0, named=True)
    assert row["InstrumentId"] == 10
    assert row["BidPrice1"] == 100.0
    assert row["BidQuantity1"] == 17.0
    assert row["BidNumOrders1"] == 3
    assert row["AskPrice1"] == 101.0
    assert row["AskQuantity1"] == 5.0
    assert row["AskNumOrders1"] == 1
    assert row["AskPrice2"] == 102.0
    assert row["AskQuantity2"] == 6.0
    assert row["AskNumOrders2"] == 1


def test_cbbo_preprocess_marketstate_filter():
    ref = _make_ref()
    l2 = _make_l2(marketstate_y="OPENING_AUCTION")
    cfg = CBBOPreprocessConfig(top_n=1, levels_in=1, market_states=["CONTINUOUS_TRADING"])
    module = CBBOPreprocess(ref, cfg)
    module.l2 = l2
    out = module.compute().collect()
    row = out.row(0, named=True)
    assert row["BidQuantity1"] == 10.0
    assert row["BidNumOrders1"] == 1


def test_cbbo_preprocess_event_time_index():
    ref = _make_ref()
    l2 = _make_l2()
    cfg = CBBOPreprocessConfig(top_n=1, levels_in=1, time_index="event")
    module = CBBOPreprocess(ref, cfg)
    module.l2 = l2
    out = module.compute().collect()
    assert "EventTimestamp" in out.columns
    assert "TimeBucket" not in out.columns
