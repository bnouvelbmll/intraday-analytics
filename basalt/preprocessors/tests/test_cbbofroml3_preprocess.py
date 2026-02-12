from datetime import datetime

import polars as pl

from basalt.preprocessors.cbbofroml3_preprocess import (
    CBBOfromL3Preprocess,
    CBBOfromL3PreprocessConfig,
)


def _make_ref():
    return pl.DataFrame(
        {
            "ListingId": [1, 2],
            "InstrumentId": [10, 10],
            "MIC": ["X", "Y"],
            "IsPrimary": [True, False],
        }
    )


def _make_l3():
    ts = datetime(2025, 1, 1, 9, 0, 0)
    return pl.DataFrame(
        {
            "ListingId": [1, 2, 1, 2],
            "EventTimestamp": [ts, ts, ts, ts],
            "LobAction": [2, 2, 2, 2],
            "Side": [1, 1, 2, 2],
            "Price": [100.0, 100.0, 101.0, 102.0],
            "Size": [10.0, 7.0, 5.0, 6.0],
            "OldPrice": [None, None, None, None],
            "OldSize": [None, None, None, None],
            "MarketState": [
                "CONTINUOUS_TRADING",
                "CONTINUOUS_TRADING",
                "CONTINUOUS_TRADING",
                "CONTINUOUS_TRADING",
            ],
        }
    )


def test_cbbofroml3_preprocess_combines_l3_books():
    ref = _make_ref()
    l3 = _make_l3()
    cfg = CBBOfromL3PreprocessConfig(top_n=2, time_index="event")
    module = CBBOfromL3Preprocess(ref, cfg)
    module.l3 = l3
    out = module.compute().collect()

    assert out.shape[0] == 1
    row = out.row(0, named=True)
    assert row["InstrumentId"] == 10
    assert row["BidPrice1"] == 100.0
    assert row["BidQuantity1"] == 17.0
    assert row["AskPrice1"] == 101.0
    assert row["AskQuantity1"] == 5.0
    assert row["AskPrice2"] == 102.0
    assert row["AskQuantity2"] == 6.0


def test_cbbofroml3_preprocess_primary_listing_index():
    ref = _make_ref()
    l3 = _make_l3()
    cfg = CBBOfromL3PreprocessConfig(
        top_n=1,
        time_index="event",
        index_by="PrimaryListingId",
    )
    module = CBBOfromL3Preprocess(ref, cfg)
    module.l3 = l3
    out = module.compute().collect()
    row = out.row(0, named=True)
    assert row["PrimaryListingId"] == 1
