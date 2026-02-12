from datetime import datetime, timedelta

import polars as pl

from basalt.rebuilder import compile_l3_deltas, rebuild_l2_from_l3


def test_compile_l3_deltas_event_index():
    t0 = datetime(2025, 1, 1, 9, 0, 0)
    t1 = t0 + timedelta(seconds=1)
    l3 = pl.DataFrame(
        {
            "ListingId": [1, 1, 1],
            "EventTimestamp": [t0, t0, t1],
            "LobAction": [2, 2, 3],
            "Side": [1, 2, 1],
            "Price": [100.0, 101.0, None],
            "Size": [10.0, 8.0, None],
            "OldPrice": [None, None, 100.0],
            "OldSize": [None, None, 4.0],
        }
    )
    out = compile_l3_deltas(
        l3,
        group_cols=["ListingId"],
        time_index="event",
    ).collect()

    bid_t0 = out.filter(
        (pl.col("ListingId") == 1)
        & (pl.col("__event_time") == t0)
        & (pl.col("__side") == 1)
        & (pl.col("__price") == 100.0)
    )
    bid_t1 = out.filter(
        (pl.col("ListingId") == 1)
        & (pl.col("__event_time") == t1)
        & (pl.col("__side") == 1)
        & (pl.col("__price") == 100.0)
    )
    assert bid_t0["DeltaSize"][0] == 10.0
    assert bid_t1["DeltaSize"][0] == -4.0


def test_rebuild_l2_from_l3_combines_levels():
    t0 = datetime(2025, 1, 1, 9, 0, 0)
    l3 = pl.DataFrame(
        {
            "InstrumentId": [10, 10, 10, 10],
            "EventTimestamp": [t0, t0, t0, t0],
            "LobAction": [2, 2, 2, 2],
            "Side": [1, 1, 2, 2],
            "Price": [100.0, 99.0, 101.0, 102.0],
            "Size": [10.0, 5.0, 8.0, 3.0],
            "OldPrice": [None, None, None, None],
            "OldSize": [None, None, None, None],
        }
    )

    out = rebuild_l2_from_l3(
        l3,
        group_cols=["InstrumentId"],
        time_index="event",
        top_n=2,
    ).collect()
    row = out.row(0, named=True)
    assert row["InstrumentId"] == 10
    assert row["BidPrice1"] == 100.0
    assert row["BidQuantity1"] == 10.0
    assert row["BidPrice2"] == 99.0
    assert row["BidQuantity2"] == 5.0
    assert row["AskPrice1"] == 101.0
    assert row["AskQuantity1"] == 8.0
    assert row["AskPrice2"] == 102.0
    assert row["AskQuantity2"] == 3.0
