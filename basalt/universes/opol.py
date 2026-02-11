import bmll.reference
import polars as pl


def get_universe(date, value):
    if not value:
        raise ValueError("Universe OPOL requires --universe opol=<OPOL>.")
    universe_query = bmll.reference.query(
        object_type="Instrument",
        start_date=date,
        OPOL=value,
        IsAlive=True,
    )
    return pl.DataFrame(universe_query)
