import datetime as dt

import polars as pl

from basalt.analytics_base import BaseAnalytics


class _DummyAnalytics(BaseAnalytics):
    REQUIRES = []

    def __init__(self):
        super().__init__("dummy")

    def compute(self, **kwargs) -> pl.LazyFrame:
        return self.df


def test_join_asof_aligns_to_previous_timebucket():
    base = pl.DataFrame(
        {
            "ListingId": [1, 1, 1],
            "TimeBucket": [
                dt.datetime(2025, 1, 1, 9, 0, 0),
                dt.datetime(2025, 1, 1, 9, 1, 0),
                dt.datetime(2025, 1, 1, 9, 2, 0),
            ],
        }
    ).lazy()
    module = _DummyAnalytics()
    module.df = pl.DataFrame(
        {
            "ListingId": [1, 1],
            "TimeBucket": [
                dt.datetime(2025, 1, 1, 9, 0, 0),
                dt.datetime(2025, 1, 1, 9, 2, 0),
            ],
            "metric": [10.0, 30.0],
        }
    ).lazy()

    joined = module.join(base, other_specific_cols={}, use_asof=True).collect()
    assert joined["metric"].to_list() == [10.0, 10.0, 30.0]

