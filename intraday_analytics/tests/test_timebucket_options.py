import unittest
import datetime as dt
import polars as pl

from intraday_analytics.tables import TradesPlusTable


class TestTimeBucketOptions(unittest.TestCase):
    def test_timebucket_anchor_and_closed(self):
        table = TradesPlusTable()
        ref = pl.DataFrame({"ListingId": [1]})
        nanoseconds = int(60 * 1e9)

        ts1 = dt.datetime(2025, 1, 1, 0, 0, 30)
        ts2 = dt.datetime(2025, 1, 1, 0, 1, 0)

        lf = pl.DataFrame(
            {
                "ListingId": [1, 1],
                "TradeTimestamp": [ts1, ts2],
                "TradeNotional": [100.0, 110.0],
                "TradeNotionalEUR": [100.0, 110.0],
                "Size": [10.0, 10.0],
            }
        ).lazy()

        transform_end = table.get_transform_fn(
            ref, nanoseconds, time_bucket_anchor="end", time_bucket_closed="right"
        )
        df_end = transform_end(lf).collect().sort("TradeTimestamp")

        self.assertEqual(df_end["TimeBucket"][0], dt.datetime(2025, 1, 1, 0, 1, 0))
        self.assertEqual(df_end["TimeBucket"][1], dt.datetime(2025, 1, 1, 0, 1, 0))

        transform_start = table.get_transform_fn(
            ref, nanoseconds, time_bucket_anchor="start", time_bucket_closed="left"
        )
        df_start = transform_start(lf).collect().sort("TradeTimestamp")

        self.assertEqual(df_start["TimeBucket"][0], dt.datetime(2025, 1, 1, 0, 0, 0))
        self.assertEqual(df_start["TimeBucket"][1], dt.datetime(2025, 1, 1, 0, 1, 0))

