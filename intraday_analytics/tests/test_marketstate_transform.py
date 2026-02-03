import polars as pl
import unittest
from intraday_analytics.tables import MarketStateTable


class TestMarketStateTableTransform(unittest.TestCase):
    def test_transform_fn_with_int64_timestamp(self):
        # Create a mock MarketState dataframe with Int64 TimestampNanoseconds
        # simulating raw parquet data
        df = pl.DataFrame(
            {
                "TimestampNanoseconds": [1609491600000000000],  # 2021-01-01 09:00:00
                "ListingId": [123],
                "MarketState": ["OPEN"],
                "MIC": ["XAMS"],
                "ExchangeMarketState": ["O"],
                "InstrumentId": [1],
                "ExchangeSequenceNo": [1],
                "BMLLSequenceNo": [1],
                "BMLLSequenceSource": ["S"],
            }
        ).lazy()

        ref = pl.DataFrame({"ListingId": [123]})
        nanoseconds = 60_000_000_000  # 1 minute

        table = MarketStateTable()
        transform_fn = table.get_transform_fn(
            ref, nanoseconds, time_bucket_anchor="end", time_bucket_closed="right"
        )

        # Apply transformation
        result_lf = transform_fn(df)
        result_df = result_lf.collect()

        # Check if EventTimestamp exists and is of correct type
        self.assertIn("EventTimestamp", result_df.columns)
        self.assertEqual(result_df["EventTimestamp"].dtype, pl.Datetime("ns"))

        # Check if TimeBucket exists and is of correct type
        self.assertIn("TimeBucket", result_df.columns)
        self.assertEqual(result_df["TimeBucket"].dtype, pl.Datetime("ns"))


if __name__ == "__main__":
    unittest.main()
