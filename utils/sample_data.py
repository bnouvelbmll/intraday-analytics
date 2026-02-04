## Sample script to generate sample data for tests

import bmll2
import polars as pl


MIC='XLON'
listingids=[121317]
date="2026-01-02"
max_rows=50
start_hour=11
tables=["l2","l3", "imbalance", "cbbo", "market-state", "reference", "trades-plus"]

for t in tables:
   tbl=bmll2.get_market_data(market=MIC, date=date, table_name=t, df_engine='polars', lazy_load=True)
   tbl=tbl.filter(pl.col("ListingId").is_in(listingids))
   if t == "trades-plus":
      tbl = tbl.filter(pl.col("TradeTimestamp").dt.hour()>start_hour)
   elif t in ["l3"]:
      tbl = tbl.filter(pl.col("TimestampNanoseconds").dt.hour()>start_hour)
   elif t == "imbalance":
      tbl = tbl.filter(pl.col("Timestamp").dt.hour()>start_hour)
   elif t not in ["reference","market-state"]:
      tbl = tbl.filter(pl.col("EventTimestamp").dt.hour()>start_hour)
   tbl.limit(max_rows).collect().to_pandas().to_parquet(f"sample/{t}.parquet")
