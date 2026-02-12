from __future__ import annotations

from basalt.utils.schema_name_mapping import (
    resolve_schema_dataset,
    db_to_pascal_column,
    pascal_to_db_column,
)
from basalt.tables import ALL_TABLES


def test_resolve_schema_dataset_aliases():
    assert resolve_schema_dataset("trades_plus") == "trades_plus"
    assert resolve_schema_dataset("Trades-Plus") == "trades_plus"
    assert resolve_schema_dataset("marketstate") == "market_state"
    assert resolve_schema_dataset("cbbo") == "cbbo_equity"
    assert resolve_schema_dataset("Some-New-Table") == "some_new_table"


def test_mapping_uses_acronym_aware_nat_to_pascal():
    assert db_to_pascal_column("trades_plus", "TRADE_DATE") == "TradeDate"
    assert db_to_pascal_column("trades_plus", "BMLL_SEQUENCE_NO") == "BMLLSequenceNo"
    assert db_to_pascal_column("trades_plus", "MIC") == "MIC"
    assert db_to_pascal_column("reference", "OPOL") == "OPOL"
    assert db_to_pascal_column("trades_plus", "LISTING_ID") == "ListingId"
    assert db_to_pascal_column("trades_plus", "TRADE_NOTIONAL_USD") == "TradeNotionalUSD"
    assert db_to_pascal_column("trades_plus", "POST_TRADE_BEST_BID_1V") == "PostTradeBestBid1V"
    assert db_to_pascal_column("trades_plus", "PRE_TRADE_MID_300000MS") == "PreTradeMid300000ms"


def test_mapping_uses_acronym_aware_pascal_to_nat():
    assert pascal_to_db_column("trades_plus", "TradeDate") == "TRADE_DATE"
    assert pascal_to_db_column("trades_plus", "BMLLSequenceNo") == "BMLL_SEQUENCE_NO"
    assert pascal_to_db_column("reference", "MIC") == "MIC"
    assert pascal_to_db_column("reference", "OPOL") == "OPOL"
    assert pascal_to_db_column("trades_plus", "ListingId") == "LISTING_ID"
    assert pascal_to_db_column("trades_plus", "TradeNotionalUSD") == "TRADE_NOTIONAL_USD"
    assert pascal_to_db_column("trades_plus", "PostTradeBestBid1V") == "POST_TRADE_BEST_BID_1V"
    assert pascal_to_db_column("trades_plus", "PreTradeMid300000ms") == "PRE_TRADE_MID_300000MS"


def test_data_table_helpers_use_schema_mapping():
    trades = ALL_TABLES["trades"]
    assert trades.db_to_pascal_column("BMLL_SEQUENCE_NO") == "BMLLSequenceNo"
    assert trades.pascal_to_db_column("TradeTimestamp") == "TRADE_TIMESTAMP"
