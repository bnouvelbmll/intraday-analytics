from __future__ import annotations

from basalt.tables import ALL_TABLES


def test_table_metadata_for_cloud_sources():
    assert ALL_TABLES["trades"].snowflake_table_name() == "TRADES_PLUS"
    assert ALL_TABLES["trades"].databricks_table_name() == "trades_plus"
    assert ALL_TABLES["l2"].snowflake_table_name() == "L2_DATA_EQUITY"
    assert ALL_TABLES["l2"].databricks_table_name() == "l2_data_equity"
    assert ALL_TABLES["cbbo"].snowflake_table_name() == "CBBO_EQUITY"
    assert ALL_TABLES["cbbo"].databricks_table_name() == "cbbo_equity"
    assert ALL_TABLES["marketstate"].snowflake_table_name() == "MARKET_STATE"
    assert ALL_TABLES["marketstate"].databricks_table_name() == "market_state"
    assert ALL_TABLES["imbalance"].snowflake_table_name() == "IMBALANCE"
    assert ALL_TABLES["nbbo"].snowflake_table_name() == "NBBO"
    assert ALL_TABLES["future_l1"].snowflake_table_name() == "FUTURE_L1"
    assert ALL_TABLES["opra_trades"].snowflake_table_name() == "OPRA_TRADES"


def test_reference_databricks_column_override_mapping():
    ref = ALL_TABLES["reference"]
    assert ref.nat_to_source_column("databricks", "REFERENCE_DATE") == "DATE"
    assert ref.source_to_nat_column("databricks", "DATE") == "REFERENCE_DATE"
    assert ref.nat_to_source_column("snowflake", "REFERENCE_DATE") == "REFERENCE_DATE"
    assert ref.source_to_nat_column("snowflake", "REFERENCE_DATE") == "REFERENCE_DATE"


def test_extended_tables_registered():
    expected = {
        "imbalance",
        "nbbo",
        "future_l1",
        "future_market_state",
        "future_reference",
        "future_statistics",
        "future_trades",
        "opra_reference",
        "opra_statistics",
        "opra_trades",
    }
    assert expected.issubset(set(ALL_TABLES.keys()))
