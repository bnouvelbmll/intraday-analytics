import unittest
from unittest import mock

try:
    from dagster import MultiPartitionKey, StaticPartitionsDefinition
    DAGSTER_AVAILABLE = True
except Exception:
    DAGSTER_AVAILABLE = False

from intraday_analytics.dagster_compat import (
    build_input_source_assets,
    build_s3_input_asset_checks,
)


@unittest.skipIf(not DAGSTER_AVAILABLE, "dagster is not installed")
class TestDagsterCompatInputAssets(unittest.TestCase):
    def _build_partitions(self):
        date_partitions = StaticPartitionsDefinition(["2025-01-02"])
        mic_partitions = StaticPartitionsDefinition(["XLON"])
        return date_partitions, mic_partitions

    def test_build_input_source_assets_partitions(self):
        date_partitions, mic_partitions = self._build_partitions()
        assets, _ = build_input_source_assets(
            tables=["l2", "l3", "trades", "marketstate"],
            date_partitions_def=date_partitions,
            mic_partitions_def=mic_partitions,
            asset_key_prefix=["input"],
            date_dim="date",
            mic_dim="mic",
        )

        self.assertEqual(len(assets), 4)
        for asset in assets:
            self.assertEqual(asset.partitions_def.partition_dimension_names, ["date", "mic"])
            partitioning = asset.metadata.get("partitioning")
            if hasattr(partitioning, "value"):
                partitioning = partitioning.value
            self.assertEqual(partitioning, "mic_date")

    def test_input_asset_checks_for_mic_date(self):
        date_partitions, mic_partitions = self._build_partitions()
        assets, table_map = build_input_source_assets(
            tables=["l2", "l3", "trades", "marketstate"],
            date_partitions_def=date_partitions,
            mic_partitions_def=mic_partitions,
            asset_key_prefix=["input"],
            date_dim="date",
            mic_dim="mic",
        )

        class DummyTable:
            def __init__(self, name):
                self.name = name
                self.calls = []

            def get_s3_paths(self, mics, year, month, day):
                self.calls.append((mics, year, month, day))
                return [f"s3://bucket/{self.name}/{mics[0]}/{year}{month:02d}{day:02d}.parquet"]

        dummy_tables = {asset.key: DummyTable(asset.key.path[-1]) for asset in assets}
        table_map = dummy_tables

        checks = build_s3_input_asset_checks(
            assets=assets,
            table_map=table_map,
            date_dim="date",
            mic_dim="mic",
            check_mode="head",
        )

        ctx = type("Ctx", (), {})()
        ctx.partition_key = MultiPartitionKey({"date": "2025-01-02", "mic": "XLON"})

        with mock.patch("intraday_analytics.dagster_compat._s3_path_exists", return_value=True):
            for check_def in checks:
                result = check_def.node_def.compute_fn.decorated_fn(ctx)
                self.assertTrue(result.passed)

        for dummy in dummy_tables.values():
            self.assertEqual(dummy.calls, [(["XLON"], 2025, 1, 2)])
