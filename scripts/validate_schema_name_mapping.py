#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import yaml

from basalt.schema_name_mapping import (
    db_to_pascal_column,
    pascal_to_db_column,
    resolve_schema_dataset,
)


def _expected_pascal(column: dict) -> str:
    for key in ("pq_name", "name", "ext_name"):
        value = str(column.get(key) or "").strip()
        if value:
            return value
    return ""


def _iter_schema_files(schema_dir: Path) -> Iterable[Path]:
    for suffix in ("*.yml", "*.yaml"):
        yield from sorted(schema_dir.glob(suffix))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate basalt column name conversion helpers against an external "
            "schema catalog."
        )
    )
    parser.add_argument(
        "--schema-dir",
        required=True,
        help="Path to schema catalog directory containing dataset YAML files.",
    )
    args = parser.parse_args()

    schema_dir = Path(args.schema_dir).expanduser().resolve()
    if not schema_dir.exists() or not schema_dir.is_dir():
        raise SystemExit(f"Invalid schema directory: {schema_dir}")

    files = list(_iter_schema_files(schema_dir))
    if not files:
        raise SystemExit(f"No schema yaml files found in: {schema_dir}")

    alias_mismatches: list[str] = []
    conversion_mismatches: list[str] = []
    checked_columns = 0
    checked_datasets = 0

    for path in files:
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            conversion_mismatches.append(f"{path.name}: yaml parse error ({exc})")
            continue

        dataset = data.get("dataset") or {}
        dataset_name = str(dataset.get("name") or "").strip()
        if not dataset_name:
            continue
        checked_datasets += 1

        aliases = {
            dataset_name,
            str(dataset.get("s3_folder_name") or ""),
            str(dataset.get("s3_filename_prefix") or ""),
            str(dataset.get("name_in_filenames") or ""),
            str(dataset.get("customer_view_code") or ""),
            str(dataset.get("ext_table_name") or ""),
            path.stem,
        }
        aliases = {a for a in aliases if a}
        for alias in sorted(aliases):
            resolved = resolve_schema_dataset(alias)
            if resolved != dataset_name:
                alias_mismatches.append(
                    f"{path.name}: alias '{alias}' resolved as '{resolved}', expected '{dataset_name}'"
                )

        for column in dataset.get("columns") or []:
            if not isinstance(column, dict):
                continue
            nat = str(column.get("nat_name") or "").strip()
            pascal = _expected_pascal(column)
            if not nat or not pascal:
                continue
            checked_columns += 1
            got_pascal = db_to_pascal_column(dataset_name, nat)
            got_nat = pascal_to_db_column(dataset_name, pascal)
            if got_pascal != pascal:
                conversion_mismatches.append(
                    f"{path.name}: {dataset_name}.{nat} -> '{got_pascal}', expected '{pascal}'"
                )
            if got_nat != nat:
                conversion_mismatches.append(
                    f"{path.name}: {dataset_name}.{pascal} -> '{got_nat}', expected '{nat}'"
                )

    print(
        f"Checked datasets={checked_datasets}, columns={checked_columns}, "
        f"alias_mismatches={len(alias_mismatches)}, conversion_mismatches={len(conversion_mismatches)}"
    )
    if alias_mismatches:
        print("\nAlias mismatches:")
        for line in alias_mismatches[:200]:
            print(f"- {line}")
        if len(alias_mismatches) > 200:
            print(f"... {len(alias_mismatches) - 200} more")
    if conversion_mismatches:
        print("\nConversion mismatches:")
        for line in conversion_mismatches[:200]:
            print(f"- {line}")
        if len(conversion_mismatches) > 200:
            print(f"... {len(conversion_mismatches) - 200} more")

    return 1 if alias_mismatches or conversion_mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
