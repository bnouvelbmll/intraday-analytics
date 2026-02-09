import polars as pl
from pathlib import Path


def extract_and_save_schemas():
    """
    Finds all Parquet files in the 'sample_fixture' directory,
    extracts their schemas, and saves them to corresponding CSV files.
    """
    fixture_dir = Path(__file__).parent.parent / "sample_fixture"
    parquet_files = list(fixture_dir.glob("*.parquet"))

    if not parquet_files:
        print("No Parquet files found in the 'sample_fixture' directory.")
        return

    for parquet_file in parquet_files:
        try:
            # Read only the schema without loading data
            schema = pl.read_parquet(parquet_file, n_rows=0).schema

            # Create a DataFrame from the schema
            schema_df = pl.DataFrame(
                {
                    "ColumnName": list(schema.keys()),
                    "DataType": [str(t) for t in schema.values()],
                }
            )

            # Define the output CSV path
            csv_file = parquet_file.with_suffix(".csv")

            # Write the schema DataFrame to a CSV file
            schema_df.write_csv(csv_file)
            print(f"Extracted schema for {parquet_file.name} to {csv_file.name}")

        except Exception as e:
            print(f"Failed to process {parquet_file.name}: {e}")


if __name__ == "__main__":
    extract_and_save_schemas()
