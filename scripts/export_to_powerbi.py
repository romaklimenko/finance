"""
Export dbt mart tables to CSV format for Power BI consumption.

Usage:
    python scripts/export_to_powerbi.py [--db-path PATH] [--export-dir PATH]

Exports:
    - dim_account
    - dim_category
    - dim_date
    - fct_transactions
"""

import sys
from pathlib import Path

import duckdb


def export_table_to_csv(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    output_path: Path,
) -> int:
    """
    Export a single table to CSV format.

    Args:
        con: Database connection
        table_name: Name of the table to export
        output_path: Path where CSV will be written

    Returns:
        Number of rows exported
    """
    # Get row count
    result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    row_count: int = result[0] if result else 0

    # Export to CSV using DuckDB's native COPY TO
    # This is more efficient than reading into Python and writing
    con.execute(f"""
        COPY {table_name}
        TO '{output_path}'
        WITH (
            HEADER true,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"'
        )
    """)

    return row_count


def export_all_marts(db_path: Path, export_dir: Path) -> None:
    """
    Export all mart tables to CSV files for Power BI.

    Args:
        db_path: Path to DuckDB database file
        export_dir: Directory where CSV files will be written
    """
    # Validate database exists
    if not db_path.exists():
        print(f"Error: Database does not exist: {db_path}", file=sys.stderr)
        sys.exit(1)

    # Create export directory if it doesn't exist
    export_dir.mkdir(parents=True, exist_ok=True)

    # Tables to export
    tables = [
        "dim_account",
        "dim_category",
        "dim_date",
        "fct_transactions",
    ]

    print(f"Exporting mart tables from {db_path}")
    print(f"Output directory: {export_dir}\n")

    total_rows = 0

    with duckdb.connect(str(db_path), read_only=True) as con:
        for table in tables:
            output_path = export_dir / f"{table}.csv"
            print(f"Exporting {table}...", end=" ")

            try:
                row_count = export_table_to_csv(con, table, output_path)
                total_rows += row_count
                print(f"OK {row_count:,} rows -> {output_path.name}")
            except Exception as e:
                print(f"ERROR: {e}", file=sys.stderr)
                sys.exit(1)

    print(f"\n{'=' * 50}")
    print(f"Total rows exported: {total_rows:,}")
    print(f"Files written to: {export_dir}")
    print("\nPower BI Import Instructions:")
    print("1. Open Power BI Desktop")
    print("2. Get Data > Text/CSV")
    print(f"3. Navigate to: {export_dir.absolute()}")
    print("4. Import each CSV file")
    print("5. Create relationships:")
    print("   - fct_transactions[account_key] -> dim_account[account_key]")
    print("   - fct_transactions[category_key] -> dim_category[category_key]")
    print("   - fct_transactions[date_key] -> dim_date[date_key]")


def main() -> None:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Export mart tables to CSV for Power BI"
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/db/finance.duckdb"),
        help="Path to DuckDB database file",
    )
    parser.add_argument(
        "--export-dir",
        type=Path,
        default=Path("data/export"),
        help="Directory where CSV files will be written",
    )
    args = parser.parse_args()

    export_all_marts(args.db_path, args.export_dir)


if __name__ == "__main__":
    main()
