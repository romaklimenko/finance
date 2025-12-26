"""
Load Nordea bank CSV exports into DuckDB staging area.

Usage:
    python scripts/load_nordea.py [--csv-dir PATH] [--db-path PATH]
"""

import csv
import hashlib
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb
from pydantic import BaseModel, field_validator


class NordeaTransaction(BaseModel):
    """Represents a single Nordea transaction from CSV export."""

    posting_date: str | None  # Can be "Reserveret" for pending transactions
    amount: float
    sender: str | None
    recipient: str | None
    name: str | None
    description: str | None
    balance: float | None
    currency: str = "DKK"
    reconciled: str | None

    @field_validator("amount", "balance", mode="before")
    @classmethod
    def parse_danish_decimal(cls, v: str | None) -> float | None:
        """Convert Danish decimal format (comma separator, period thousands) to float."""
        if v is None or v == "":
            return None
        # Remove thousands separator (period) and convert decimal separator (comma)
        cleaned = str(v).replace(".", "").replace(",", ".")
        return float(cleaned)

    @field_validator("posting_date", mode="before")
    @classmethod
    def normalize_date(cls, v: str | None) -> str | None:
        """Normalize date or return None for 'Reserveret'."""
        if v is None or v == "" or v == "Reserveret":
            return None
        return v

    def compute_hash(self) -> str:
        """
        Generate unique hash for deduplication.

        Uses MD5 of key fields to create a stable identifier.
        """
        hash_input = "|".join(
            [
                str(self.posting_date or ""),
                str(self.amount),
                str(self.description or ""),
                str(self.name or ""),
                str(self.sender or ""),
                str(self.recipient or ""),
            ]
        )
        return hashlib.md5(hash_input.encode("utf-8")).hexdigest()


# Column mapping: Danish CSV header -> English field name
COLUMN_MAPPING = {
    "Bogføringsdato": "posting_date",
    "Beløb": "amount",
    "Afsender": "sender",
    "Modtager": "recipient",
    "Navn": "name",
    "Beskrivelse": "description",
    "Saldo": "balance",
    "Valuta": "currency",
    "Afstemt": "reconciled",
}


def parse_csv_file(csv_path: Path) -> list[NordeaTransaction]:
    """
    Parse a Nordea CSV file into transaction objects.

    Handles:
    - UTF-8 with BOM encoding
    - Semicolon delimiter
    - Danish headers
    - Danish decimal format (comma)
    - Quoted fields containing delimiters
    """
    transactions = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=";")

        # Parse header
        header = next(reader, None)
        if not header:
            print(f"  Warning: {csv_path.name} appears to be empty", file=sys.stderr)
            return []

        # Remove empty trailing column if present (from trailing semicolon)
        if header and header[-1] == "":
            header = header[:-1]

        english_header = [COLUMN_MAPPING.get(col, col) for col in header]

        # Parse data rows
        for row in reader:
            if not row or all(v == "" for v in row):
                continue

            # Remove empty trailing value if present
            if row and row[-1] == "":
                row = row[:-1]

            # Pad values list to match header length (some rows may have fewer values)
            while len(row) < len(english_header):
                row.append("")

            # Create dict from header + values
            row_dict = dict(zip(english_header, row))

            # Convert empty strings to None
            row_dict = {k: (v if v != "" else None) for k, v in row_dict.items()}

            transaction = NordeaTransaction(**row_dict)
            transactions.append(transaction)

    return transactions


def create_staging_table(con: duckdb.DuckDBPyConnection, truncate: bool = False) -> None:
    """Create the raw_nordea_transactions staging table if it doesn't exist.

    Args:
        con: Database connection
        truncate: If True, drop and recreate the table for a fresh load
    """
    if truncate:
        con.execute("DROP TABLE IF EXISTS raw_nordea_transactions")

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_nordea_transactions (
            transaction_hash VARCHAR PRIMARY KEY,
            posting_date DATE,
            amount DECIMAL(18, 2),
            sender VARCHAR,
            recipient VARCHAR,
            name VARCHAR,
            description VARCHAR,
            balance DECIMAL(18, 2),
            currency VARCHAR,
            reconciled VARCHAR,
            source_file VARCHAR,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def parse_posting_date(date_str: str | None, source_file: str) -> date | None:
    """Parse posting date string to date object with error handling."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y/%m/%d").date()
    except ValueError as e:
        raise ValueError(
            f"Failed to parse posting_date '{date_str}' in file "
            f"'{source_file}': {e}"
        ) from e


def load_transactions(
    con: duckdb.DuckDBPyConnection,
    transactions: list[NordeaTransaction],
    source_file: str,
    loaded_dates: set[date],
) -> tuple[int, int, set[date]]:
    """
    Load transactions into DuckDB using batch insert with ON CONFLICT.

    Skips transactions for dates already loaded from newer files.

    Args:
        con: Database connection
        transactions: List of transactions to load
        source_file: Name of source CSV file
        loaded_dates: Set of dates already loaded from newer files

    Returns:
        Tuple of (inserted_count, skipped_count, new_dates_in_file)
    """
    if not transactions:
        return 0, 0, set()

    # Prepare batch data, skipping dates already loaded
    batch_data = []
    dates_in_file: set[date] = set()
    skipped_by_date = 0

    for txn in transactions:
        posting_date = parse_posting_date(txn.posting_date, source_file)

        # Skip reserved (pending) transactions - they have no posting date
        if posting_date is None:
            skipped_by_date += 1
            continue

        # Skip if this date was already loaded from a newer file
        if posting_date in loaded_dates:
            skipped_by_date += 1
            continue

        if posting_date:
            dates_in_file.add(posting_date)

        batch_data.append(
            (
                txn.compute_hash(),
                posting_date,
                txn.amount,
                txn.sender,
                txn.recipient,
                txn.name,
                txn.description,
                txn.balance,
                txn.currency,
                txn.reconciled,
                source_file,
            )
        )

    if not batch_data:
        return 0, skipped_by_date, set()

    # Get count before insert
    result = con.execute(
        "SELECT COUNT(*) FROM raw_nordea_transactions"
    ).fetchone()
    count_before: int = result[0] if result else 0

    # Batch insert with ON CONFLICT DO NOTHING for deduplication
    con.executemany(
        """
        INSERT INTO raw_nordea_transactions (
            transaction_hash, posting_date, amount, sender, recipient,
            name, description, balance, currency, reconciled, source_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (transaction_hash) DO NOTHING
        """,
        batch_data,
    )

    # Get count after insert
    result = con.execute(
        "SELECT COUNT(*) FROM raw_nordea_transactions"
    ).fetchone()
    count_after: int = result[0] if result else 0

    inserted = count_after - count_before
    skipped_by_hash = len(batch_data) - inserted

    return inserted, skipped_by_date + skipped_by_hash, dates_in_file


def extract_account_from_filename(filename: str) -> str:
    """Extract account identifier from Nordea filename.

    Expected format: 'Konto XXXXXXXXXX - YYYY-MM-DD HH.MM.SS.csv'
    Returns the account number or the full filename if pattern doesn't match.
    """
    # Try to extract account number (e.g., "Konto 0718425948")
    if filename.startswith("Konto "):
        parts = filename.split(" - ", 1)
        if len(parts) >= 1:
            return parts[0]  # "Konto XXXXXXXXXX"
    return filename


def load_all_csv_files(csv_dir: Path, db_path: Path, truncate: bool = True) -> None:
    """Load all Nordea CSV files from directory into DuckDB.

    Files are processed newest-first (sorted alphabetically descending by name).
    For each account, dates already loaded from newer files are skipped in older files.

    Args:
        csv_dir: Directory containing Nordea CSV files
        db_path: Path to DuckDB database file
        truncate: If True (default), drop and recreate the table for a fresh load
    """
    # Validate CSV directory exists
    if not csv_dir.exists():
        print(f"Error: CSV directory does not exist: {csv_dir}", file=sys.stderr)
        sys.exit(1)
    if not csv_dir.is_dir():
        print(f"Error: Path is not a directory: {csv_dir}", file=sys.stderr)
        sys.exit(1)

    # Ensure db directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    csv_files = list(csv_dir.glob("*.csv"))
    # Sort alphabetically descending - newer files (with later dates) come first
    csv_files.sort(key=lambda f: f.name, reverse=True)
    print(f"Found {len(csv_files)} CSV file(s) in {csv_dir} (processing newest first)")

    total_inserted = 0
    total_skipped = 0

    # Track loaded dates per account
    loaded_dates_by_account: dict[str, set[date]] = {}

    with duckdb.connect(str(db_path)) as con:
        create_staging_table(con, truncate=truncate)

        for csv_file in csv_files:
            print(f"\nProcessing: {csv_file.name}")

            account = extract_account_from_filename(csv_file.name)
            loaded_dates = loaded_dates_by_account.get(account, set())

            transactions = parse_csv_file(csv_file)
            print(f"  Parsed {len(transactions)} transactions")

            inserted, skipped, new_dates = load_transactions(
                con, transactions, csv_file.name, loaded_dates
            )
            print(f"  Inserted: {inserted}, Skipped: {skipped}")

            # Add newly loaded dates to the account's set
            if account not in loaded_dates_by_account:
                loaded_dates_by_account[account] = set()
            loaded_dates_by_account[account].update(new_dates)

            total_inserted += inserted
            total_skipped += skipped

    print(f"\n{'=' * 50}")
    print(f"Total inserted: {total_inserted}")
    print(f"Total skipped: {total_skipped}")
    print(f"Database: {db_path}")


def main() -> None:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Load Nordea CSV files into DuckDB")
    parser.add_argument(
        "--csv-dir",
        type=Path,
        default=Path("data/raw/nordea"),
        help="Directory containing Nordea CSV files",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/db/finance.duckdb"),
        help="Path to DuckDB database file",
    )
    args = parser.parse_args()

    load_all_csv_files(args.csv_dir, args.db_path)


if __name__ == "__main__":
    main()
