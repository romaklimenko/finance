"""
Load Nordea bank CSV exports into DuckDB staging area.

Usage:
    python scripts/load_nordea.py [--csv-dir PATH] [--db-path PATH]
"""

import hashlib
from pathlib import Path
from datetime import datetime

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
    currency: str
    reconciled: str | None

    @field_validator("amount", "balance", mode="before")
    @classmethod
    def parse_danish_decimal(cls, v: str | None) -> float | None:
        """Convert Danish decimal format (comma separator) to float."""
        if v is None or v == "":
            return None
        return float(str(v).replace(",", "."))

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
                str(self.balance or ""),
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
    """
    transactions = []

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    if not lines:
        return []

    # Parse header
    header = lines[0].strip().rstrip(";").split(";")
    english_header = [COLUMN_MAPPING.get(col, col) for col in header]

    # Parse data rows
    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue

        values = line.rstrip(";").split(";")

        # Pad values list to match header length (some rows may have fewer values)
        while len(values) < len(english_header):
            values.append("")

        # Create dict from header + values
        row_dict = dict(zip(english_header, values))

        # Convert empty strings to None
        row_dict = {k: (v if v != "" else None) for k, v in row_dict.items()}

        transaction = NordeaTransaction(**row_dict)
        transactions.append(transaction)

    return transactions


def create_staging_table(con: duckdb.DuckDBPyConnection) -> None:
    """Create the raw_nordea_transactions staging table if it doesn't exist."""
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


def load_transactions(
    con: duckdb.DuckDBPyConnection,
    transactions: list[NordeaTransaction],
    source_file: str,
) -> tuple[int, int]:
    """
    Load transactions into DuckDB, skipping duplicates by hash.

    Returns:
        Tuple of (inserted_count, skipped_count)
    """
    inserted = 0
    skipped = 0

    for txn in transactions:
        txn_hash = txn.compute_hash()

        # Check if already exists
        result = con.execute(
            "SELECT 1 FROM raw_nordea_transactions WHERE transaction_hash = ?",
            [txn_hash],
        ).fetchone()

        if result:
            skipped += 1
            continue

        # Parse date if present
        posting_date = None
        if txn.posting_date:
            posting_date = datetime.strptime(txn.posting_date, "%Y/%m/%d").date()

        # Insert new transaction
        con.execute(
            """
            INSERT INTO raw_nordea_transactions (
                transaction_hash, posting_date, amount, sender, recipient,
                name, description, balance, currency, reconciled, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                txn_hash,
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
            ],
        )
        inserted += 1

    return inserted, skipped


def load_all_csv_files(csv_dir: Path, db_path: Path) -> None:
    """Load all Nordea CSV files from directory into DuckDB."""
    # Ensure db directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    create_staging_table(con)

    csv_files = list(csv_dir.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV file(s) in {csv_dir}")

    total_inserted = 0
    total_skipped = 0

    for csv_file in csv_files:
        print(f"\nProcessing: {csv_file.name}")
        transactions = parse_csv_file(csv_file)
        print(f"  Parsed {len(transactions)} transactions")

        inserted, skipped = load_transactions(con, transactions, csv_file.name)
        print(f"  Inserted: {inserted}, Skipped (duplicates): {skipped}")

        total_inserted += inserted
        total_skipped += skipped

    con.close()

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
