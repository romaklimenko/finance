# Finance

Personal finance tracking and reporting system using DuckDB and dbt Core.

## Overview

This project automates personal finance tracking by:
1. Loading bank transaction exports (CSV files)
2. Transforming data using dbt Core with Kimball warehouse architecture
3. Generating reports for Power BI visualization

## Tech Stack

- **Database**: DuckDB (local file-based)
- **Transformations**: dbt Core with dbt-duckdb adapter
- **Package Manager**: uv
- **Python**: 3.11+

## Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/finance.git
cd finance

# Install dependencies
uv sync

# Run dbt
cd dbt_project
dbt run
```

## Project Structure

```
finance/
├── data/                    # Data files (gitignored)
│   ├── raw/                 # Bank exports landing zone
│   ├── db/                  # DuckDB databases
│   └── export/              # CSV exports for Power BI
├── dbt_project/             # dbt Core project
│   ├── models/
│   │   ├── staging/         # Raw data loading
│   │   ├── intermediate/    # Transformations
│   │   └── marts/           # Facts & dimensions
│   └── seeds/               # Reference data
└── scripts/                 # Python utilities
```

## Data Privacy

This is a personal finance project. All sensitive data (bank exports, databases) is excluded from version control via `.gitignore`.

## License

Private use only.
