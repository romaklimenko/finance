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

## Workflow

### 1. Load Bank Data

Export transactions from your bank and load into DuckDB:

```bash
# Place CSV files in data/raw/nordea/
python scripts/load_nordea.py
```

### 2. Transform with dbt

Run dbt to transform raw data into a star schema:

```bash
cd dbt_project

# Load seed data (category mappings)
dbt seed

# Run all models
dbt run

# Run tests
dbt test
```

### 3. Export for Power BI

Export mart tables to CSV format:

```bash
python scripts/export_to_powerbi.py
```

Then follow the [Power BI Setup Guide](docs/POWER_BI_SETUP.md) to import and visualize the data.

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
