# Finance

Personal finance tracking and reporting system using DuckDB and dbt Core.

## Overview

This project automates personal finance tracking by:
1. Loading bank transaction exports (CSV files)
2. Transforming data using dbt Core with Kimball warehouse architecture
3. Visualizing data with interactive Streamlit dashboard or Power BI reports

## Tech Stack

- **Database**: DuckDB (local file-based)
- **Transformations**: dbt Core with dbt-duckdb adapter
- **Visualization**: Streamlit dashboard, Power BI
- **Package Manager**: uv
- **Python**: 3.11+

## Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/finance.git
cd finance

# Install dependencies
uv sync
```

## Quick Start

Run the complete pipeline (load data, transform, and launch dashboard):

```bash
# Activate virtual environment (Windows)
.venv\Scripts\activate

# Run the pipeline
python run.py
```

This will:
1. Load CSV files from `data/raw/nordea/` into DuckDB
2. Run dbt seed and models
3. Launch the Streamlit dashboard at http://localhost:8501

**Options:**
- `--skip-load` - Skip CSV loading, use existing data
- `--skip-dbt` - Skip dbt transformations, use existing models

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

### 3. Visualize Data

#### Option A: Streamlit Dashboard (Recommended)

Launch the interactive web dashboard:

```bash
streamlit run scripts/dashboard.py
```

The dashboard will open in your browser at http://localhost:8501 with:
- Key metrics (Income, Expenses, Net Flow)
- Interactive charts (spending by category, trends over time)
- Transaction search and filtering
- CSV export functionality

#### Option B: Power BI

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
