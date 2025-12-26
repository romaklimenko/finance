# Finance Project - Claude Instructions

## Project Overview

Personal finance tracking and reporting system using:
- **DuckDB** for data storage
- **dbt Core** for data transformations (Kimball warehouse architecture)
- **Power BI** for visualization (future)

## Directory Structure

```
finance/
├── data/                    # GITIGNORED - Contains sensitive data
│   ├── raw/nordea/          # Bank CSV exports (landing zone)
│   ├── db/                  # DuckDB databases
│   └── export/              # CSV exports for Power BI
├── dbt_project/             # dbt Core project
│   ├── models/staging/      # Raw data loading (views)
│   ├── models/intermediate/ # Transformations (views)
│   ├── models/marts/        # Final facts & dimensions (tables)
│   ├── seeds/               # Reference data (category mappings)
│   └── macros/              # Custom SQL macros
└── scripts/                 # Python utility scripts
```

## Development Workflow

### Branch Strategy
1. Each GitHub Issue = separate branch
2. Branch naming: `issue-{number}-{short-description}`
3. Work on branch, commit, push
4. Create PR for code review
5. Merge after approval

### Running dbt

```bash
# Activate virtual environment
.venv\Scripts\activate  # Windows

# Navigate to dbt project
cd dbt_project

# Run models
dbt run

# Test models
dbt test

# Generate docs
dbt docs generate
```

## Security Requirements

**CRITICAL**: Never commit sensitive data!

- All files in `/data/` are gitignored
- Never include account numbers in code or commit messages
- Never include transaction details in code or commit messages
- Use hashes for deduplication, never raw identifiers

## Data Sources

### Nordea Bank (Denmark)
- Location: `data/raw/nordea/`
- Format: CSV (semicolon-delimited, UTF-8 with BOM)
- Language: Danish headers
- Columns:
  - Bogføringsdato (Posting Date)
  - Beløb (Amount)
  - Afsender (Sender)
  - Modtager (Recipient)
  - Navn (Name)
  - Beskrivelse (Description)
  - Saldo (Balance)
  - Valuta (Currency)
  - Afstemt (Reconciled)

## dbt Conventions

### Naming
- Staging models: `stg_{source}_{entity}` (e.g., `stg_nordea_transactions`)
- Intermediate models: `int_{entity}_{description}`
- Mart models: `dim_{entity}` or `fct_{entity}`

### Transaction Hashing
Generate unique hash for deduplication:
```sql
md5(
    coalesce(posting_date::text, '') ||
    coalesce(amount::text, '') ||
    coalesce(description, '') ||
    coalesce(name, '') ||
    coalesce(balance::text, '')
)
```

## Python Environment

```bash
# Install dependencies
uv sync

# Add new dependency
uv add <package>
```

Required Python version: >=3.11, <3.14 (dbt compatibility)
