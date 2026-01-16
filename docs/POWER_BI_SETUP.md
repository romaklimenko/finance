# Power BI Setup Guide

This guide explains how to import the finance data warehouse into Power BI Desktop and set up the data model.

## Prerequisites

- Power BI Desktop installed
- Data exported using `scripts/export_to_powerbi.py`

## Step 1: Export Data from DuckDB

Run the export script to generate CSV files:

```bash
# Activate virtual environment
.venv\Scripts\activate  # Windows

# Export mart tables to CSV
python scripts/export_to_powerbi.py

# Output will be in data/export/
# - dim_account.csv
# - dim_category.csv
# - dim_date.csv
# - fct_transactions.csv
```

## Step 2: Import CSV Files into Power BI

1. Open **Power BI Desktop**
2. Click **Get Data** > **Text/CSV**
3. Navigate to `data/export/` directory
4. Import each CSV file:
   - `dim_account.csv`
   - `dim_category.csv`
   - `dim_date.csv`
   - `fct_transactions.csv`

For each file:
- Click **Load** (or **Transform Data** if you need to make changes)
- Power BI will automatically detect data types

## Step 3: Create Relationships

After importing all tables, set up the star schema relationships:

1. Click **Model** view (left sidebar)
2. Create the following relationships by dragging from fact to dimension:

### Relationships to Create:

| From (Fact)                    | To (Dimension)           | Cardinality | Cross Filter |
|--------------------------------|--------------------------|-------------|--------------|
| `fct_transactions[account_key]` | `dim_account[account_key]` | Many-to-One | Single       |
| `fct_transactions[category_key]`| `dim_category[category_key]`| Many-to-One | Single       |
| `fct_transactions[date_key]`    | `dim_date[date_key]`      | Many-to-One | Single       |

**Important:** Ensure all relationships are **Many-to-One** from fact to dimension, with **Single** direction cross-filtering.

## Step 4: Verify Data Model

In Model view, your schema should look like this:

```
        dim_account
              |
              | (account_key)
              |
        dim_category ---- fct_transactions ---- dim_date
              |                 |                     |
        (category_key)       (fact)            (date_key)
```

## Step 5: Create Measures (Optional)

Add these common measures to the `fct_transactions` table:

### Total Amount
```dax
Total Amount = SUM(fct_transactions[amount])
```

### Total Income
```dax
Total Income =
CALCULATE(
    SUM(fct_transactions[amount]),
    fct_transactions[transaction_type] = "credit"
)
```

### Total Expenses
```dax
Total Expenses =
CALCULATE(
    SUM(fct_transactions[amount]),
    fct_transactions[transaction_type] = "debit"
)
```

### Net Flow
```dax
Net Flow = [Total Income] + [Total Expenses]
```

### Transaction Count
```dax
Transaction Count = COUNTROWS(fct_transactions)
```

### Average Transaction Amount
```dax
Average Transaction = AVERAGE(fct_transactions[absolute_amount])
```

## Step 6: Create Report Pages

### Suggested Visualizations:

1. **Overview Dashboard**
   - Card: Total Income, Total Expenses, Net Flow
   - Line chart: Amount by Date (using dim_date)
   - Bar chart: Amount by Category (using dim_category)

2. **Category Analysis**
   - Pie chart: Expenses by Category Group
   - Matrix: Category breakdown with amounts
   - Filter: Category Type (Essential vs Discretionary)

3. **Time Series**
   - Line chart: Daily balance
   - Line chart: Monthly income vs expenses
   - Slicer: Year, Quarter, Month (from dim_date)

4. **Transaction Details**
   - Table: Transaction list with filters
   - Filters: Date range, Category, Account

## Data Refresh

To refresh the data with new transactions:

1. Run the dbt pipeline to update mart tables:
   ```bash
   cd dbt_project
   dbt run
   ```

2. Export fresh data to CSV:
   ```bash
   python scripts/export_to_powerbi.py
   ```

3. In Power BI Desktop:
   - Click **Home** > **Refresh**
   - Power BI will reload the CSV files

## Tips

- Use `dim_date` for time-based filters (it includes year, quarter, month, week fields)
- Filter out transfers using `dim_category[category_type] <> "Transfer"` for spending analysis
- Use `categorization_status = "Uncategorized"` to find transactions that need category mapping
- The `absolute_amount` field is useful for charts where you don't want negative values

## Troubleshooting

### Issue: Relationships not working
- Verify key columns exist in both tables
- Check that key values match exactly (case-sensitive)
- Ensure relationship cardinality is Many-to-One (never Many-to-Many)

### Issue: Measures showing wrong totals
- Check filter context in visualizations
- Verify that relationships are set to Single direction
- Use DAX Studio to debug measure calculations

### Issue: Data not refreshing
- Ensure CSV files were re-exported after running dbt
- Check file paths haven't changed
- Try **Transform Data** > **Refresh Preview**
