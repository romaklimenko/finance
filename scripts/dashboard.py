"""
Interactive Streamlit dashboard for personal finance visualization.

Usage:
    streamlit run scripts/dashboard.py
"""

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# Configuration
DB_PATH = Path("data/db/finance.duckdb")
st.set_page_config(
    page_title="Finance Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_db_connection():
    """Create a cached database connection."""
    if not DB_PATH.exists():
        st.error(f"Database not found: {DB_PATH}")
        st.stop()
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data
def load_data():
    """Load transaction data with all dimensions."""
    con = get_db_connection()
    query = """
    SELECT
        t.transaction_key,
        t.date_key,
        t.account_key,
        t.category_key,
        t.amount,
        t.absolute_amount,
        t.balance,
        t.transaction_type,
        t.counterparty_name,
        t.transaction_description,
        t.currency,
        t.categorization_status,
        d.date_day,
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        d.week,
        d.day_of_week,
        c.category_name,
        c.category_group,
        c.category_type,
        c.category_classification,
        a.account_number,
        a.bank_name,
        a.account_type
    FROM fct_transactions t
    LEFT JOIN dim_date d ON t.date_key = d.date_key
    LEFT JOIN dim_category c ON t.category_key = c.category_key
    LEFT JOIN dim_account a ON t.account_key = a.account_key
    ORDER BY d.date_day DESC
    """
    df = con.execute(query).df()
    df["date_day"] = pd.to_datetime(df["date_day"])
    return df


def format_currency(value):
    """Format value as Danish Krone."""
    return f"{value:,.2f} DKK"


def main():
    st.title("💰 Personal Finance Dashboard")

    # Load data
    with st.spinner("Loading data..."):
        df = load_data()

    # Sidebar filters
    st.sidebar.header("Filters")

    # Date range filter
    min_date = df["date_day"].min().date()
    max_date = df["date_day"].max().date()

    date_range = st.sidebar.date_input(
        "Date Range",
        value=(max_date - timedelta(days=90), max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
        df_filtered = df[
            (df["date_day"].dt.date >= start_date) & (df["date_day"].dt.date <= end_date)
        ]
    else:
        df_filtered = df

    # Category filter
    categories = ["All"] + sorted(df["category_name"].unique().tolist())
    selected_categories = st.sidebar.multiselect(
        "Categories", categories, default=["All"]
    )

    if "All" not in selected_categories and selected_categories:
        df_filtered = df_filtered[df_filtered["category_name"].isin(selected_categories)]

    # Transaction type filter
    transaction_types = st.sidebar.multiselect(
        "Transaction Type",
        ["credit", "debit"],
        default=["credit", "debit"],
    )
    df_filtered = df_filtered[df_filtered["transaction_type"].isin(transaction_types)]

    # Key Metrics
    st.header("Key Metrics")
    col1, col2, col3, col4 = st.columns(4)

    total_income = df_filtered[df_filtered["transaction_type"] == "credit"]["amount"].sum()
    total_expenses = df_filtered[df_filtered["transaction_type"] == "debit"]["amount"].sum()
    net_flow = total_income + total_expenses
    transaction_count = len(df_filtered)

    col1.metric("Total Income", format_currency(total_income))
    col2.metric("Total Expenses", format_currency(abs(total_expenses)))
    col3.metric("Net Flow", format_currency(net_flow))
    col4.metric("Transactions", f"{transaction_count:,}")

    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📊 Overview", "📈 Trends", "🏷️ Categories", "📋 Transactions"]
    )

    with tab1:
        st.subheader("Overview")

        col1, col2 = st.columns(2)

        with col1:
            # Income vs Expenses
            summary_data = pd.DataFrame(
                {
                    "Type": ["Income", "Expenses"],
                    "Amount": [total_income, abs(total_expenses)],
                }
            )
            fig = px.bar(
                summary_data,
                x="Type",
                y="Amount",
                title="Income vs Expenses",
                color="Type",
                color_discrete_map={"Income": "#2ecc71", "Expenses": "#e74c3c"},
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Category breakdown (expenses only)
            expenses_df = df_filtered[df_filtered["transaction_type"] == "debit"]
            category_totals = (
                expenses_df.groupby("category_name")["absolute_amount"]
                .sum()
                .sort_values(ascending=False)
            )
            fig = px.pie(
                values=category_totals.values,
                names=category_totals.index,
                title="Expenses by Category",
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("Spending Trends Over Time")

        # Daily spending
        daily_data = (
            df_filtered.groupby(["date_day", "transaction_type"])["amount"]
            .sum()
            .reset_index()
        )

        fig = px.line(
            daily_data,
            x="date_day",
            y="amount",
            color="transaction_type",
            title="Daily Cash Flow",
            color_discrete_map={"credit": "#2ecc71", "debit": "#e74c3c"},
        )
        fig.update_xaxes(title="Date")
        fig.update_yaxes(title="Amount (DKK)")
        st.plotly_chart(fig, use_container_width=True)

        # Monthly summary
        st.subheader("Monthly Summary")
        monthly_data = (
            df_filtered.groupby(["year", "month", "month_name", "transaction_type"])[
                "amount"
            ]
            .sum()
            .reset_index()
        )
        monthly_data["period"] = (
            monthly_data["year"].astype(str)
            + "-"
            + monthly_data["month"].astype(str).str.zfill(2)
        )

        fig = go.Figure()
        for txn_type in monthly_data["transaction_type"].unique():
            data = monthly_data[monthly_data["transaction_type"] == txn_type]
            fig.add_trace(
                go.Bar(
                    x=data["period"],
                    y=data["amount"].abs(),
                    name=txn_type.capitalize(),
                    marker_color="#2ecc71" if txn_type == "credit" else "#e74c3c",
                )
            )

        fig.update_layout(
            title="Monthly Income vs Expenses",
            xaxis_title="Month",
            yaxis_title="Amount (DKK)",
            barmode="group",
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.subheader("Category Analysis")

        # Category group breakdown
        col1, col2 = st.columns(2)

        with col1:
            expenses_df = df_filtered[df_filtered["transaction_type"] == "debit"]
            group_totals = (
                expenses_df.groupby("category_group")["absolute_amount"]
                .sum()
                .sort_values(ascending=False)
            )
            fig = px.bar(
                x=group_totals.values,
                y=group_totals.index,
                orientation="h",
                title="Spending by Category Group",
                labels={"x": "Amount (DKK)", "y": "Category Group"},
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Essential vs Discretionary
            type_totals = (
                expenses_df.groupby("category_type")["absolute_amount"]
                .sum()
                .sort_values(ascending=False)
            )
            fig = px.pie(
                values=type_totals.values,
                names=type_totals.index,
                title="Essential vs Discretionary Spending",
            )
            st.plotly_chart(fig, use_container_width=True)

        # Category over time
        st.subheader("Category Spending Over Time")
        category_time = (
            expenses_df.groupby(["date_day", "category_name"])["absolute_amount"]
            .sum()
            .reset_index()
        )

        # Top categories only
        top_categories = (
            expenses_df.groupby("category_name")["absolute_amount"]
            .sum()
            .nlargest(5)
            .index.tolist()
        )
        category_time_top = category_time[
            category_time["category_name"].isin(top_categories)
        ]

        fig = px.line(
            category_time_top,
            x="date_day",
            y="absolute_amount",
            color="category_name",
            title="Top 5 Categories - Spending Over Time",
        )
        fig.update_xaxes(title="Date")
        fig.update_yaxes(title="Amount (DKK)")
        st.plotly_chart(fig, use_container_width=True)

        # Uncategorized transactions
        uncategorized = df_filtered[
            df_filtered["categorization_status"] == "Uncategorized"
        ]
        if len(uncategorized) > 0:
            st.warning(
                f"⚠️ {len(uncategorized)} transactions ({len(uncategorized)/len(df_filtered)*100:.1f}%) are uncategorized"
            )

    with tab4:
        st.subheader("Transaction Details")

        # Display options
        show_columns = st.multiselect(
            "Select columns to display",
            [
                "date_day",
                "amount",
                "category_name",
                "counterparty_name",
                "transaction_description",
                "transaction_type",
                "account_number",
                "categorization_status",
            ],
            default=[
                "date_day",
                "amount",
                "category_name",
                "counterparty_name",
                "transaction_description",
            ],
        )

        # Search
        search_term = st.text_input("Search transactions", "")
        if search_term:
            mask = (
                df_filtered["transaction_description"]
                .str.contains(search_term, case=False, na=False)
                | df_filtered["counterparty_name"].str.contains(
                    search_term, case=False, na=False
                )
            )
            display_df = df_filtered[mask]
        else:
            display_df = df_filtered

        # Display transaction table
        st.dataframe(
            display_df[show_columns].sort_values("date_day", ascending=False),
            use_container_width=True,
            height=400,
        )

        # Export to CSV
        csv = display_df.to_csv(index=False)
        st.download_button(
            label="Download as CSV",
            data=csv,
            file_name="transactions.csv",
            mime="text/csv",
        )

    # Footer
    st.sidebar.markdown("---")
    st.sidebar.info(
        f"""
        **Data Summary**
        - Total Transactions: {len(df):,}
        - Date Range: {min_date} to {max_date}
        - Database: {DB_PATH}
        """
    )


if __name__ == "__main__":
    main()
