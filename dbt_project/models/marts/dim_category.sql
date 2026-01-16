{{
    config(
        materialized='table'
    )
}}

with categories as (
    select
        'CAT001' as category_key,
        'Groceries' as category_name,
        'Food & Dining' as category_group,
        'Essential' as category_type,
        1 as sort_order

    union all

    select 'CAT002', 'Restaurants', 'Food & Dining', 'Discretionary', 2
    union all
    select 'CAT003', 'Cafes & Bars', 'Food & Dining', 'Discretionary', 3

    union all

    select 'CAT004', 'Rent', 'Housing', 'Essential', 4
    union all
    select 'CAT005', 'Utilities', 'Housing', 'Essential', 5
    union all
    select 'CAT006', 'Home Maintenance', 'Housing', 'Essential', 6

    union all

    select 'CAT007', 'Public Transport', 'Transportation', 'Essential', 7
    union all
    select 'CAT008', 'Fuel', 'Transportation', 'Essential', 8
    union all
    select 'CAT009', 'Parking', 'Transportation', 'Discretionary', 9

    union all

    select 'CAT010', 'Entertainment', 'Leisure', 'Discretionary', 10
    union all
    select 'CAT011', 'Shopping', 'Leisure', 'Discretionary', 11
    union all
    select 'CAT012', 'Sports & Fitness', 'Leisure', 'Discretionary', 12

    union all

    select 'CAT013', 'Healthcare', 'Health', 'Essential', 13
    union all
    select 'CAT014', 'Pharmacy', 'Health', 'Essential', 14
    union all
    select 'CAT015', 'Insurance', 'Health', 'Essential', 15

    union all

    select 'CAT016', 'Salary', 'Income', 'Income', 16
    union all
    select 'CAT017', 'Interest', 'Income', 'Income', 17
    union all
    select 'CAT018', 'Refund', 'Income', 'Income', 18

    union all

    select 'CAT019', 'Bank Fees', 'Financial', 'Essential', 19
    union all
    select 'CAT020', 'Subscriptions', 'Financial', 'Discretionary', 20
    union all
    select 'CAT021', 'Transfers', 'Financial', 'Transfer', 21

    union all

    select 'CAT999', 'Uncategorized', 'Other', 'Unknown', 999
),

final as (
    select
        category_key,
        category_name,
        category_group,
        category_type,
        sort_order,

        -- Category type classification
        case
            when category_type = 'Income' then 'Income'
            when category_type = 'Transfer' then 'Transfer'
            when category_type = 'Essential' then 'Expense - Essential'
            when category_type = 'Discretionary' then 'Expense - Discretionary'
            else 'Expense - Unknown'
        end as category_classification

    from categories
)

select * from final
