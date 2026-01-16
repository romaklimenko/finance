{{
    config(
        materialized='table'
    )
}}

with transactions as (
    select * from {{ ref('int_transactions_categorized') }}
),

final as (
    select
        -- Keys
        transaction_hash as transaction_key,
        posting_date as date_key,
        coalesce(sender_account, recipient_account) as account_key,

        -- Measures
        amount,
        absolute_amount,
        balance,

        -- Attributes
        transaction_type,
        counterparty_name,
        transaction_description,
        currency,

        -- Category
        category_key,
        categorization_status,

        -- Metadata
        source_file,
        loaded_at

    from transactions
)

select * from final
