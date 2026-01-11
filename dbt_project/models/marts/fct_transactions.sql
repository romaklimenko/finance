{{
    config(
        materialized='table'
    )
}}

with transactions as (
    select * from {{ ref('stg_nordea_transactions') }}
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

        -- Future: category_key (will be populated by categorization system)
        null::varchar as category_key,

        -- Metadata
        source_file,
        loaded_at

    from transactions
)

select * from final
