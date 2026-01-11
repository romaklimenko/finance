with source as (
    select * from {{ source('raw', 'raw_nordea_transactions') }}
),

staged as (
    select
        -- Primary key
        transaction_hash,

        -- Date fields
        posting_date,

        -- Financial fields
        amount,
        balance,
        currency,

        -- Transaction parties
        sender as sender_account,
        recipient as recipient_account,
        name as counterparty_name,
        description as transaction_description,

        -- Status
        reconciled as is_reconciled,

        -- Derived fields
        case when amount < 0 then 'debit' else 'credit' end as transaction_type,
        abs(amount) as absolute_amount,

        -- Metadata
        source_file,
        loaded_at

    from source
    where posting_date is not null  -- Exclude pending transactions
)

select * from staged
