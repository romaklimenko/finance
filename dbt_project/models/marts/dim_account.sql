{{
    config(
        materialized='table'
    )
}}

with accounts as (
    select distinct
        sender_account as account_number
    from {{ ref('stg_nordea_transactions') }}
    where sender_account is not null

    union

    select distinct
        recipient_account as account_number
    from {{ ref('stg_nordea_transactions') }}
    where recipient_account is not null
),

final as (
    select
        account_number as account_key,
        account_number,
        'Nordea' as bank_name,
        'DKK' as default_currency,
        case
            when account_number like '07%' then 'Checking'
            else 'Unknown'
        end as account_type
    from accounts
)

select * from final
