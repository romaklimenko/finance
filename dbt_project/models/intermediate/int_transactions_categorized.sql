{{
    config(
        materialized='view'
    )
}}

with transactions as (
    select * from {{ ref('stg_nordea_transactions') }}
),

category_mappings as (
    select * from {{ ref('category_mappings') }}
),

-- Match transactions to categories based on description patterns
description_matches as (
    select
        t.transaction_hash,
        cm.category_key,
        cm.priority,
        row_number() over (
            partition by t.transaction_hash
            order by cm.priority desc, cm.category_key
        ) as match_rank
    from transactions t
    inner join category_mappings cm
        on cm.pattern_type = 'description'
        and lower(t.transaction_description) like '%' || lower(cm.pattern_value) || '%'
),

-- Match transactions to categories based on counterparty name patterns
name_matches as (
    select
        t.transaction_hash,
        cm.category_key,
        cm.priority,
        row_number() over (
            partition by t.transaction_hash
            order by cm.priority desc, cm.category_key
        ) as match_rank
    from transactions t
    inner join category_mappings cm
        on cm.pattern_type = 'name'
        and lower(t.counterparty_name) like '%' || lower(cm.pattern_value) || '%'
),

-- Match transactions to categories based on transaction type
type_matches as (
    select
        t.transaction_hash,
        cm.category_key,
        cm.priority,
        row_number() over (
            partition by t.transaction_hash
            order by cm.priority desc, cm.category_key
        ) as match_rank
    from transactions t
    inner join category_mappings cm
        on cm.pattern_type = 'transaction_type'
        and lower(t.transaction_type) = lower(cm.pattern_value)
),

-- Combine all matches and select the best match per transaction
all_matches as (
    select * from description_matches where match_rank = 1
    union all
    select * from name_matches where match_rank = 1
    union all
    select * from type_matches where match_rank = 1
),

best_match as (
    select
        transaction_hash,
        category_key,
        row_number() over (
            partition by transaction_hash
            order by priority desc, category_key
        ) as final_rank
    from all_matches
),

final as (
    select
        t.*,
        coalesce(bm.category_key, 'CAT999') as category_key,
        case
            when bm.category_key is null then 'Uncategorized'
            else 'Matched'
        end as categorization_status
    from transactions t
    left join best_match bm
        on t.transaction_hash = bm.transaction_hash
        and bm.final_rank = 1
)

select * from final
