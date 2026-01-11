{{
    config(
        materialized='table'
    )
}}

with date_spine as (
    select
        unnest(generate_series(date '2020-01-01', date '2030-12-31', interval '1 day'))::date as date_day
),

dates as (
    select
        date_day as date_key,
        date_day,

        -- Year
        extract(year from date_day)::int as year,
        extract(quarter from date_day)::int as quarter,
        extract(month from date_day)::int as month,
        extract(week from date_day)::int as week_of_year,
        extract(day from date_day)::int as day_of_month,
        extract(dayofyear from date_day)::int as day_of_year,

        -- Week
        extract(isodow from date_day)::int as day_of_week,
        case extract(isodow from date_day)::int
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
            when 7 then 'Sunday'
        end as day_name,
        case when extract(isodow from date_day)::int in (6, 7) then true else false end as is_weekend,

        -- Month name
        case extract(month from date_day)::int
            when 1 then 'January'
            when 2 then 'February'
            when 3 then 'March'
            when 4 then 'April'
            when 5 then 'May'
            when 6 then 'June'
            when 7 then 'July'
            when 8 then 'August'
            when 9 then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
        end as month_name,

        -- Formatted strings
        strftime(date_day, '%Y-%m') as year_month,
        strftime(date_day, '%Y-Q') || extract(quarter from date_day)::varchar as year_quarter,

        -- First/last of period flags
        date_day = date_trunc('month', date_day)::date as is_first_day_of_month,
        date_day = (date_trunc('month', date_day) + interval '1 month' - interval '1 day')::date as is_last_day_of_month

    from date_spine
)

select * from dates
