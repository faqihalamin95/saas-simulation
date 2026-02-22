with date_range as (
    select
        min(event_date) as start_date,
        max(event_date) as end_date
    from {{ ref('stg_subscription_events') }}

    union all

    select
        min(event_date) as start_date,
        max(event_date) as end_date
    from {{ ref('stg_product_events') }}

    union all

    select
        min(payment_date) as start_date,
        max(payment_date) as end_date
    from {{ ref('stg_payments') }}
),

date_range_final as (
    select
        min(start_date) as start_date,
        max(end_date)   as end_date
    from date_range
),

date_spine as (
    select generate_series(start_date, end_date, interval '1 day')::date as date_day
    from date_range_final
),

final as (
    select
        cast(to_char(date_day, 'YYYYMMDD') as integer)   as date_key,
        date_day                                         as full_date,
        extract(day from date_day)::int                  as day,
        to_char(date_day, 'Day')                         as day_name,
        extract(month from date_day)::int                as month,
        to_char(date_day, 'Month')                       as month_name,
        extract(year from date_day)::int                 as year,
        'Q' || extract(quarter from date_day)::int       as quarter,
        case
            when extract(dow from date_day) in (0, 6) then true
            else false
        end                                              as is_weekend
    from date_spine
)

select * from final