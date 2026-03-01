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
    select
        dateadd(day, row_number() over (order by seq4()) - 1, start_date)::date as date_day
    from table(generator(rowcount => 10000)),
    date_range_final
    qualify date_day <= end_date
),

final as (
    select
        to_number(to_char(date_day, 'YYYYMMDD'))            as date_key,
        date_day                                            as full_date,
        dayofmonth(date_day)                                as day,
        dayname(date_day)                                   as day_name,
        month(date_day)                                     as month,
        monthname(date_day)                                 as month_name,
        year(date_day)                                      as year,
        'Q' || quarter(date_day)                            as quarter,
        iff(dayofweek(date_day) in (0, 6), true, false)     as is_weekend
    from date_spine
)

select * from final