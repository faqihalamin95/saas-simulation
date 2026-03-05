with cohort as (
    select
        user_key,
        date_trunc('month', event_timestamp_utc)::date      as cohort_month
    from {{ ref('fct_subscription_events') }}
    where event_type = 'trial_convert'
),

activity as (
    select 
        user_key,
        date_trunc('month', payment_timestamp_utc)::date    as activity_month
    from {{ ref('fct_payments') }}
    where status = 'success'
    group by user_key, activity_month
),

period as (
    select
        c.cohort_month,
        a.activity_month,
        datediff('month',c.cohort_month,a.activity_month)   as period_number,
        c.user_key
    from cohort c
    inner join activity a on c.user_key = a.user_key
    where period_number >= 0
),

cohort_population as (
    select
        cohort_month,
        count(user_key) as cohort_size
    from cohort
    group by cohort_month
),

retention as (
    select
        c.cohort_month,
        p.period_number,
        c.cohort_size,
        count(p.user_key) as retained_user,
        round(count(p.user_key)::float / c.cohort_size *100 , 2) as retention_rate
    from cohort_population c
    inner join period p on c.cohort_month = p.cohort_month
    group by c.cohort_month, p.period_number, c.cohort_size
)

select * from retention
order by cohort_month, period_number