with cohort_month as (
    select
        user_key,
        plan_key,
        date_trunc('month', event_timestamp_utc)::date      as cohort_month
    from {{ ref('fct_subscription_events') }}
    where event_type = 'trial_convert'
),

first_plan as (
    select
        c.user_key,
        c.plan_key,
        p.plan          as first_plan
    from cohort_month c
    left join {{ ref('dim_plans') }} p on c.plan_key = p.plan_key
),

total_revenue as (
    select
        user_key,
        sum(amount_usd) as total_revenue
    from {{ ref('fct_payments') }}
    where status = 'success'
    group by user_key
),

total_month_active as (
    select
        user_key,
        count(distinct date_trunc('month', payment_timestamp_utc)) as total_month_active
    from {{ ref('fct_payments') }}
    where status = 'success'
    group by user_key
),

avg_monthly_ltv as (
    select
        r.user_key,
        r.total_revenue,
        m.total_month_active,
        round(r.total_revenue / m.total_month_active, 2) as avg_monthly_ltv
    from total_revenue r
    inner join total_month_active m on r.user_key = m.user_key
),

subscription_status as (
    select
        user_key,
        subscription_status
    from (
        select
            user_key,
            subscription_status,
            row_number() over (partition by user_key order by event_timestamp_utc desc) as rn
        from {{ ref('fct_subscription_events') }}
    ) t
    where rn = 1
),
    
final as (
    select
        u.user_id,
        u.name,
        c.cohort_month,
        u.acquisition_channel,
        u.country, 
        f.first_plan,
        l.total_revenue,
        l.total_month_active,
        l.avg_monthly_ltv,
        s.subscription_status
    from {{ ref('dim_users') }} u
    inner join cohort_month c           on u.user_key = c.user_key
    inner join first_plan f             on u.user_key = f.user_key
    inner join avg_monthly_ltv l        on u.user_key = l.user_key
    inner join subscription_status s    on u.user_key = s.user_key
)
select * from final