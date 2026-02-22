with cohort_starts as (
    -- Identify cohort start for each user
    select
        f.user_key,
        date_trunc('month', d.full_date)::date as cohort_month,
        min(d.full_date) as cohort_start_date
    from {{ ref('fct_subscription_events') }} f
    join {{ ref('dim_date') }} d on f.date_key = d.date_key
    where f.event_type in ('trial_convert', 'upgrade', 'reactivate')
    group by f.user_key, date_trunc('month', d.full_date)::date
),

user_monthly_status as (
    -- Track status per user per month
    select
        f.user_key,
        date_trunc('month', d.full_date)::date as activity_month,
        max(case when f.event_type in ('cancel', 'trial_expire') then 1 else 0 end) as is_churned
    from {{ ref('fct_subscription_events') }} f
    join {{ ref('dim_date') }} d on f.date_key = d.date_key
    group by f.user_key, date_trunc('month', d.full_date)::date
),

cohort_timeline as (
    -- Generate timeline per cohort per user
    select
        c.user_key,
        c.cohort_month,
        generate_series(
            c.cohort_month,
            '2024-12-31'::date,
            interval '1 month'
        )::date as month
    from cohort_starts c
),

retention_status as (
    select
        ct.cohort_month,
        ct.user_key,
        ct.month,
        extract(year from age(ct.month, ct.cohort_month)) * 12 + 
        extract(month from age(ct.month, ct.cohort_month)) as month_number,
        case 
            when ums.is_churned = 1 then false
            else true
        end as is_retained
    from cohort_timeline ct
    left join user_monthly_status ums 
        on ct.user_key = ums.user_key 
        and ct.month = ums.activity_month
),

final as (
    select
        cohort_month,
        month_number,
        count(distinct user_key) as cohort_size,
        sum(case when is_retained then 1 else 0 end) as retained_users,
        round(100.0 * sum(case when is_retained then 1 else 0 end) / count(distinct user_key), 2) as retention_rate
    from retention_status
    group by cohort_month, month_number
)

select * from final
order by cohort_month, month_number