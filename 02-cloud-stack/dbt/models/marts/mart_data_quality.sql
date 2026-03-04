with late_arriving_observe as (
    select
        batch_month,
        count(*)                                                    as total_events,
        count(case when is_late_arriving = true then 1 end)         as late_arriving_count,
        round(
            count(case when is_late_arriving = true then 1 end) / 
            count(*) * 100, 2)                                      as late_arriving_pct
    from data_platform.staging.stg_subscription_events
    group by batch_month
),

unknown_plan_observe as (
    select
        batch_month,
        count(case when plan_cleaned = 'Unknown' then 1 end)        as unknown_plan_count,
        count(plan_cleaned)                                         as plan_count,
        round(
            count(case when plan_cleaned = 'Unknown' then 1 end) / 
            count (plan_cleaned) * 100, 2 )                         as null_plan_pct
    from data_platform.staging.stg_subscription_events
    group by batch_month 
),

users_observe as (
    select
        batch_month,
        count(case when event_type = 'trial_start' then 1 end)                  as new_user_count,
        count(case when event_type in ('trial_expire', 'cancel') then 1 end)    as churned_users_count
    from data_platform.staging.stg_subscription_events
    group by batch_month
),

payments_observe as (
    select
        batch_month,
        count(case when status = 'failed' then 1 end)           as failed_payment_count,
        count(*)                                                as total_payment_count,
        round(
            count(case when status = 'failed' then 1 end) / 
            count(*) * 100, 2) as failed_payment_pct,
        sum(case when status = 'success' then amount_usd end)   as total_monthly_revenue
    from data_platform.staging.stg_payments
    group by batch_month
),

final as (
    select
        l.batch_month,
        l.total_events,
        l.late_arriving_count,
        l.late_arriving_pct,
        n.unknown_plan_count,
        n.plan_count,
        n.null_plan_pct,
        u.new_user_count,
        u.churned_users_count,
        p.failed_payment_count,
        p.total_payment_count,
        p.failed_payment_pct,
        p.total_monthly_revenue
    from late_arriving_observe l
    full join unknown_plan_observe n    on l.batch_month = n.batch_month
    full join users_observe u           on l.batch_month = u.batch_month
    full join payments_observe p        on l.batch_month = p.batch_month
)

select * from final
order by batch_month asc