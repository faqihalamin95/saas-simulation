with subscription_events as (
    select
        se.event_type,
        se.subscription_status,
        se.plan_key,
        se.user_key,
        se.date_key,
        p.price_usd,
        to_char(date_trunc('month', d.full_date), 'YYYY-MM') as month
    from {{ ref('fct_subscription_events') }} se
    left join {{ ref('dim_date') }} d on se.date_key = d.date_key
    left join {{ ref('dim_plans') }} p on se.plan_key = p.plan_key
),

with_previous as (
    select
        *,
        lag(price_usd) over (
            partition by user_key
            order by date_key
        ) as previous_price_usd
    from subscription_events
),

mrr_movements as (
    select
        month,
        sum(case when event_type in ('trial_convert','reactivate') then price_usd       else 0 end) as new_mrr,
        sum(case when event_type = 'upgrade'       then price_usd - previous_price_usd  else 0 end) as expansion_mrr,
        sum(case when event_type = 'downgrade'     then previous_price_usd - price_usd  else 0 end) as contraction_mrr,
        sum(case when event_type = 'cancel'        then previous_price_usd              else 0 end) as churned_mrr
    from with_previous
    group by 1
),

net_mrr as (
    select
        month,
        new_mrr,
        expansion_mrr,
        contraction_mrr,
        churned_mrr,
        (new_mrr + expansion_mrr - contraction_mrr - churned_mrr) as net_mrr
    from mrr_movements
),

total_mrr as (
    select
        *,
        sum(net_mrr) over (order by month) as total_mrr
    from net_mrr
),

final as (
    select
        month,
        new_mrr,
        expansion_mrr,
        contraction_mrr,
        churned_mrr,
        net_mrr,
        case 
            when lag(total_mrr) over (order by month) <= 0 then null
            else round((net_mrr / lag(total_mrr) over (order by month)) * 100, 2)
        end as mrr_growth_rate
    from total_mrr
)

select * from final
order by month