with new_mrr as (
    select 
        count(distinct se.user_key)     as new_mrr_users,
        sum(p.price_usd)                as new_mrr_payment,
        d.batch_month
    from data_platform.foundation.fct_subscription_events se
    left join data_platform.foundation.dim_date d   on se.date_key = d.date_key
    left join data_platform.foundation.dim_plans p  on se.plan_key = p.plan_key
    where se.event_type in ('trial_convert', 'reactivate')
    group by batch_month
),

prev_price_expansion as (
    select
        se.user_key,
        lag(p.price_usd) over (partition by se.user_key order by d.full_date)   as prev_price,
        p.price_usd                                                             as current_price,
        d.batch_month
    from data_platform.foundation.fct_subscription_events se
    left join data_platform.foundation.dim_date d   on se.date_key = d.date_key
    left join data_platform.foundation.dim_plans p  on se.plan_key = p.plan_key
    where se.event_type in ('upgrade')
),

expansion_mrr as (
    select
        count(distinct user_key)            as expansion_mrr_users,
        sum (current_price - prev_price)    as expansion_mrr_payment,
        batch_month
    from prev_price_expansion
    group by batch_month
),

prev_price_contraction as (
    select
        se.user_key,
        lag(p.price_usd) over (partition by se.user_key order by d.full_date)   as prev_price,
        p.price_usd                                                             as current_price,
        d.batch_month
    from data_platform.foundation.fct_subscription_events se
    left join data_platform.foundation.dim_date d   on se.date_key = d.date_key
    left join data_platform.foundation.dim_plans p  on se.plan_key = p.plan_key
    where se.event_type in ('downgrade')
),

contraction_mrr as (
    select
        count(distinct user_key)            as contraction_mrr_users,
        sum (prev_price - current_price)    as contraction_mrr_payment,
        batch_month
    from prev_price_contraction
    group by batch_month
),

churn_mrr as (
    select
        count(distinct se.user_key)     as churn_mrr_users,
        sum(p.price_usd)                as churn_mrr_payment,
        d.batch_month
    from data_platform.foundation.fct_subscription_events se
    left join data_platform.foundation.dim_date d   on se.date_key = d.date_key
    left join data_platform.foundation.dim_plans p  on se.plan_key = p.plan_key
    where se.event_type in ('cancel')
    group by batch_month
),

net_mrr as (
    select 
        coalesce(ne.batch_month, ex.batch_month, co.batch_month, ch.batch_month)    as batch_month,
        coalesce(ne.new_mrr_users, 0)                                               as new_mrr_users,
        coalesce(ne.new_mrr_payment, 0)                                             as new_mrr_payment,
        coalesce(ex.expansion_mrr_users, 0)                                         as expansion_mrr_users,
        coalesce(ex.expansion_mrr_payment, 0)                                       as expansion_mrr_payment,
        coalesce(co.contraction_mrr_users, 0)                                       as contraction_mrr_users,
        coalesce(co.contraction_mrr_payment, 0)                                     as contraction_mrr_payment,
        coalesce(ch.churn_mrr_users, 0)                                             as churn_mrr_users,
        coalesce(ch.churn_mrr_payment, 0)                                           as churn_mrr_payment
    from new_mrr ne
    full join expansion_mrr ex      on ne.batch_month = ex.batch_month
    full join contraction_mrr co    on ne.batch_month = co.batch_month
    full join churn_mrr ch          on ne.batch_month = ch.batch_month
),

final as (
    select
        batch_month,
        new_mrr_users,
        new_mrr_payment,
        expansion_mrr_users,
        expansion_mrr_payment,
        contraction_mrr_users,
        contraction_mrr_payment,
        churn_mrr_users,
        churn_mrr_payment,
        (new_mrr_payment + expansion_mrr_payment - contraction_mrr_payment - churn_mrr_payment) as net_mrr
    from net_mrr
)

select * from final
order by batch_month