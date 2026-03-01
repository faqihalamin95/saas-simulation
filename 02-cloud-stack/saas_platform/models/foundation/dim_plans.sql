with plan_appearance_final as (
    select 
        plan_cleaned                        as plan,
        min(event_date)                     as first_seen
    from {{ ref('stg_product_events') }}
    where plan_cleaned is not null
      and plan_cleaned != 'Unknown'
    group by plan_cleaned
),

all_plans as (
    select 
        plan,
        case 
            when plan = 'Pro Plus'      then '2024-07-01'::date
            when plan = 'Growth'        then '2025-01-01'::date
            when plan = 'Enterprise'    then '2025-01-01'::date
            when plan = 'Starter'       then '2025-01-01'::date
            else first_seen
        end as effective_from,
        case 
            when plan = 'Pro'           then '2024-06-30'::date
            when plan = 'Pro Plus'      then '2024-12-31'::date
            when plan = 'Free'          then '2024-12-31'::date
            when plan = 'Business'      then '2024-12-31'::date
            else '9999-12-31'::date
        end as effective_to,
        case plan
            when 'Free'         then 0.00
            when 'Starter'      then 0.00
            when 'Pro'          then 15.00
            when 'Pro Plus'     then 15.00
            when 'Growth'       then 25.00
            when 'Business'     then 50.00
            when 'Enterprise'   then 80.00
        end as price_usd
    from plan_appearance_final
),

final as (
    select
        row_number() over (order by price_usd, effective_from)  as plan_key,
        plan,
        price_usd,
        effective_from,
        effective_to,
        effective_to = '9999-12-31'::date                       as is_current
    from all_plans
)

select * from final
order by price_usd